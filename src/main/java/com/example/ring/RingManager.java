package com.example.ring;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.PatternsCS;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of nodes in the distributed ring and facilitates communication.
 *
 * <p>The RingManager is a central actor responsible for:
 * <ul>
 *     <li><b>Node Management:</b> Creating new nodes ({@link NodeActor}) and adding them to the ring,
 *     as well as removing them.</li>
 *     <li><b>Membership Broadcasting:</b> Maintaining a map of node keys to their {@link ActorRef}s and
 *     broadcasting updated membership lists to all nodes in the ring whenever a node is added or removed.
 *     This ensures that all nodes have a consistent view of the ring's topology.</li>
 *     <li><b>Request Forwarding:</b> Exposing {@link Messages.ManagerPut} and {@link Messages.ManagerGet} APIs.
 *     These are primarily for demonstration and testing purposes, allowing external clients (like the main application)
 *     to initiate PUT and GET operations by specifying an "origin" node. The manager forwards these requests
 *     to the designated origin node, which then handles the request according to the ring's protocol.</li>
 * </ul>
 *
 * <p>This actor plays a crucial role in the dynamic nature of the ring, enabling nodes to join and leave
 * without disrupting the overall service.
 */
public class RingManager extends AbstractActor {

    private final Map<Long, ActorRef> nodesByKey = new HashMap<>();
    private final int replicationFactor;
    private int nextId = 0;

    public static Props props(int replicationFactor) {
        return Props.create(RingManager.class, () -> new RingManager(replicationFactor));
    }

    public RingManager(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.AddNode.class, this::onAddNode)
                .match(Messages.RemoveNode.class, this::onRemoveNode)
                .match(Messages.ManagerPut.class, this::onManagerPut)
                .match(Messages.ManagerGet.class, this::onManagerGet)
                .build();
    }

    private void onAddNode(Messages.AddNode msg) {
        long nodeKey = msg.nodeKey;
        if (nodesByKey.containsKey(nodeKey)) {
            System.out.println("[manager] node " + Long.toUnsignedString(nodeKey) + " already exists");
            return;
        }
        ActorRef node = getContext().actorOf(NodeActor.props(nextId++, nodeKey, replicationFactor), "node" + Long.toUnsignedString(nodeKey));
        nodesByKey.put(nodeKey, node);
        broadcastMembership();
        System.out.println("[manager] added node " + Long.toUnsignedString(nodeKey));
    }

    private void onRemoveNode(Messages.RemoveNode msg) {
        long nodeKey = msg.nodeKey;
        ActorRef ref = nodesByKey.remove(nodeKey);
        if (ref != null) {
            getContext().stop(ref);
            broadcastMembership();
            System.out.println("[manager] removed node " + Long.toUnsignedString(nodeKey));
        } else {
            System.out.println("[manager] no such node to remove: " + Long.toUnsignedString(nodeKey));
        }
    }

    private void onManagerPut(Messages.ManagerPut msg) {
        ActorRef origin = nodesByKey.get(msg.originNodeKey);
        final ActorRef replyTo = getSender();

        if (origin == null) {
            replyTo.tell(new Messages.PutAck(false), getSelf());
            return;
        }

        Duration timeout = Duration.ofSeconds(7);
        CompletionStage<Object> future = PatternsCS.ask(origin, new Messages.DataPutRequest(msg.item, replicationFactor), timeout);

        future.whenComplete((resp, ex) -> {
            if (ex != null) {
                replyTo.tell(new Messages.PutAck(false), getSelf());
            } else {
                replyTo.tell(resp, getSelf());
            }
        });
    }

    private void onManagerGet(Messages.ManagerGet msg) {
        ActorRef origin = nodesByKey.get(msg.originNodeKey);
        if (origin == null) {
            DataItem tmp = new DataItem(0, msg.key, null);
            getSender().tell(new Messages.DataItemResponse(tmp), getSelf());
            return;
        }

        CompletionStage<Object> future = PatternsCS.ask(origin, new Messages.DataGetRequest(msg.key, replicationFactor), Duration.ofSeconds(5));
        final ActorRef replyTo = getSender();
        future.whenComplete((resp, ex) -> {
            if (ex != null) {
                DataItem tmp = new DataItem(0, msg.key, null);
                replyTo.tell(new Messages.DataItemResponse(tmp), getSelf());
            } else {
                replyTo.tell(resp, getSelf());
            }
        });
    }

    private void broadcastMembership() {
        List<Messages.NodeInfo> infos = nodesByKey.entrySet().stream()
                .map(e -> new Messages.NodeInfo(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        Messages.UpdateMembership membershipMessage = new Messages.UpdateMembership(infos);

        for (ActorRef node : nodesByKey.values()) {
            node.tell(membershipMessage, getSelf());
        }
    }
}
