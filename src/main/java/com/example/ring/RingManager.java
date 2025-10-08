package com.example.ring;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.PatternsCS;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;

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

    /**
     * Creates a {@link Props} object for creating a {@link RingManager} actor.
     *
     * @param replicationFactor The number of replicas to maintain for each data item.
     * @return A {@link Props} configuration object for the RingManager.
     */
    public static Props props(int replicationFactor) {
        return Props.create(RingManager.class, () -> new RingManager(replicationFactor));
    }

    /**
     * Constructs a RingManager.
     *
     * @param replicationFactor The replication factor to be used for data storage.
     */
    public RingManager(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    /**
     * Defines the message-handling behavior of the actor.
     * <p>This method maps message types to their corresponding handler methods.
     *
     * @return A {@link Receive} object that defines how to handle incoming messages.
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.AddNode.class, this::onAddNode)
                .match(Messages.RemoveNode.class, this::onRemoveNode)
                .match(Messages.ManagerPut.class, this::onManagerPut)
                .match(Messages.ManagerGet.class, this::onManagerGet)
                .build();
    }

    /**
     * Handles the {@link Messages.AddNode} message.
     * <p>Creates a new {@link NodeActor}, adds it to the ring, and broadcasts the updated membership.
     *
     * @param msg The message containing the key of the node to add.
     */
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

    /**
     * Handles the {@link Messages.RemoveNode} message.
     * <p>Removes the specified node from the ring, stops its actor, and broadcasts the updated membership.
     *
     * @param msg The message containing the key of the node to remove.
     */
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

    /**
     * Handles the {@link Messages.ManagerPut} message.
     * <p>Forwards a data put request to the specified origin node.
     *
     * @param msg The message containing the origin node key and the data item to store.
     */
    private void onManagerPut(Messages.ManagerPut msg) {
        ActorRef origin = nodesByKey.get(msg.originNodeKey);
        if (origin == null) {
            getSender().tell(new Messages.PutAck(false), getSelf());
            return;
        }
        // forward a DataPutRequest to the chosen node (origin)
        origin.tell(new Messages.DataPutRequest(msg.item, replicationFactor), getSelf());
        getSender().tell(new Messages.PutAck(true), getSelf());
    }

    /**
     * Handles the {@link Messages.ManagerGet} message.
     * <p>Forwards a data get request to the specified origin node and pipes the result back to the original sender.
     *
     * @param msg The message containing the origin node key and the key of the data to retrieve.
     */
    private void onManagerGet(Messages.ManagerGet msg) {
        ActorRef origin = nodesByKey.get(msg.originNodeKey);
        if (origin == null) {
            DataItem tmp = new DataItem(0, msg.key, null);
            getSender().tell(new Messages.DataGetResponse(tmp), getSelf());
            return;
        }
        // ask the origin node to perform a DataGetRequest (which will consult responsible nodes)
        CompletionStage<Object> future = PatternsCS.ask(origin, new Messages.DataGetRequest(msg.key, replicationFactor), Duration.ofSeconds(3));
        final ActorRef replyTo = getSender();
        future.whenComplete((resp, ex) -> {
            if (ex != null) {
                DataItem tmp = new DataItem(0, msg.key, null);
                replyTo.tell(new Messages.DataGetResponse(tmp), getSelf());
            } else {
                replyTo.tell(resp, getSelf());
            }
        });
    }

    /**
     * Broadcasts the current ring membership to all nodes.
     * <p>This method is called whenever a node is added or removed, ensuring all nodes have a consistent view.
     */
    private void broadcastMembership() {
        List<Messages.NodeInfo> infos = new ArrayList<>();
        for (Map.Entry<Long, ActorRef> e : nodesByKey.entrySet()) {
            infos.add(new Messages.NodeInfo(e.getKey(), e.getValue()));
        }
        // send to all nodes
        for (ActorRef node : nodesByKey.values()) {
            node.tell(new Messages.UpdateMembership(infos), getSelf());
        }
    }
}
