package com.example.ring;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A demo application that showcases the functionality of a distributed ring data store.
 * This application demonstrates:
 * <ul>
 *     <li>Creation of a ring with a specified replication factor.</li>
 *     <li>Dynamically adding and removing nodes from the ring.</li>
 *     <li>Putting, getting, and updating data from the ring via a client actor.</li>
 *     <li>Automatic data rebalancing when nodes are added or removed.</li>
 * </ul>
 * The application uses the Akka actor model to manage concurrency and distribution.
 */
public class RingApp {

    private static final Map<Long, ActorRef> nodesByKey = new HashMap<>();
    private static int nextId = 0;

    /**
     * The main entry point of the application.
     * <p>This method sets up an Akka {@link ActorSystem}, creates the nodes,
     * and then simulates a series of operations to demonstrate the ring's features.
     *
     * @param args Command line arguments (not used).
     * @throws InterruptedException if any of the sleep operations are interrupted.
     */
    public static void main(String[] args) throws InterruptedException {
        ActorSystem system = ActorSystem.create("RingSystem");

        // Choose replication factor
        final int replicationFactor = 3;

        // --- Initial Setup ---
        System.out.println(">>> Adding initial nodes: 10, 20, 30, 40");
        addNode(system, 10L, replicationFactor);
        addNode(system, 20L, replicationFactor);
        addNode(system, 30L, replicationFactor);
        addNode(system, 40L, replicationFactor);

        // Create clients that use nodes as their entry points
        ActorRef client1 = system.actorOf(ClientActor.props(nodesByKey.get(10L)), "client1");
        ActorRef client2 = system.actorOf(ClientActor.props(nodesByKey.get(20L)), "client2");

        // In a real app, you wouldn't use Thread.sleep. This is for demo purposes
        // to allow membership to propagate before sending client requests.
        Thread.sleep(1000);

        // --- Basic PUT/GET ---
        System.out.println("\n>>> Client 1 putting K=15, V='v15'");
        client1.tell(new Messages.DataPutRequest(new DataItem(0, 15L, "v15"), replicationFactor), ActorRef.noSender());
        Thread.sleep(1000); // Allow time for the write to complete

        System.out.println("\n>>> Client 1 getting K=15");
        client1.tell(new Messages.DataGetRequest(15L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Client 2 putting K=35, V='v35' (will wrap around the ring)");
        client2.tell(new Messages.DataPutRequest(new DataItem(0, 35L, "v35"), replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Client 2 getting K=35");
        client2.tell(new Messages.DataGetRequest(35L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Rebalancing Demo ---
        System.out.println("\n>>> Adding node 25. This should trigger rebalancing.");
        addNode(system, 25L, replicationFactor);
        Thread.sleep(2000); // Wait a moment for rebalance to complete

        System.out.println("\n>>> Client 1 getting K=15 (after adding node 25)");
        client1.tell(new Messages.DataGetRequest(15L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Removing node 20. This should trigger rebalancing.");
        removeNode(20L);
        Thread.sleep(2000); // Wait for rebalance

        System.out.println("\n>>> Client 1 getting K=15 (after removing node 20)");
        client1.tell(new Messages.DataGetRequest(15L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Update Demo ---
        System.out.println("\n>>> Client 1 updating K=15 with V='v15-updated'");
        client1.tell(new Messages.DataPutRequest(new DataItem(0, 15L, "v15-updated"), replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Client 1 getting K=15 (to see updated value)");
        client1.tell(new Messages.DataGetRequest(15L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Get Non-Existent Key ---
        System.out.println("\n>>> Client 1 getting K=99 (should not be found)");
        client1.tell(new Messages.DataGetRequest(99L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);


        System.out.println("\nDemo finished. Shutting down actor system.");
        system.terminate();
    }

    private static void addNode(ActorSystem system, long nodeKey, int replicationFactor) {
        if (nodesByKey.containsKey(nodeKey)) {
            System.out.println("[main] node " + Long.toUnsignedString(nodeKey) + " already exists");
            return;
        }
        ActorRef node = system.actorOf(NodeActor.props(nextId++, nodeKey, replicationFactor), "node" + Long.toUnsignedString(nodeKey));
        nodesByKey.put(nodeKey, node);
        broadcastMembership();
        System.out.println("[main] added node " + Long.toUnsignedString(nodeKey));
    }

    private static void removeNode(long nodeKey) {
        ActorRef ref = nodesByKey.remove(nodeKey);
        if (ref != null) {
            ref.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
            broadcastMembership();
            System.out.println("[main] removed node " + Long.toUnsignedString(nodeKey));
        } else {
            System.out.println("[main] no such node to remove: " + Long.toUnsignedString(nodeKey));
        }
    }

    private static void broadcastMembership() {
        List<Messages.NodeInfo> infos = nodesByKey.entrySet().stream()
                .map(e -> new Messages.NodeInfo(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        Messages.UpdateMembership membershipMessage = new Messages.UpdateMembership(new ArrayList<>(infos));

        for (ActorRef node : nodesByKey.values()) {
            node.tell(membershipMessage, ActorRef.noSender());
        }
    }
}
