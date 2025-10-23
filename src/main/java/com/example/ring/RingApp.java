package com.example.ring;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.time.Duration;
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

    // --- Configurable Parameters ---
    private static final int REPLICATION_FACTOR = 3;
    private static final int W = 2; // Write quorum
    private static final int R = 2; // Read quorum
    private static final Duration T = Duration.ofSeconds(3); // Timeout

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

        // --- Initial Setup ---
        System.out.println(">>> Adding initial nodes: 10, 20, 30, 40");
        addNode(system, 10L);
        addNode(system, 20L);
        addNode(system, 30L);
        addNode(system, 40L);

        // Create clients that use nodes as their entry points
        ActorRef client1 = system.actorOf(ClientActor.props(nodesByKey.get(10L)), "client1");
        ActorRef client2 = system.actorOf(ClientActor.props(nodesByKey.get(20L)), "client2");

        // In a real app, you wouldn't use Thread.sleep. This is for demo purposes
        // to allow membership to propagate before sending client requests.
        Thread.sleep(1000);

        // --- Basic PUT/GET ---
        System.out.println("\n>>> Client 1 putting K=15, V='v15'");
        client1.tell(new Messages.DataPutRequest(new DataItem(0, 15L, "v15"), REPLICATION_FACTOR), ActorRef.noSender());
        Thread.sleep(1000); // Allow time for the write to complete

        System.out.println("\n>>> Client 1 getting K=15");
        client1.tell(new Messages.DataGetRequest(15L, REPLICATION_FACTOR), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Client 2 putting K=35, V='v35' (will wrap around the ring)");
        client2.tell(new Messages.DataPutRequest(new DataItem(0, 35L, "v35"), REPLICATION_FACTOR), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Client 2 getting K=35");
        client2.tell(new Messages.DataGetRequest(35L, REPLICATION_FACTOR), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Telling a node to crash
        System.out.println("\n>>> Telling node 30 to crash");
        nodesByKey.get(30L).tell(new Messages.CrashNode(), ActorRef.noSender());
        Thread.sleep(2000);

        // --- Rebalancing Demo ---
        System.out.println("\n>>> Adding node 35. This should trigger rebalancing.");
        addNode(system, 35L);
        Thread.sleep(2000); // Wait a moment for rebalance to complete
        // Debug print
        _debug_Print();

        System.out.println("\n>>> Client 1 getting K=15 (after adding node 25)");
        client1.tell(new Messages.DataGetRequest(15L, REPLICATION_FACTOR), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Removing node 20. This should trigger rebalancing.");
        removeNode(20L);
        Thread.sleep(2000); // Wait for rebalance
        // Debug print
        _debug_Print();

        System.out.println("\n>>> Client 1 getting K=15 (after removing node 20)");
        client1.tell(new Messages.DataGetRequest(15L, REPLICATION_FACTOR), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Update Demo ---
        System.out.println("\n>>> Client 1 updating K=15 with V='v15-updated'");
        client1.tell(new Messages.DataPutRequest(new DataItem(0, 15L, "v15-updated"), REPLICATION_FACTOR), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Client 1 getting K=15 (to see updated value)");
        client1.tell(new Messages.DataGetRequest(15L, REPLICATION_FACTOR), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Get Non-Existent Key ---
        System.out.println("\n>>> Client 1 getting K=99 (should not be found)");
        client1.tell(new Messages.DataGetRequest(99L, REPLICATION_FACTOR), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Telling node 30 to recover
        System.out.println("\n>>> Telling node 30 to recover");
        nodesByKey.get(30L).tell(new Messages.RecoverNode(), ActorRef.noSender());
        Thread.sleep(10000);

        // Debug print
        _debug_Print();


        System.out.println("\nDemo finished. Shutting down actor system.");
        system.terminate();
    }

    private static void _debug_Print() {
        // DEBUG print
        for (ActorRef node : nodesByKey.values()) {
            node.tell(new Messages._debug_GetStoredItems(), ActorRef.noSender());
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addNode(ActorSystem system, long nodeKey) {
        if (nodesByKey.containsKey(nodeKey)) {
            System.out.println("[main] node " + Long.toUnsignedString(nodeKey) + " already exists");
            return;
        }
        ActorRef node = system.actorOf(NodeActor.props(nextId++, nodeKey, REPLICATION_FACTOR, W, R, T), "node" + Long.toUnsignedString(nodeKey));
        nodesByKey.put(nodeKey, node);
        broadcastMembership();
        System.out.println("[main] added node " + Long.toUnsignedString(nodeKey));
    }

    private static void removeNode(long nodeKey) {
        ActorRef ref = nodesByKey.remove(nodeKey);
        if (ref != null) {
            ref.tell(new Messages.ForwardData(), ActorRef.noSender());
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
