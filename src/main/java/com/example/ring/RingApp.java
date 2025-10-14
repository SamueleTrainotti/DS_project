package com.example.ring;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

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
    /**
     * The main entry point of the application.
     * <p>This method sets up an Akka {@link ActorSystem}, creates a {@link RingManager},
     * and then simulates a series of operations to demonstrate the ring's features.
     *
     * @param args Command line arguments (not used).
     * @throws InterruptedException if any of the sleep operations are interrupted.
     */
    public static void main(String[] args) throws InterruptedException {
        ActorSystem system = ActorSystem.create("RingSystem");

        // Choose replication factor
        final int replicationFactor = 3;

        // Create manager
        ActorRef manager = system.actorOf(RingManager.props(replicationFactor), "manager");

        // Create a client that uses node 10 as its entry point/coordinator
        ActorRef client = system.actorOf(ClientActor.props(manager, 10L), "client1");

        // --- Initial Setup ---
        System.out.println(">>> Adding initial nodes: 10, 20, 30, 40");
        manager.tell(new Messages.AddNode(10L), ActorRef.noSender());
        manager.tell(new Messages.AddNode(20L), ActorRef.noSender());
        manager.tell(new Messages.AddNode(30L), ActorRef.noSender());
        manager.tell(new Messages.AddNode(40L), ActorRef.noSender());

        // In a real app, you wouldn't use Thread.sleep. This is for demo purposes
        // to allow membership to propagate before sending client requests.
        Thread.sleep(1000);

        // --- Basic PUT/GET ---
        System.out.println("\n>>> Client putting K=15, V='v15'");
        client.tell(new Messages.DataPutRequest(new DataItem(0, 15L, "v15"), replicationFactor), ActorRef.noSender());
        Thread.sleep(1000); // Allow time for the write to complete

        System.out.println("\n>>> Client getting K=15");
        client.tell(new Messages.DataGetRequest(15L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Client putting K=35, V='v35' (will wrap around the ring)");
        client.tell(new Messages.DataPutRequest(new DataItem(0, 35L, "v35"), replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Client getting K=35");
        client.tell(new Messages.DataGetRequest(35L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Rebalancing Demo ---
        System.out.println("\n>>> Adding node 25. This should trigger rebalancing.");
        manager.tell(new Messages.AddNode(25L), ActorRef.noSender());
        Thread.sleep(2000); // Wait a moment for rebalance to complete

        System.out.println("\n>>> Client getting K=15 (after adding node 25)");
        client.tell(new Messages.DataGetRequest(15L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Removing node 20. This should trigger rebalancing.");
        manager.tell(new Messages.RemoveNode(20L), ActorRef.noSender());
        Thread.sleep(2000); // Wait for rebalance

        System.out.println("\n>>> Client getting K=15 (after removing node 20)");
        client.tell(new Messages.DataGetRequest(15L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Update Demo ---
        System.out.println("\n>>> Client updating K=15 with V='v15-updated'");
        client.tell(new Messages.DataPutRequest(new DataItem(0, 15L, "v15-updated"), replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> Client getting K=15 (to see updated value)");
        client.tell(new Messages.DataGetRequest(15L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);

        // --- Get Non-Existent Key ---
        System.out.println("\n>>> Client getting K=99 (should not be found)");
        client.tell(new Messages.DataGetRequest(99L, replicationFactor), ActorRef.noSender());
        Thread.sleep(1000);


        System.out.println("\nDemo finished. Shutting down actor system.");
        system.terminate();
    }
}
