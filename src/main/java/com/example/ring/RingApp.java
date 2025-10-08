package com.example.ring;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.PatternsCS;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class RingApp {
    public static void main(String[] args) throws Exception {
        ActorSystem system = ActorSystem.create("RingSystem");

        // choose replication factor
        final int replicationFactor = 3;

        // create manager
        ActorRef manager = system.actorOf(RingManager.props(replicationFactor), "manager");

        // create a few nodes with keys 10,20,30,40 (unsigned keys)
        manager.tell(new Messages.AddNode(10L), ActorRef.noSender());
        manager.tell(new Messages.AddNode(20L), ActorRef.noSender());
        manager.tell(new Messages.AddNode(30L), ActorRef.noSender());
        manager.tell(new Messages.AddNode(40L), ActorRef.noSender());

        // small sleep so membership messages propagate (demo only)
        Thread.sleep(500);

        // Put a data item with numeric key 15 -> should be stored on nodes 20,30,40
        System.out.println("\n>>> put K=15 value='v15' via origin node 10");
        manager.tell(new Messages.ManagerPut(10L, new DataItem(0, 15L, "v15")), ActorRef.noSender());
        Thread.sleep(200);

        // Put a data item with numeric key 35 -> should be stored on nodes 40,10,20 (wrap-around)
        System.out.println("\n>>> put K=35 value='v35' via origin node 30");
        manager.tell(new Messages.ManagerPut(30L, new DataItem(0, 35L, "v35")), ActorRef.noSender());
        Thread.sleep(200);

        // Synchronous get via manager (ask manager, which forwards to origin node)
        System.out.println("\n>>> get K=15,key='t1' via origin node 10 (sync)");
        @SuppressWarnings("deprecation")
        CompletionStage<Object> fut1 = PatternsCS.ask(manager, new Messages.ManagerGet(10L, 15L), Duration.ofSeconds(3));
        Object r1 = fut1.toCompletableFuture().get();
        System.out.println("GET result: " + r1);

        System.out.println("\n>>> get K=35,key='t2' via origin node 30 (sync)");
        @SuppressWarnings("deprecation")
        CompletionStage<Object> fut2 = PatternsCS.ask(manager, new Messages.ManagerGet(30L, 35L), Duration.ofSeconds(3));
        Object r2 = fut2.toCompletableFuture().get();
        System.out.println("GET result: " + r2);

        // Now add a node with key 25 (between 20 and 30). This should trigger rebalance:
        System.out.println("\n>>> adding node 25 (rebalance should move some replicas)");
        manager.tell(new Messages.AddNode(25L), ActorRef.noSender());
        Thread.sleep(1000); // wait a moment for rebalance to complete

        // Attempt get again after rebalance
        System.out.println("\n>>> get K=15,key='t1' via origin node 10 (after adding 25)");
        @SuppressWarnings("deprecation")
        CompletionStage<Object> fut3 = PatternsCS.ask(manager, new Messages.ManagerGet(10L, 15L), Duration.ofSeconds(3));
        Object r3 = fut3.toCompletableFuture().get();
        System.out.println("GET result after add: " + r3);

        // Remove node 20
        System.out.println("\n>>> removing node 20 (rebalance should replicate items hosted by 20 to others)");
        manager.tell(new Messages.RemoveNode(20L), ActorRef.noSender());
        Thread.sleep(1000);

        System.out.println("\n>>> get K=15,key='t1' after removing node 20");
        @SuppressWarnings("deprecation")
        CompletionStage<Object> fut4 = PatternsCS.ask(manager, new Messages.ManagerGet(10L, 15L), Duration.ofSeconds(3));
        Object r4 = fut4.toCompletableFuture().get();
        System.out.println("GET after remove: " + r4);

        System.out.println("\n##########");
        System.out.println("Client usage");
        System.out.println("##########");

        // create a client that uses node 10 as coordinator
        ActorRef client = system.actorOf(ClientActor.props(manager, 10L), "client1");

        // demo sequence
        System.out.println("Request <update> of '15' on node 15");
        client.tell(new Messages.ClientUpdate(new DataItem(0, 15L, "22C")), ActorRef.noSender());
        Thread.sleep(500);
        System.out.println("Request <get> of 'temperature' on node 15");
        client.tell(new Messages.ClientGet(15L), ActorRef.noSender());
        Thread.sleep(1500);

        System.out.println("\nDemo finished -- shutting down.");
        system.terminate();
    }
}
