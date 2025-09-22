package com.example.ring;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class RingApp {
    public static void main(String[] args) {
        final int numNodes = 5;
        boolean asynchronous = false;

        ActorSystem system = ActorSystem.create("RingSystem");

        // Create nodes (with unique node keys, fot now just "NodeKeyX")
        List<ActorRef> nodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            String nodeKey = "NodeKey" + i;
            nodes.add(system.actorOf(NodeActor.props(i, nodeKey), "node" + i));
        }

        // add connections to build the ring
        for (int i = 0; i < numNodes; i++) {
            ActorRef current = nodes.get(i);
            ActorRef next = nodes.get((i + 1) % numNodes);
            current.tell(new Messages.SetNext(next), ActorRef.noSender());
        }

        // Start ring demo
        nodes.get(0).tell(new Messages.Start("asd123"), ActorRef.noSender());

        // Put some data into node 2
        nodes.get(2).tell(new Messages.Put("25", "rock123"), ActorRef.noSender());
        nodes.get(2).tell(new Messages.Put("3", "iron"), ActorRef.noSender());

        // testing both sync and async for demo, then exit
        for (int i = 0; i < 2; i++) {
            if (asynchronous) {
                // Retrieve data from node 2
                CompletionStage<Object> result = Patterns.ask(
                        nodes.get(2),
                        new Messages.Get("25"),
                        Duration.ofSeconds(3)
                );

                result.whenComplete((resp, ex) -> {
                    if (resp instanceof Messages.GetResponse) {
                        Messages.GetResponse r = (Messages.GetResponse) resp;
                        System.out.println("[ASYNC] Got from node2: " + r.key + "=" + r.value);
                    }
                    system.terminate();
                });
            } else {
                try {
                    Timeout timeout = Timeout.create(Duration.ofSeconds(3));

                    Future<Object> future = Patterns.ask(
                            nodes.get(2),
                            new Messages.Get("3"),
                            timeout
                    );

                    Messages.GetResponse response =
                            (Messages.GetResponse) Await.result(future, timeout.duration());

                    System.out.println("[SYNC] Got from node2: " + response.key + "=" + response.value);

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    asynchronous = !asynchronous;
                }
            }
        }

    }
}
