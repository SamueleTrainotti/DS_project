package com.example.ring;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;

public class NodeActor extends AbstractActor {

    private final int id;         // node ID
    private final String nodeKey; // node key (identifier in the ring)
    private ActorRef next;

    // Local key-value store
    private final Map<String, String> localStore = new HashMap<>();

    private NodeActor(int id, String nodeKey) {
        this.id = id;
        this.nodeKey = nodeKey;
    }

    public static Props props(int id, String nodeKey) {
        return Props.create(NodeActor.class, () -> new NodeActor(id, nodeKey));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.SetNext.class, msg -> {
                    this.next = msg.next;
                })
                .match(Messages.Start.class, msg -> {
                    System.out.println("Node " + id + " (" + nodeKey + ") START: " + msg.message);
                    if (next != null) {
                        next.tell(new Messages.Pass(msg.message, 1), getSelf());
                    }
                })
                .match(Messages.Pass.class, msg -> {
                    System.out.println("Node " + id + " (" + nodeKey + ") PASS (" + msg.hops + "): " + msg.message);

                    // for demo. stop after 10 hops
                    if (msg.hops < 10) {
                        if (next != null) {
                            next.tell(new Messages.Pass(msg.message, msg.hops + 1), getSelf());
                        }
                    }
                })
                // === KV Store ===
                .match(Messages.Put.class, msg -> {
                    localStore.put(msg.key, msg.value);
                    System.out.println("Node " + id + " (" + nodeKey + ") stored: " + msg.key + "=" + msg.value);
                })
                .match(Messages.Get.class, msg -> {
                    String value = localStore.get(msg.key);
                    getSender().tell(new Messages.GetResponse(msg.key, value), getSelf());
                })
                .build();
    }
}
