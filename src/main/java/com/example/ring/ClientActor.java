package com.example.ring;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.PatternsCS;
import akka.util.Timeout;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class ClientActor extends AbstractActor {

    private final ActorRef manager; // entrypoint to the system
    private final long entryNodeKey; // which node to use as coordinator
    private final Timeout timeout = Timeout.create(Duration.ofSeconds(3));

    public ClientActor(ActorRef manager, long entryNodeKey) {
        this.manager = manager;
        this.entryNodeKey = entryNodeKey;
    }

    public static Props props(ActorRef manager, long entryNodeKey) {
        return Props.create(ClientActor.class, () -> new ClientActor(manager, entryNodeKey));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.ClientUpdate.class, this::onClientUpdate)
                .match(Messages.ClientGet.class, this::onClientGet)
                .build();
    }

    private void onClientUpdate(Messages.ClientUpdate req) {
        Messages.ManagerPut put = new Messages.ManagerPut(
                entryNodeKey, req.dataKey, req.key, req.value);

        @SuppressWarnings("deprecation")
        CompletionStage<Object> fut =
                PatternsCS.ask(manager, put, timeout);

        fut.whenComplete((reply, ex) -> {
            if (ex != null) {
                System.out.println("[client] Update failed: " + ex.getMessage());
            } else {
                System.out.println("[client] Update OK for key=" + req.key);
            }
        });
    }

    private void onClientGet(Messages.ClientGet req) {
        Messages.ManagerGet get = new Messages.ManagerGet(entryNodeKey, req.dataKey, req.key);

        @SuppressWarnings("deprecation")
        CompletionStage<Object> fut =
                PatternsCS.ask(manager, get, timeout);

        fut.whenComplete((reply, ex) -> {
            if (ex != null) {
                System.out.println("[client] Get failed: " + ex.getMessage());
            } else if (reply instanceof Messages.DataGetResponse) {
                Messages.DataGetResponse r = (Messages.DataGetResponse) reply;
                System.out.println("[client] Get result: " + r.key + "=" + r.value);
            }
        });
    }
}
