package com.example.ring;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.PatternsCS;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * An actor that simulates a client interacting with the distributed ring data store.
 *
 * <p>The {@code ClientActor} provides a simplified interface for performing PUT and GET operations
 * on the ring. It is initialized with a reference to an "entry" node, which will be used as
 * the coordinator for all its requests.
 *
 * <p>Its responsibilities include:
 * <ul>
 *     <li>Receiving client-level requests, such as {@link Messages.DataPutRequest} and {@link Messages.DataGetRequest}.</li>
 *     <li>Sending these requests directly to its designated entry node.</li>
 *     <li>Asynchronously handling the responses and printing the outcome (success, failure, or retrieved data)
 *     to the console.</li>
 * </ul>
 *
 * <p>This actor demonstrates how a client application would interact with the ring, abstracting away the
 * details of which node is ultimately responsible for the data.
 */
public class ClientActor extends AbstractActor {

    private final ActorRef entryNode; // which node to use as coordinator
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    public ClientActor(ActorRef entryNode) {
        this.entryNode = entryNode;
    }

    public static Props props(ActorRef entryNode) {
        return Props.create(ClientActor.class, () -> new ClientActor(entryNode));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.DataPutRequest.class, this::onDataPutRequest)
                .match(Messages.DataGetRequest.class, this::onDataGetRequest)
                .build();
    }

    /**
     * Handles a {@link Messages.DataPutRequest} request from the client.
     * <p>It sends the request directly to its entry node and asynchronously handles the acknowledgement.
     *
     * @param req The client update request.
     */
    private void onDataPutRequest(Messages.DataPutRequest req) {
        CompletionStage<Object> fut = PatternsCS.ask(entryNode, req, TIMEOUT);

        fut.whenComplete((reply, ex) -> {
            if (ex != null || !(reply instanceof Messages.PutAck) || !((Messages.PutAck) reply).ok) {
                System.out.println("[client] Update failed for key=" + req.item.getKey());
            } else {
                System.out.println("[client] Update OK for key=" + req.item.getKey());
            }
        });
    }

    /**
     * Handles a {@link Messages.DataGetRequest} request from the client.
     * <p>It sends the request directly to its entry node and asynchronously handles the response,
     * printing the retrieved value.
     *
     * @param req The client get request.
     */
    private void onDataGetRequest(Messages.DataGetRequest req) {
        CompletionStage<Object> fut = PatternsCS.ask(entryNode, req, TIMEOUT);

        fut.whenComplete((reply, ex) -> {
            if (ex != null) {
                System.out.println("[client] Get failed for key=" + req.key + ": " + ex.getMessage());
            } else if (reply instanceof Messages.DataItemResponse) {
                Messages.DataItemResponse r = (Messages.DataItemResponse) reply;
                if (r.item != null && r.item.getValue() != null) {
                    System.out.println("[client] Get result: " + r.item.getKey() + "=" + r.item.getValue() + " (v" + r.item.getVersion() + ")");
                } else {
                    System.out.println("[client] Get result: key=" + req.key + " not found.");
                }
            } else {
                System.out.println("[client] Get failed for key=" + req.key + ": Unexpected response type.");
            }
        });
    }
}
