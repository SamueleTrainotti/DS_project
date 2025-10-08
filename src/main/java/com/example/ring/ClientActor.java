package com.example.ring;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.PatternsCS;
import akka.util.Timeout;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * An actor that simulates a client interacting with the distributed ring data store.
 *
 * <p>The {@code ClientActor} provides a simplified interface for performing PUT and GET operations
 * on the ring. It is initialized with a reference to the {@link RingManager} and the key of an
 * "entry" node, which will be used as the coordinator for all its requests.
 *
 * <p>Its responsibilities include:
 * <ul>
 *     <li>Receiving client-level requests, such as {@link Messages.ClientUpdate} and {@link Messages.ClientGet}.</li>
 *     <li>Translating these requests into the manager-level protocol ({@link Messages.ManagerPut} and
 *     {@link Messages.ManagerGet}).</li>
 *     <li>Sending the translated requests to the {@link RingManager}, specifying the coordinator node.</li>
 *     <li>Asynchronously handling the responses and printing the outcome (success, failure, or retrieved data)
 *     to the console.</li>
 * </ul>
 *
 * <p>This actor demonstrates how a client application would interact with the ring, abstracting away the
 * details of which node is ultimately responsible for the data.
 */
public class ClientActor extends AbstractActor {

    private final ActorRef manager; // entrypoint to the system
    private final long entryNodeKey; // which node to use as coordinator
    private final Timeout timeout = Timeout.create(Duration.ofSeconds(3));

    /**
     * Constructs a ClientActor.
     *
     * @param manager      The {@link ActorRef} of the {@link RingManager}.
     * @param entryNodeKey The key of the node to be used as a coordinator for requests.
     */
    public ClientActor(ActorRef manager, long entryNodeKey) {
        this.manager = manager;
        this.entryNodeKey = entryNodeKey;
    }

    /**
     * Creates a {@link Props} object for creating a {@link ClientActor}.
     *
     * @param manager      The {@link ActorRef} of the {@link RingManager}.
     * @param entryNodeKey The key of the node to be used as a coordinator.
     * @return A {@link Props} configuration object for the ClientActor.
     */
    public static Props props(ActorRef manager, long entryNodeKey) {
        return Props.create(ClientActor.class, () -> new ClientActor(manager, entryNodeKey));
    }

    /**
     * Defines the message-handling behavior of the actor.
     *
     * @return A {@link Receive} object that maps message types to handler methods.
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.ClientUpdate.class, this::onClientUpdate)
                .match(Messages.ClientGet.class, this::onClientGet)
                .build();
    }

    /**
     * Handles a {@link Messages.ClientUpdate} request.
     * <p>It wraps the request in a {@link Messages.ManagerPut} message and sends it to the
     * {@link RingManager}. It then asynchronously handles the acknowledgement.
     *
     * @param req The client update request.
     */
    private void onClientUpdate(Messages.ClientUpdate req) {
        Messages.ManagerPut put = new Messages.ManagerPut(
                entryNodeKey, req.item);

        @SuppressWarnings("deprecation")
        CompletionStage<Object> fut =
                PatternsCS.ask(manager, put, timeout);

        fut.whenComplete((reply, ex) -> {
            if (ex != null) {
                System.out.println("[client] Update failed: " + ex.getMessage());
            } else {
                System.out.println("[client] Update OK for key=" + req.item.getKey());
            }
        });
    }

    /**
     * Handles a {@link Messages.ClientGet} request.
     * <p>It wraps the request in a {@link Messages.ManagerGet} message and sends it to the
     * {@link RingManager}. It then asynchronously handles the response, printing the retrieved value.
     *
     * @param req The client get request.
     */
    private void onClientGet(Messages.ClientGet req) {
        Messages.ManagerGet get = new Messages.ManagerGet(entryNodeKey, req.key);

        @SuppressWarnings("deprecation")
        CompletionStage<Object> fut =
                PatternsCS.ask(manager, get, timeout);

        fut.whenComplete((reply, ex) -> {
            if (ex != null) {
                System.out.println("[client] Get failed: " + ex.getMessage());
            } else if (reply instanceof Messages.DataGetResponse) {
                Messages.DataGetResponse r = (Messages.DataGetResponse) reply;
                System.out.println("[client] Get result: " + r.item.getKey() + "=" + r.item.getValue());
            }
        });
    }
}
