package com.example.ring;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.PatternsCS;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Represents a single node in the distributed data store ring.
 *
 * <p>Each {@code NodeActor} is responsible for a segment of the hash ring, identified by its {@code nodeKey}.
 * Its primary duties include:
 * <ul>
 *     <li><b>Data Storage:</b> Maintaining a local key-value store ({@link LocalStore}) for data items for which it is a replica.</li>
 *     <li><b>Request Coordination:</b> Handling client requests for data operations. For a PUT operation, it coordinates a two-phase process
 *     of reading versions and then writing the new value to a quorum of nodes. For a GET operation, it retrieves data from a quorum of nodes
 *     and returns the most up-to-date version.</li>
 *     <li><b>Membership Management:</b> Receiving and processing membership updates from the {@link RingManager}.</li>
 *     <li><b>Data Rebalancing:</b> Redistributing data to the correct nodes when ring membership changes.</li>
 * </ul>
 * <p>The actor uses a quorum-based system (N, W, R) to ensure sequential consistency.
 */
public class NodeActor extends AbstractActor {

    private final int id;
    private final long nodeKey;
    private final int replicationFactor;

    private static final int W = 2; // write quorum
    private static final int R = 2; // read quorum
    private static final Duration T = Duration.ofSeconds(3);

    private List<Messages.NodeInfo> membership = new ArrayList<>();
    private final LocalStore localStore = new LocalStore();

    private NodeActor(int id, long nodeKey, int replicationFactor) {
        this.id = id;
        this.nodeKey = nodeKey;
        this.replicationFactor = replicationFactor;
    }

    public static Props props(int id, long nodeKey, int replicationFactor) {
        return Props.create(NodeActor.class, () -> new NodeActor(id, nodeKey, replicationFactor));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.UpdateMembership.class, this::onUpdateMembership)
                .match(Messages.DataPutRequest.class, this::onDataPutRequest)
                .match(Messages.StoreReplica.class, this::onStoreReplica)
                .match(Messages.DataGetRequest.class, this::onDataGetRequest)
                .match(Messages.GetReplica.class, this::onGetReplica)
                .build();
    }

    private void onUpdateMembership(Messages.UpdateMembership msg) {
        List<Messages.NodeInfo> sorted = new ArrayList<>(msg.nodes);
        sorted.sort(Comparator.comparing(a -> a.nodeKey, Long::compareUnsigned));
        this.membership = sorted;
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] membership updated: " + membership);
        rebalance();
    }

    /**
     * Handles a request to get data from the ring, ensuring sequential consistency.
     * <p>This node acts as the coordinator. It requests the data item from all N responsible nodes
     * and waits for R responses. It then finds the item with the highest version among the
     * responses and returns it to the original sender.
     *
     * @param req The {@link Messages.DataGetRequest} containing the key of the data to retrieve.
     */
    private void onDataGetRequest(Messages.DataGetRequest req) {
        final ActorRef replyTo = getSender();
        List<Messages.NodeInfo> responsible = findResponsibleNodes(req.key, req.replicationFactor);

        List<CompletionStage<Object>> futures = responsible.stream()
                .map(nodeInfo -> PatternsCS.ask(nodeInfo.ref, new Messages.GetReplica(req.key), T))
                .collect(Collectors.toList());

        CompletionStage<List<Messages.GetReplicaResponse>> quorumFuture = collectQuorum(futures, R, Messages.GetReplicaResponse.class);

        withTimeout(quorumFuture.toCompletableFuture(), T.getSeconds(), TimeUnit.SECONDS)
                .whenComplete((responses, ex) -> {
                    if (ex != null) {
                        // On timeout or failure to achieve quorum, reply with a not-found item.
                        replyTo.tell(new Messages.DataGetResponse(new DataItem(0, req.key, null)), getSelf());
                    } else {
                        // Find the response with the highest version number.
                        DataItem latestItem = responses.stream()
                                .map(response -> response.item)
                                .filter(Objects::nonNull)
                                .max(Comparator.comparing(DataItem::getVersion))
                                .orElse(new DataItem(0, req.key, null)); // Or not-found if all were null

                        replyTo.tell(new Messages.DataGetResponse(latestItem), getSelf());
                    }
                });
    }

    /**
     * Handles a request to put data into the ring, ensuring sequential consistency.
     * <p>This implements a two-phase write process:
     * <p>1. **Read Phase:** It requests the current version of the data item from all N responsible nodes
     *    and waits for W responses to determine the highest current version.
     * <p>2. **Write Phase:** It increments the version number, sends a {@link Messages.StoreReplica} request
     *    to all N nodes, and waits for W acknowledgements before confirming success to the client.
     *
     * @param req The {@link Messages.DataPutRequest} containing the data item to store.
     */
    private void onDataPutRequest(Messages.DataPutRequest req) {
        final ActorRef replyTo = getSender();
        final Long key = req.item.getKey();
        List<Messages.NodeInfo> responsible = findResponsibleNodes(key, req.replicationFactor);

        // Phase 1: Read versions from W nodes
        List<CompletionStage<Object>> readFutures = responsible.stream()
                .map(nodeInfo -> PatternsCS.ask(nodeInfo.ref, new Messages.GetReplica(key), T))
                .collect(Collectors.toList());

        CompletionStage<List<Messages.GetReplicaResponse>> readQuorumFuture = collectQuorum(readFutures, W, Messages.GetReplicaResponse.class);

        CompletionStage<List<Messages.PutAck>> finalFuture = readQuorumFuture.thenCompose(responses -> {
            // Determine the highest version from the read quorum
            int maxVersion = responses.stream()
                    .map(response -> response.item)
                    .filter(Objects::nonNull)
                    .mapToInt(DataItem::getVersion)
                    .max()
                    .orElse(0);

            DataItem newItem = new DataItem(maxVersion + 1, key, req.item.getValue());

            // Phase 2: Write the new version to N nodes and wait for W acks
            List<CompletionStage<Object>> writeFutures = responsible.stream()
                    .map(nodeInfo -> PatternsCS.ask(nodeInfo.ref, new Messages.StoreReplica(newItem), T))
                    .collect(Collectors.toList());

            return collectQuorum(writeFutures, W, Messages.PutAck.class);
        });

        withTimeout(finalFuture.toCompletableFuture(), T.getSeconds() * 2, TimeUnit.SECONDS) // Give more time for 2 phases
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        replyTo.tell(new Messages.PutAck(false), getSelf());
                    } else {
                        replyTo.tell(new Messages.PutAck(true), getSelf());
                    }
                });
    }

    /**
     * Handles a request to store a replica of a data item and sends an acknowledgement.
     */
    private void onStoreReplica(Messages.StoreReplica msg) {
        localStore.store(msg.item);
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] StoreReplica stored: " + msg.item.getKey() + "=" + msg.item.getValue() + " v" + msg.item.getVersion());
        getSender().tell(new Messages.PutAck(true), getSelf()); // Acknowledge the write
    }

    /**
     * Handles a request to retrieve a replica of a data item from the local store.
     */
    private void onGetReplica(Messages.GetReplica msg) {
        DataItem item = localStore.get(msg.key);
        getSender().tell(new Messages.GetReplicaResponse(item), getSelf());
    }

    // --------------------
    // Helpers
    // --------------------

    /**
     * A helper that transforms a list of futures into a single future that completes when a quorum is reached.
     *
     * @param futures       The list of {@link CompletionStage}s to wait for.
     * @param quorum        The number of successful results required.
     * @param expectedClass The class of the expected successful result.
     * @param <T>           The type of the expected result.
     * @return A {@link CompletionStage} that completes with a list of results when the quorum is met,
     * or completes exceptionally if the quorum cannot be reached.
     */
    private <T> CompletionStage<List<T>> collectQuorum(List<CompletionStage<Object>> futures, int quorum, Class<T> expectedClass) {
        CompletableFuture<List<T>> promise = new CompletableFuture<>();
        List<T> results = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);
        int totalFutures = futures.size();

        if (quorum <= 0) {
            promise.complete(Collections.emptyList());
            return promise;
        }
        if (quorum > totalFutures) {
            promise.completeExceptionally(new IllegalArgumentException("Quorum size cannot be greater than the number of futures."));
            return promise;
        }

        for (CompletionStage<Object> f : futures) {
            f.whenComplete((response, exception) -> {
                if (promise.isDone()) return;

                if (exception == null && expectedClass.isInstance(response)) {
                    results.add(expectedClass.cast(response));
                    if (successes.incrementAndGet() >= quorum) {
                        promise.complete(results);
                    }
                } else {
                    if (failures.incrementAndGet() > totalFutures - quorum) {
                        promise.completeExceptionally(new TimeoutException("Could not achieve quorum of " + quorum + " from " + totalFutures));
                    }
                }
            });
        }
        return promise;
    }

    /**
     * A helper to wrap a {@link CompletableFuture} with a timeout using the Akka scheduler.
     *
     * @param future  The future to wrap.
     * @param timeout The timeout value.
     * @param unit    The time unit for the timeout.
     * @param <T>     The type of the future's result.
     * @return A new {@link CompletableFuture} that will complete with the original future's result
     * or be completed exceptionally with a {@link TimeoutException}.
     */
    private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> future, long timeout, TimeUnit unit) {
        CompletableFuture<T> timeoutFuture = new CompletableFuture<>();
        getContext().system().scheduler().scheduleOnce(
                // Use scala's duration for Akka scheduler to ensure Java 8 compatibility
                scala.concurrent.duration.Duration.create(timeout, unit),
                () -> timeoutFuture.completeExceptionally(new TimeoutException("Operation timed out")),
                getContext().dispatcher()
        );

        return future.applyToEither(timeoutFuture, java.util.function.Function.identity());
    }

    private List<Messages.NodeInfo> findResponsibleNodes(Long key, int replication) {
        List<Messages.NodeInfo> res = new ArrayList<>();
        if (membership.isEmpty()) return res;

        int n = membership.size();
        int idx = -1;
        for (int i = 0; i < n; i++) {
            if (Long.compareUnsigned(membership.get(i).nodeKey, key) >= 0) {
                idx = i;
                break;
            }
        }
        if (idx == -1) idx = 0;

        int toTake = Math.min(replication, n);
        for (int i = 0; i < toTake; i++) {
            res.add(membership.get((idx + i) % n));
        }
        return res;
    }

    private void rebalance() {
        Set<Long> keys = new HashSet<>(localStore.getKeys());
        for (Long dataKey : keys) {
            List<Messages.NodeInfo> responsible = findResponsibleNodes(dataKey, this.replicationFactor);
            boolean iShouldHold = responsible.stream().anyMatch(ni -> ni.ref.equals(getSelf()));

            if (!iShouldHold) {
                DataItem item = localStore.get(dataKey);
                if (item != null) {
                    for (Messages.NodeInfo ni : responsible) {
                        ni.ref.tell(new Messages.StoreReplica(item), getSelf());
                    }
                }
                localStore.remove(dataKey);
                System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] rebalance: removed and forwarded item " + Long.toUnsignedString(dataKey));
            }
        }
    }
}
