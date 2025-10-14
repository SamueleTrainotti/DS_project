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

    // FIXME: These values are hardcoded. For more flexibility, they could be passed via configuration.
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
     * This node acts as the coordinator.
     */
    private void onDataGetRequest(Messages.DataGetRequest req) {
        final ActorRef replyTo = getSender();
        final Long key = req.key;
        final List<Messages.NodeInfo> responsible = findResponsibleNodes(key, req.replicationFactor);

        CompletionStage<List<Messages.DataItemResponse>> quorumFuture = readReplicasFromQuorum(key, responsible, R);

        withTimeout(quorumFuture.toCompletableFuture(), T.getSeconds(), TimeUnit.SECONDS)
                .whenComplete((responses, ex) -> {
                    if (ex != null) {
                        // On timeout or failure to achieve quorum, reply with a not-found item.
                        replyTo.tell(new Messages.DataItemResponse(new DataItem(0, key, null)), getSelf());
                    } else {
                        DataItem latestItem = getLatestItem(responses, key);
                        replyTo.tell(new Messages.DataItemResponse(latestItem), getSelf());
                    }
                });
    }

    /**
     * Handles a request to put data into the ring, ensuring sequential consistency.
     * This implements a two-phase write process.
     */
    private void onDataPutRequest(Messages.DataPutRequest req) {
        final ActorRef replyTo = getSender();
        final List<Messages.NodeInfo> responsible = findResponsibleNodes(req.item.getKey(), req.replicationFactor);

        CompletionStage<List<Messages.PutAck>> finalFuture = getNewDataItem(req, responsible)
                .thenCompose(newItem -> writeItemToQuorum(newItem, responsible));

        withTimeout(finalFuture.toCompletableFuture(), T.getSeconds() * 2, TimeUnit.SECONDS)
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
        getSender().tell(new Messages.DataItemResponse(item), getSelf());
    }

    // --------------------
    // Asynchronous Operation Helpers
    // --------------------

    /**
     * Phase 1 of GET: Reads replicas from a quorum of R nodes.
     */
    private CompletionStage<List<Messages.DataItemResponse>> readReplicasFromQuorum(Long key, List<Messages.NodeInfo> responsible, int quorumSize) {
        List<CompletionStage<Object>> futures = responsible.stream()
                .map(nodeInfo -> PatternsCS.ask(nodeInfo.ref, new Messages.GetReplica(key), T))
                .collect(Collectors.toList());
        return collectQuorum(futures, quorumSize, Messages.DataItemResponse.class);
    }

    /**
     * Processes responses from a GET operation to find the item with the highest version.
     */
    private DataItem getLatestItem(List<Messages.DataItemResponse> responses, Long key) {
        return responses.stream()
                .map(response -> response.item)
                .filter(Objects::nonNull)
                .max(Comparator.comparing(DataItem::getVersion))
                .orElse(new DataItem(0, key, null)); // Or not-found if all were null
    }

    /**
     * Phase 1 of PUT: Reads versions from a quorum of W nodes to determine the next version.
     */
    private CompletionStage<DataItem> getNewDataItem(Messages.DataPutRequest req, List<Messages.NodeInfo> responsible) {
        Long key = req.item.getKey();
        CompletionStage<List<Messages.DataItemResponse>> readQuorumFuture = readReplicasFromQuorum(key, responsible, W);

        return readQuorumFuture.thenApply(responses -> {
            int maxVersion = responses.stream()
                    .map(response -> response.item)
                    .filter(Objects::nonNull)
                    .mapToInt(DataItem::getVersion)
                    .max()
                    .orElse(0);
            return new DataItem(maxVersion + 1, key, req.item.getValue());
        });
    }

    /**
     * Phase 2 of PUT: Writes the new data item to a quorum of W nodes.
     */
    private CompletionStage<List<Messages.PutAck>> writeItemToQuorum(DataItem newItem, List<Messages.NodeInfo> responsible) {
        List<CompletionStage<Object>> writeFutures = responsible.stream()
                .map(nodeInfo -> PatternsCS.ask(nodeInfo.ref, new Messages.StoreReplica(newItem), T))
                .collect(Collectors.toList());
        return collectQuorum(writeFutures, W, Messages.PutAck.class);
    }


    // --------------------
    // General Helpers
    // --------------------

    /**
     * A helper that transforms a list of futures into a single future that completes when a quorum is reached.
     */
    private <T> CompletionStage<List<T>> collectQuorum(List<CompletionStage<Object>> futures, int quorum, Class<T> expectedClass) {
        CompletableFuture<List<T>> promise = new CompletableFuture<>();
        List<T> results = Collections.synchronizedList(new ArrayList<>());
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
                    if (results.size() >= quorum) {
                        promise.complete(results);
                    }
                } else {
                    // The quorum cannot be met if the number of failures exceeds the maximum allowed failures.
                    // Max allowed failures = totalFutures - quorum.
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
     */
    private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> future, long timeout, TimeUnit unit) {
        CompletableFuture<T> timeoutFuture = new CompletableFuture<>();
        getContext().system().scheduler().scheduleOnce(
                // Use scala's duration for Akka scheduler
                scala.concurrent.duration.Duration.create(timeout, unit),
                () -> timeoutFuture.completeExceptionally(new TimeoutException("Operation timed out")),
                getContext().dispatcher()
        );

        return future.applyToEither(timeoutFuture, java.util.function.Function.identity());
    }

    /**
     * Finds the N successor nodes for a given key, starting from the first node whose key is >= the data key.
     * This implementation uses a binary search for efficiency.
     */
    private List<Messages.NodeInfo> findResponsibleNodes(Long key, int replication) {
        List<Messages.NodeInfo> res = new ArrayList<>();
        if (membership.isEmpty()) {
            return res;
        }

        int n = membership.size();
        int idx;

        // Find the first node with a key >= the data key using binary search.
        // This is more efficient than a linear scan for a large number of nodes.
        int low = 0;
        int high = n - 1;
        int insertionPoint = n; // Default to n, which wraps around to 0.

        while (low <= high) {
            int mid = low + (high - low) / 2;
            if (Long.compareUnsigned(membership.get(mid).nodeKey, key) >= 0) {
                insertionPoint = mid;
                high = mid - 1; // Continue searching in the left half to find the *first* such node.
            } else {
                low = mid + 1; // Search in the right half.
            }
        }

        // The insertion point is the starting node. Wrap around if needed.
        idx = insertionPoint % n;

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
                    // This node is no longer a replica for dataKey, so forward the item to the new responsible nodes.
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
