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
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.example.ring.NetworkSimulator.sendWithDelay;

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
    private final int W;
    private final int R;
    private final Duration T;

    private List<Messages.NodeInfo> membership = new ArrayList<>();
    private final LocalStore localStore = new LocalStore();
    private boolean hasJoined = false;

    private NodeActor(int id, long nodeKey, int replicationFactor, int W, int R, Duration T) {
        this.id = id;
        this.nodeKey = nodeKey;
        this.replicationFactor = replicationFactor;
        this.W = W;
        this.R = R;
        this.T = T;
    }

    public static Props props(int id, long nodeKey, int replicationFactor, int W, int R, Duration T) {
        return Props.create(NodeActor.class, () -> new NodeActor(id, nodeKey, replicationFactor, W, R, T));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.UpdateMembership.class, this::onUpdateMembership)
                .match(Messages.DataPutRequest.class, this::onDataPutRequest)
                .match(Messages.StoreReplica.class, this::onStoreReplica)
                .match(Messages.DataGetRequest.class, this::onDataGetRequest)
                .match(Messages.GetReplica.class, this::onGetReplica)
                .match(Messages.CrashNode.class, this::onCrash)
                .match(Messages.RecoverNode.class, this::onRecover)
                .match(Messages.GetTopology.class, this::onGetTopology)
                .match(Messages.GetAllData.class, this::onGetAllData)
                .match(Messages._debug_GetStoredItems.class, this::onDebugGetAllData)
                .match(Messages.Leave.class, this::onLeave)
                .build();
    }

    private void onLeave(Messages.Leave msg) {
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] leaving gracefully. Transferring data...");

        for (DataItem item : localStore.getAll()) {
            List<Messages.NodeInfo> responsible = findResponsibleNodes(item.getKey(), replicationFactor);
            for (Messages.NodeInfo ni : responsible) {
                if (!ni.ref.equals(getSelf())) { // Don't send to self
                    sendWithDelay(getContext(), ni.ref,new Messages.StoreReplica(item), getSelf());
                }
            }
        }
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] data transfer initiated. Stopping actor.");
        getContext().stop(getSelf());
    }

    private Receive crashedReceive() {
        return receiveBuilder()
                .match(Messages.RecoverNode.class, this::onRecover)
                .build();
    }

    private void onDebugGetAllData(Messages._debug_GetStoredItems debugGetStoredItems) {
        System.out.println("[DEBUG] Stored items in node " + Long.toUnsignedString(nodeKey) + ": " + localStore);
    }

    private void onGetTopology(Messages.GetTopology getTopology) {
        final ActorRef replyTo = getSender();
        sendWithDelay(getContext(), replyTo, new Messages.UpdateMembership(new ArrayList<>(membership)), getSelf());
    }

    private void onGetAllData(Messages.GetAllData msg) {
        sendWithDelay(getContext(), getSender(), new Messages.AllDataResponse(new ArrayList<DataItem>(localStore.getAll())), getSelf());
    }

    private Receive recoveringReceive() {
        return receiveBuilder()
                .match(Messages.UpdateMembership.class, this::recoveringUpdateMembership)
                .build();
    }

    private void onRecover(Messages.RecoverNode recoverNode) {
        getContext().become(recoveringReceive());
        for (Messages.NodeInfo ni : membership) {
            sendWithDelay(getContext(), ni.ref, new Messages.GetTopology(), getSelf());
        }
    }

    private void onCrash(Messages.CrashNode crashNode) {
        getContext().become(crashedReceive());
    }

    /**
     * Handles a membership update while the node is in a recovering state.
     * This method orchestrates the data recovery process.
     *
     * @param msg The latest membership update.
     */
    private void recoveringUpdateMembership(Messages.UpdateMembership msg) {
        // Phase 1: Rebalance existing data based on the new membership.
        // This ensures that any data this node *should no longer* hold is forwarded.
        onUpdateMembership(msg);

        // Phase 2: Query a predecessor node to retrieve data that this node is now
        // responsible for, but might have missed while crashed.

        // Find this node's position in the sorted membership list.
        int i = 0;
        for (i = 0; i < membership.size(); i++) {
            if (membership.get(i).nodeKey == this.nodeKey)
                break;
        }

        // If membership is empty or this node isn't found (shouldn't happen if membership is valid),
        // log an error and exit recovery.
        if (membership.isEmpty() || i == membership.size()) {
            System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] recovered, but membership is empty or node not found.");
            getContext().unbecome();
            return;
        }

        // Determine the direct predecessor in the ring. This node is likely to hold
        // data that this recovering node is now responsible for.
        // The modulo operator handles wrapping around the ring.
        int predecessorIndex = (i - replicationFactor + 1 + membership.size()) % membership.size();
        ActorRef predecessorRef = membership.get(predecessorIndex).ref;

        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] recovering: querying predecessor " + Long.toUnsignedString(membership.get(predecessorIndex).nodeKey) + " for data.");

        // Send a request to the predecessor to get all its data.
        // Use PatternsCS.ask for an asynchronous request-response pattern.
        PatternsCS.ask(predecessorRef, new Messages.GetAllData(), T)
                .whenComplete((response, ex) -> {
                    if (ex == null && response instanceof Messages.AllDataResponse) {
                        Messages.AllDataResponse allData = (Messages.AllDataResponse) response;
                        // Iterate through the received data items.
                        for (DataItem item : allData.data) {
                            // For each item, determine if this node is now responsible for it
                            // based on the current (recovered) membership and replication factor.
                            List<Messages.NodeInfo> responsibleNodes = findResponsibleNodes(item.getKey(), replicationFactor);
                            if (responsibleNodes.stream().anyMatch(ni -> ni.ref.equals(getSelf()))) {
                                // If this node is responsible, store the item locally.
                                localStore.store(item);
                                System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] recovered: stored item " + Long.toUnsignedString(item.getKey()));
                            }
                        }
                        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] recovered successfully.");
                    } else {
                        // Log any errors during data retrieval from the predecessor.
                        System.err.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] recovery failed: " + (ex != null ? ex.getMessage() : "Unexpected response type."));
                    }
                    // Exit the recovering state, regardless of whether data recovery was fully successful.
                    // The node is now ready to process normal messages.
                    getContext().unbecome();
                });
    }

    private void onUpdateMembership(Messages.UpdateMembership msg) {
        List<Messages.NodeInfo> sorted = new ArrayList<>(msg.nodes);
        sorted.sort(Comparator.comparing(a -> a.nodeKey, Long::compareUnsigned));
        this.membership = sorted;
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] membership updated: " + membership);

        if (!hasJoined) {
            hasJoined = true;
            joinRing();
        } else {
            rebalance();
        }
    }

    private void joinRing() {
        if (membership.size() <= 1) {
            return; // Nothing to do if we are the only node
        }

        int selfIndex = -1;
        for (int i = 0; i < membership.size(); i++) {
            if (membership.get(i).ref.equals(getSelf())) {
                selfIndex = i;
                break;
            }
        }

        if (selfIndex == -1) return; // Should not happen

        int successorIndex = (selfIndex + 1) % membership.size();
        ActorRef successor = membership.get(successorIndex).ref;

        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] joining: querying successor for data.");

        PatternsCS.ask(successor, new Messages.GetAllData(), T)
                .whenComplete((response, ex) -> {
                    if (ex == null && response instanceof Messages.AllDataResponse) {
                        Messages.AllDataResponse allData = (Messages.AllDataResponse) response;
                        for (DataItem item : allData.data) {
                            List<Messages.NodeInfo> responsibleNodes = findResponsibleNodes(item.getKey(), replicationFactor);
                            if (responsibleNodes.stream().anyMatch(ni -> ni.ref.equals(getSelf()))) {
                                localStore.store(item);
                                System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] joining: stored item " + Long.toUnsignedString(item.getKey()));
                            }
                        }
                        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] joined successfully.");
                    } else {
                        System.err.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] join failed: " + (ex != null ? ex.getMessage() : "Unexpected response type."));
                    }
                });
    }

    private void onDataGetRequest(Messages.DataGetRequest req) {
        final ActorRef replyTo = getSender();
        final Long key = req.key;
        final List<Messages.NodeInfo> responsible = findResponsibleNodes(key, req.replicationFactor);

        CompletionStage<List<Messages.DataItemResponse>> quorumFuture = readReplicasFromQuorum(key, responsible, R);

        withTimeout(quorumFuture.toCompletableFuture(), T.getSeconds(), TimeUnit.SECONDS)
                .whenComplete((responses, ex) -> {
                    if (ex != null) {
                        // On timeout or failure to achieve quorum, reply with a not-found item.
                        sendWithDelay(getContext(), replyTo, new Messages.DataItemResponse(new DataItem(0, key, null)), getSelf());
                    } else {
                        DataItem latestItem = getLatestItem(responses, key);
                        sendWithDelay(getContext(), replyTo, new Messages.DataItemResponse(latestItem), getSelf());
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
                        sendWithDelay(getContext(), replyTo, new Messages.PutAck(false), getSelf());
                    } else {
                        sendWithDelay(getContext(), replyTo, new Messages.PutAck(true), getSelf());
                    }
                });
    }

    /**
     * Handles a request to store a replica of a data item and sends an acknowledgement.
     */
    private void onStoreReplica(Messages.StoreReplica msg) {
        localStore.store(msg.item);
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] StoreReplica stored: " + msg.item.getKey() + "=" + msg.item.getValue() + " v" + msg.item.getVersion());
        sendWithDelay(getContext(), getSender(), new Messages.PutAck(true), getSelf());
    }

    /**
     * Handles a request to retrieve a replica of a data item from the local store.
     */
    private void onGetReplica(Messages.GetReplica msg) {
        DataItem item = localStore.get(msg.key);
        sendWithDelay(getContext(), getSender(), new Messages.DataItemResponse(item), getSelf());
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
                .orElse(new DataItem(0, key, null));
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

        return future.applyToEither(timeoutFuture, Function.identity());
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
        int insertionPoint = n;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            if (Long.compareUnsigned(membership.get(mid).nodeKey, key) >= 0) {
                insertionPoint = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
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
            System.out.println("[DEBUG] responsible computated for the key " + Long.toUnsignedString(dataKey) + ": " + responsible);
            boolean iShouldHold = responsible.stream().anyMatch(ni -> ni.ref.equals(getSelf()));

            if (!iShouldHold) {
                DataItem item = localStore.get(dataKey);
                if (item != null) {
                    // This node is no longer a replica for dataKey, so forward the item to the new responsible nodes.
                    for (Messages.NodeInfo ni : responsible) {
                        sendWithDelay(getContext(), ni.ref, new Messages.StoreReplica(item), getSelf());
                    }
                }
                localStore.remove(dataKey);
                System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] rebalance: removed and forwarded item " + Long.toUnsignedString(dataKey));
            }
        }
    }
}
