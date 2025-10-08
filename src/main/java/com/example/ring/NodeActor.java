package com.example.ring;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.PatternsCS;
import akka.util.Timeout;

import java.time.Duration;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Represents a single node in the distributed data store ring.
 *
 * <p>Each {@code NodeActor} is responsible for a segment of the hash ring, identified by its {@code nodeKey}.
 * Its primary duties include:
 * <ul>
 *     <li><b>Data Storage:</b> Maintaining a local key-value store ({@link LocalStore}) for data items for which it is a replica.
 *     This includes storing new replicas ({@link #onStoreReplica(Messages.StoreReplica)}) and retrieving them
 *     ({@link #onGetReplica(Messages.GetReplica)}).</li>
 *     <li><b>Request Coordination:</b> Handling client requests for data operations. For a PUT operation
 *     ({@link #onDataPutRequest(Messages.DataPutRequest)}), it identifies the N responsible nodes and sends them the data to store,
 *     waiting for a write quorum (W) of acknowledgements. For a GET operation ({@link #onDataGetRequest(Messages.DataGetRequest)}),
 *     it requests the data from the N responsible nodes and waits for a read quorum (R) of responses.</li>
 *     <li><b>Membership Management:</b> Receiving and processing membership updates from the {@link RingManager}
 *     ({@link #onUpdateMembership(Messages.UpdateMembership)}). The membership list is kept sorted by node keys to enable
 *     consistent hashing.</li>
 *     <li><b>Data Rebalancing:</b> When the ring membership changes, the node re-evaluates its locally stored data.
 *     If it is no longer a responsible replica for a data item, it forwards that item to the new correct nodes
 *     and removes it from its local store ({@link #rebalance()}). This ensures data is correctly redistributed
 *     as nodes join and leave the ring.</li>
 * </ul>
 * <p>The actor uses a quorum-based system (N, W, R) to ensure data consistency and availability across the distributed
 * system. All network operations are performed asynchronously with timeouts to prevent the system from blocking.
 * Note that this implementation is for demonstration purposes and may not cover all edge cases of a production system.
 */
public class NodeActor extends AbstractActor {

    private final int id;
    private final long nodeKey; // unsigned key stored in a long
    private int replicationFactor;

    private static final int N = 3; // replication factor
    private static final int W = 2; // write quorum
    private static final int R = 2; // read quorum
    private static final Duration T = Duration.ofSeconds(3);

    // membership (sorted by nodeKey, ascending unsigned)
    private List<Messages.NodeInfo> membership = new ArrayList<>();

    // localStore: dataKey -> (itemKey -> value)
    //private final Map<Long, Map<String, String>> localStore = new HashMap<>();
    private LocalStore localStore = new LocalStore();

    /**
     * Constructs a NodeActor.
     *
     * @param id                A unique identifier for the node (for logging/debugging).
     * @param nodeKey           The key that determines the node's position in the ring.
     * @param replicationFactor The total number of replicas for each data item (N).
     */
    private NodeActor(int id, long nodeKey, int replicationFactor) {
        this.id = id;
        this.nodeKey = nodeKey;
        this.replicationFactor = replicationFactor;
    }

    /**
     * Creates a {@link Props} object for creating a {@link NodeActor}.
     *
     * @param id                A unique identifier for the node.
     * @param nodeKey           The key for the node in the ring.
     * @param replicationFactor The replication factor for the system.
     * @return A {@link Props} configuration object for the NodeActor.
     */
    public static Props props(int id, long nodeKey, int replicationFactor) {
        return Props.create(NodeActor.class, () -> new NodeActor(id, nodeKey, replicationFactor));
    }

    /**
     * Defines the message-handling behavior of the actor.
     *
     * @return A {@link Receive} object that maps message types to handler methods.
     */
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

    /**
     * Handles a membership update from the {@link RingManager}.
     * <p>It sorts the new membership list and triggers a data rebalance.
     *
     * @param msg The {@link Messages.UpdateMembership} message containing the new list of nodes.
     */
    private void onUpdateMembership(Messages.UpdateMembership msg) {
        // copy and sort membership by unsigned nodeKey
        List<Messages.NodeInfo> sorted = new ArrayList<>(msg.nodes);
        sorted.sort((a, b) -> Long.compareUnsigned(a.nodeKey, b.nodeKey));
        this.membership = sorted;
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] membership updated: " + membership);
        // rebalance local data based on new membership
        rebalance();
    }

    /**
     * Handles a request to put data into the ring.
     * <p>This node acts as the coordinator, finding the responsible nodes and sending them a
     * {@link Messages.StoreReplica} message. It waits for a write quorum (W) of acknowledgements
     * before confirming the success of the operation to the original sender.
     *
     * @param req The {@link Messages.DataPutRequest} containing the data item to store.
     */
    private void onDataPutRequest(Messages.DataPutRequest req) {
        final ActorRef replyTo = getSender();  // save a ref to the sender, otherwise it will be Null in whenComplete callback
        List<Messages.NodeInfo> responsible = findResponsibleNodes(req.item.getKey(), req.replicationFactor);

        // send store instructions to all responsible nodes
        List<CompletionStage<Object>> futures = new ArrayList<>();
        for (Messages.NodeInfo ni : responsible) {
            CompletionStage<Object> f = PatternsCS.ask(
                    ni.ref,
                    new Messages.StoreReplica(req.item),
                    Timeout.create(T)
            );
            futures.add(f);
        }

        // wait for W ack replies
        CompletableFuture<Void> quorumFuture =
                CompletableFuture.allOf(
                        futures.stream()
                                .limit(W)
                                .map(CompletionStage::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                );

        withTimeout(quorumFuture, T.getSeconds(), TimeUnit.SECONDS)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        replyTo.tell(new Messages.PutAck(false), getSelf());
                    } else {
                        replyTo.tell(new Messages.PutAck(true), getSelf());
                    }
                });

    }

    /**
     * Handles a request to store a replica of a data item.
     * <p>This message is sent by a coordinator node to a replica node. This node simply
     * stores the data item in its local store.
     *
     * @param msg The {@link Messages.StoreReplica} message containing the data item.
     */
    private void onStoreReplica(Messages.StoreReplica msg) {
        localStore.store(msg.item);
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] StoreReplica stored: " + msg.item.getKey() + "=" + msg.item.getValue());
    }

    /**
     * Handles a request to retrieve a replica of a data item from the local store.
     *
     * @param msg The {@link Messages.GetReplica} message containing the key of the item.
     */
    private void onGetReplica(Messages.GetReplica msg) {
        String value = null;
        DataItem item = localStore.get(msg.key);

        getSender().tell(new Messages.GetReplicaResponse(item), getSelf());
    }

    /**
     * Handles a request to get data from the ring.
     * <p>This node acts as the coordinator, finding the responsible nodes and sending them a
     * {@link Messages.GetReplica} message. It waits for a read quorum (R) of responses and
     * returns the first valid response to the original sender.
     *
     * @param req The {@link Messages.DataGetRequest} containing the key of the data to retrieve.
     */
    private void onDataGetRequest(Messages.DataGetRequest req) {
        final ActorRef replyTo = getSender();  // save a ref to the sender, otherwise it will be Null in whenComplete callback
        List<Messages.NodeInfo> responsible = findResponsibleNodes(req.key, req.replicationFactor);

        List<CompletionStage<Object>> futures = new ArrayList<>();
        for (Messages.NodeInfo ni : responsible) {
            CompletionStage<Object> f = PatternsCS.ask(
                    ni.ref,
                    new Messages.GetReplica(req.key),
                    Timeout.create(T)
            );
            futures.add(f);
        }

        // quando arrivano almeno R risposte, ritorna la prima valida
        CompletableFuture<Object> quorumRead =
                (CompletableFuture<Object>) CompletableFuture.anyOf(
                        futures.stream()
                                .limit(R)
                                .map(CompletionStage::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                );

        withTimeout(quorumRead, T.getSeconds(), TimeUnit.SECONDS)
                .whenComplete((resp, ex) -> {
                    if (ex != null) {
                        DataItem tmp = new DataItem(0, req.key, null);
                        replyTo.tell(new Messages.DataGetResponse(tmp), getSelf());
                    } else if (resp instanceof Messages.GetReplicaResponse) {
                        Messages.GetReplicaResponse r = (Messages.GetReplicaResponse) resp;
                        replyTo.tell(new Messages.DataGetResponse(r.item), getSelf());
                    }
                });

    }


    // --------------------
    // Helpers
    // --------------------

    /**
     * A helper to wrap a {@link CompletableFuture} with a timeout.
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
                scala.concurrent.duration.Duration.create(timeout, unit),
                () -> timeoutFuture.completeExceptionally(new TimeoutException("Operation timed out")),
                getContext().dispatcher()
        );
        return CompletableFuture.anyOf(future, timeoutFuture)
                .thenApply(o -> (T) o);
    }


    /**
     * Finds the set of N nodes responsible for a given data key using consistent hashing.
     * <p>It finds the first node in the sorted membership list whose key is greater than or equal to the data key
     * and then takes the next N-1 nodes in sequence (wrapping around if necessary).
     *
     * @param key         The data key for which to find responsible nodes.
     * @param replication The number of responsible nodes to find (N).
     * @return A list of {@link Messages.NodeInfo} objects for the responsible nodes.
     */
    private List<Messages.NodeInfo> findResponsibleNodes(Long key, int replication) {
        List<Messages.NodeInfo> res = new ArrayList<>();
        if (membership.isEmpty()) return res;

        int n = membership.size();

        // find first index with nodeKey >= dataKey (unsigned compare)
        int idx = -1;
        for (int i = 0; i < n; i++) {
            if (Long.compareUnsigned(membership.get(i).nodeKey, key) >= 0) {
                idx = i;
                break;
            }
        }
        if (idx == -1) idx = 0; // wrap

        int toTake = Math.min(replication, n);
        for (int i = 0; i < toTake; i++) {
            res.add(membership.get((idx + i) % n));
        }
        return res;
    }

    /**
     * Rebalances the data stored on this node after a membership change.
     * <p>For each data item in the local store, it recalculates the responsible nodes based on the new
     * membership. If this node is no longer responsible for an item, it forwards the item to the new
     * responsible nodes and removes it from its local store.
     */
    private void rebalance() {
        // copy keys to avoid concurrent modification
        Set<Long> keys = new HashSet<>(localStore.getKeys());
        for (Long dataKey : keys) {
            List<Messages.NodeInfo> responsible = findResponsibleNodes(dataKey, this.replicationFactor);
            boolean iShouldHold = false;
            for (Messages.NodeInfo ni : responsible) {
                if (ni.ref.equals(getSelf())) {
                    iShouldHold = true;
                    break;
                }
            }
            if (!iShouldHold) {
                // forward item to responsible nodes
                DataItem item = localStore.get(dataKey);
                if (item != null) {
                    for (Messages.NodeInfo ni : responsible) {
                        // skip sending back to a node that is ourselves (we already determined it's not responsible)
                        if (!ni.ref.equals(getSelf())) {
                            ni.ref.tell(new Messages.StoreReplica(item), getSelf());
                        }
                    }
                }
                // remove local copy
                localStore.remove(dataKey);
                System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] rebalance: removed item " + Long.toUnsignedString(dataKey));
            }
        }
    }
}
