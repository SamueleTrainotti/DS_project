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
 * NodeActor: holds a nodeKey (unsigned), a local store, and a view of full membership.
 * Minimal rebalance: when membership changes, items this node should not hold are forwarded
 * to new responsible nodes and removed locally.
 *
 * NOTE (demo): this actor may perform blocking waits (for simplification). Not production style.
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
        // copy and sort membership by unsigned nodeKey
        List<Messages.NodeInfo> sorted = new ArrayList<>(msg.nodes);
        sorted.sort((a, b) -> Long.compareUnsigned(a.nodeKey, b.nodeKey));
        this.membership = sorted;
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] membership updated: " + membership);
        // rebalance local data based on new membership
        rebalance();
    }

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

    private void onStoreReplica(Messages.StoreReplica msg) {
        localStore.store(msg.item);
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] StoreReplica stored: " + msg.item.getKey() + "=" + msg.item.getValue());
    }

    private void onGetReplica(Messages.GetReplica msg) {
        String value = null;
        DataItem item = localStore.get(msg.key);

        getSender().tell(new Messages.GetReplicaResponse(item), getSelf());
    }

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
     * Find the set of responsible nodes (N nodes) for a given dataKey.
     * Assumes membership is sorted ascending by unsigned nodeKey.
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
     * Minimal rebalance: for every dataKey we currently hold, compute the new responsible nodes.
     * If this node is no longer responsible, forward all items in that bucket to the responsible nodes
     * and remove the bucket locally.
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

