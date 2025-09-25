package com.example.ring;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.PatternsCS;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

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

    // membership (sorted by nodeKey, ascending unsigned)
    private List<Messages.NodeInfo> membership = new ArrayList<>();

    // localStore: dataKey -> (itemKey -> value)
    private final Map<Long, Map<String, String>> localStore = new HashMap<>();

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
        List<Messages.NodeInfo> responsible = findResponsibleNodes(req.dataKey, req.replicationFactor);
        // send store instructions to all responsible nodes
        for (Messages.NodeInfo ni : responsible) {
            if (ni.ref.equals(getSelf())) {
                // store locally
                storeLocally(req.dataKey, req.key, req.value);
                System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] locally stored (primary) " +
                        Long.toUnsignedString(req.dataKey) + " : " + req.key + "=" + req.value);
            } else {
                ni.ref.tell(new Messages.StoreReplica(req.dataKey, req.key, req.value), getSelf());
                System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] forwarded StoreReplica for key " +
                        req.key + " to " + Long.toUnsignedString(ni.nodeKey));
            }
        }
        // ack to sender
        getSender().tell(new Messages.PutAck(true), getSelf());
    }

    private void onStoreReplica(Messages.StoreReplica msg) {
        storeLocally(msg.dataKey, msg.key, msg.value);
        System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] StoreReplica stored: " +
                Long.toUnsignedString(msg.dataKey) + " : " + msg.key + "=" + msg.value);
    }

    private void onGetReplica(Messages.GetReplica msg) {
        String value = null;
        Map<String, String> bucket = localStore.get(msg.dataKey);
        if (bucket != null) value = bucket.get(msg.key);
        getSender().tell(new Messages.GetReplicaResponse(msg.key, value), getSelf());
    }

    private void onDataGetRequest(Messages.DataGetRequest req) {
        List<Messages.NodeInfo> responsible = findResponsibleNodes(req.dataKey, req.replicationFactor);

        // try each responsible node in order until a non-null value is returned
        for (Messages.NodeInfo ni : responsible) {
            if (ni.ref.equals(getSelf())) {
                Map<String, String> bucket = localStore.get(req.dataKey);
                if (bucket != null && bucket.containsKey(req.key)) {
                    getSender().tell(new Messages.DataGetResponse(req.key, bucket.get(req.key)), getSelf());
                    return;
                }
            } else {
                try {
                    CompletionStage<Object> future = PatternsCS.ask(ni.ref, new Messages.GetReplica(req.dataKey, req.key), Duration.ofSeconds(2));
                    Object reply = future.toCompletableFuture().get(2, TimeUnit.SECONDS);
                    if (reply instanceof Messages.GetReplicaResponse) {
                        Messages.GetReplicaResponse r = (Messages.GetReplicaResponse) reply;
                        if (r.value != null) {
                            getSender().tell(new Messages.DataGetResponse(r.key, r.value), getSelf());
                            return;
                        }
                    }
                } catch (Exception ex) {
                    // timeouts or other errors — try next node
                }
            }
        }
        // not found among responsible replicas
        getSender().tell(new Messages.DataGetResponse(req.key, null), getSelf());
    }

    // --------------------
    // Helpers
    // --------------------

    private void storeLocally(long dataKey, String key, String value) {
        Map<String, String> bucket = localStore.computeIfAbsent(dataKey, k -> new HashMap<>());
        bucket.put(key, value);
    }

    /**
     * Find the set of responsible nodes (N nodes) for a given dataKey.
     * Assumes membership is sorted ascending by unsigned nodeKey.
     */
    private List<Messages.NodeInfo> findResponsibleNodes(long dataKey, int replication) {
        List<Messages.NodeInfo> res = new ArrayList<>();
        if (membership.isEmpty()) return res;

        int n = membership.size();

        // find first index with nodeKey >= dataKey (unsigned compare)
        int idx = -1;
        for (int i = 0; i < n; i++) {
            if (Long.compareUnsigned(membership.get(i).nodeKey, dataKey) >= 0) {
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
        Set<Long> keys = new HashSet<>(localStore.keySet());
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
                // forward entire bucket to responsible nodes
                Map<String, String> bucket = localStore.get(dataKey);
                if (bucket != null) {
                    for (Map.Entry<String, String> e : bucket.entrySet()) {
                        for (Messages.NodeInfo ni : responsible) {
                            // skip sending back to a node that is ourselves (we already determined it's not responsible)
                            if (!ni.ref.equals(getSelf())) {
                                ni.ref.tell(new Messages.StoreReplica(dataKey, e.getKey(), e.getValue()), getSelf());
                            }
                        }
                    }
                }
                // remove local copy
                localStore.remove(dataKey);
                System.out.println("[node " + id + " (" + Long.toUnsignedString(nodeKey) + ")] rebalance: removed bucket " + Long.toUnsignedString(dataKey));
            }
        }
    }
}
