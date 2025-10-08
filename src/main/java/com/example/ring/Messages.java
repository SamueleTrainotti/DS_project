package com.example.ring;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

/**
 * A container for all the message classes used for communication between actors in the ring system.
 * <p>All messages must be {@link Serializable} to be sent across the network.
 */
public class Messages {

    /**
     * Holds information about a single node in the ring.
     */
    public static class NodeInfo implements Serializable {
        /** The key that determines the node's position in the ring. */
        public final long nodeKey; // unsigned value stored in long
        /** The {@link ActorRef} of the node actor. */
        public final ActorRef ref;

        /**
         * Constructs a NodeInfo object.
         *
         * @param nodeKey The node's key.
         * @param ref     The node's actor reference.
         */
        public NodeInfo(long nodeKey, ActorRef ref) {
            this.nodeKey = nodeKey;
            this.ref = ref;
        }

        @Override
        public String toString() {
            return "NodeInfo(" + Long.toUnsignedString(nodeKey) + ")";
        }
    }

    /**
     * A message sent from the {@link RingManager} to all nodes to inform them of a change in membership.
     */
    public static class UpdateMembership implements Serializable {
        /** A list of all nodes currently in the ring. */
        public final List<NodeInfo> nodes;

        /**
         * Constructs an UpdateMembership message.
         *
         * @param nodes The new list of nodes.
         */
        public UpdateMembership(List<NodeInfo> nodes) { this.nodes = nodes; }
    }

    // -------------------
    // Client API messages
    // -------------------

    /**
     * A request sent to a coordinator node to store a data item in the ring.
     */
    public static class DataPutRequest implements Serializable {
        /** The data item to be stored. */
        public final DataItem item;
        /** The replication factor for this operation. */
        public final int replicationFactor;

        /**
         * Constructs a DataPutRequest.
         *
         * @param item              The data item.
         * @param replicationFactor The replication factor.
         */
        public DataPutRequest(DataItem item, int replicationFactor) {
            this.item = item;
            this.replicationFactor = replicationFactor;
        }
    }

    /**
     * A request sent to a coordinator node to retrieve a data item from the ring.
     */
    public static class DataGetRequest implements Serializable {
        /** The key of the data item to retrieve. */
        public final Long key;
        /** The replication factor for this operation. */
        public final int replicationFactor;

        /**
         * Constructs a DataGetRequest.
         *
         * @param key               The key of the item.
         * @param replicationFactor The replication factor.
         */
        public DataGetRequest(Long key, int replicationFactor) {
            this.key = key;
            this.replicationFactor = replicationFactor;
        }
    }

    /**
     * A response to a {@link DataGetRequest}, containing the requested data item.
     */
    public static class DataGetResponse implements Serializable {
        /** The retrieved data item. The value may be null if the item was not found. */
        public final DataItem item;

        /**
         * Constructs a DataGetResponse.
         *
         * @param item The data item.
         */
        public DataGetResponse(DataItem item) {
            this.item = item;
        }
    }

    // ---------------------------
    // Internal replication messages
    // ---------------------------

    /**
     * An instruction from a coordinator to a replica node to store a copy of a data item.
     */
    public static class StoreReplica implements Serializable {
        /** The data item to be stored as a replica. */
        public final DataItem item;

        /**
         * Constructs a StoreReplica message.
         *
         * @param item The data item.
         */
        public StoreReplica(DataItem item) {
            this.item = item;
        }
    }

    /**
     * A request from a coordinator to a replica node to retrieve a copy of a data item.
     */
    public static class GetReplica implements Serializable {
        /** The key of the data item to retrieve. */
        public final Long key;

        /**
         * Constructs a GetReplica message.
         *
         * @param key The key of the item.
         */
        public GetReplica(Long key) {
            this.key = key;
        }
    }

    /**
     * A response from a replica node to a coordinator for a {@link GetReplica} request.
     */
    public static class GetReplicaResponse implements Serializable {
        /** The data item from the replica. */
        public final DataItem item;

        /**
         * Constructs a GetReplicaResponse.
         *
         * @param item The data item.
         */
        public GetReplicaResponse(DataItem item) {
            this.item  = item;
        }
    }

    /**
     * A generic acknowledgement message for a PUT operation, indicating success or failure.
     */
    public static class PutAck implements Serializable {
        public final boolean ok;
        public PutAck(boolean ok) { this.ok = ok; }
    }

    // ----------------
    // Manager messages
    // ----------------

    /**
     * A message to the {@link RingManager} to add a new node to the ring.
     */
    public static class AddNode implements Serializable {
        public final long nodeKey;
        public AddNode(long nodeKey) { this.nodeKey = nodeKey; }
    }

    /**
     * A message to the {@link RingManager} to remove a node from the ring.
     */
    public static class RemoveNode implements Serializable {
        public final long nodeKey;
        public RemoveNode(long nodeKey) { this.nodeKey = nodeKey; }
    }

    /**
     * A message to the {@link RingManager} to forward a PUT request to a specific origin node.
     */
    public static class ManagerPut implements Serializable {
        public final long originNodeKey;
        public final DataItem item;

        public ManagerPut(long originNodeKey, DataItem item) {
            this.originNodeKey = originNodeKey;
            this.item = item;
        }
    }

    /**
     * A message to the {@link RingManager} to forward a GET request to a specific origin node.
     */
    public static class ManagerGet implements Serializable {
        public final long originNodeKey;
        public final Long key;

        public ManagerGet(long originNodeKey, Long key) {
            this.originNodeKey = originNodeKey;
            this.key = key;
        }
    }

    // ------------------
    // Client-level API
    // ------------------

    /**
     * A high-level message from a client to update/put a data item.
     */
    public static class ClientUpdate implements Serializable {
        public final DataItem item;

        public ClientUpdate(DataItem item) {
            this.item = item;
        }
    }

    /**
     * A high-level message from a client to get a data item.
     */
    public static class ClientGet implements Serializable {
        public final Long key;

        public ClientGet(Long key) {
            this.key = key;
        }
    }

    /**
     * An acknowledgement from a single replica for a write operation.
     */
    public static class WriteAck implements Serializable {
        public final boolean ok;
        public WriteAck(boolean ok) { this.ok = ok; }
    }

    /**
     * A response from a single replica for a read operation.
     */
    public static class ReadResponse implements Serializable {
        public final DataItem item;

        public ReadResponse(DataItem item) {
            this.item = item;
        }
    }

}
