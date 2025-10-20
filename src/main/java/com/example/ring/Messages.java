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
     * A message sent to all nodes to inform them of a change in membership.
     */
    public static class UpdateMembership implements Serializable {
        /** A list of all nodes currently in the ring. */
        public final List<NodeInfo> nodes;

        public UpdateMembership(List<NodeInfo> nodes) { this.nodes = nodes; }
    }

    // -------------------
    // Client API messages
    // -------------------

    /**
     * A request sent to a coordinator node to store a data item in the ring.
     */
    public static class DataPutRequest implements Serializable {
        public final DataItem item;
        public final int replicationFactor;

        public DataPutRequest(DataItem item, int replicationFactor) {
            this.item = item;
            this.replicationFactor = replicationFactor;
        }
    }

    /**
     * A request sent to a coordinator node to retrieve a data item from the ring.
     */
    public static class DataGetRequest implements Serializable {
        public final Long key;
        public final int replicationFactor;

        public DataGetRequest(Long key, int replicationFactor) {
            this.key = key;
            this.replicationFactor = replicationFactor;
        }
    }

    /**
     * A generic response for any request that returns a single data item.
     * The value may be null if the item was not found.
     */
    public static class DataItemResponse implements Serializable {
        public final DataItem item;

        public DataItemResponse(DataItem item) {
            this.item = item;
        }
    }

    /**
     * A generic acknowledgement message for a PUT/write operation, indicating success or failure.
     */
    public static class PutAck implements Serializable {
        public final boolean ok;
        public PutAck(boolean ok) { this.ok = ok; }
    }

    // ---------------------------
    // Internal replication messages
    // ---------------------------

    /**
     * An instruction from a coordinator to a replica node to store a copy of a data item.
     */
    public static class StoreReplica implements Serializable {
        public final DataItem item;

        public StoreReplica(DataItem item) {
            this.item = item;
        }
    }

    /**
     * A request from a coordinator to a replica node to retrieve a copy of a data item.
     */
    public static class GetReplica implements Serializable {
        public final Long key;

        public GetReplica(Long key) {
            this.key = key;
        }
    }

    // ----------------
    // Control messages
    // ----------------

    /**
     * A message to add a new node to the ring.
     */
    public static class AddNode implements Serializable {
        public final long nodeKey;
        public AddNode(long nodeKey) { this.nodeKey = nodeKey; }
    }

    /**
     * A message to remove a node from the ring.
     */
    public static class RemoveNode implements Serializable {
        public final long nodeKey;
        public RemoveNode(long nodeKey) { this.nodeKey = nodeKey; }
    }
}
