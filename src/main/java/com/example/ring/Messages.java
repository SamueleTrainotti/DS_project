package com.example.ring;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

public class Messages {

    // Node information (nodeKey + ActorRef)
    public static class NodeInfo implements Serializable {
        public final long nodeKey; // unsigned value stored in long
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

    // Broadcast new membership to all nodes (manager -> nodes)
    public static class UpdateMembership implements Serializable {
        public final List<NodeInfo> nodes;
        public UpdateMembership(List<NodeInfo> nodes) { this.nodes = nodes; }
    }

    // -------------------
    // Client API messages
    // -------------------
    // Put request (issued by a client/node to any node)
    public static class DataPutRequest implements Serializable {
        public final long dataKey; // numeric key used for partitioning (unsigned)
        public final String key;   // logical key (string)
        public final String value; // value to store
        public final int replicationFactor;

        public DataPutRequest(long dataKey, String key, String value, int replicationFactor) {
            this.dataKey = dataKey;
            this.key = key;
            this.value = value;
            this.replicationFactor = replicationFactor;
        }
    }

    // Get request (client asks a node to find the key among responsible nodes)
    public static class DataGetRequest implements Serializable {
        public final long dataKey;
        public final String key;
        public final int replicationFactor;

        public DataGetRequest(long dataKey, String key, int replicationFactor) {
            this.dataKey = dataKey;
            this.key = key;
            this.replicationFactor = replicationFactor;
        }
    }

    // Response to DataGetRequest
    public static class DataGetResponse implements Serializable {
        public final String key;
        public final String value; // null when not found

        public DataGetResponse(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    // ---------------------------
    // Internal replication messages
    // ---------------------------
    // Instruct a node to store a replica of a specific dataKey/key/value
    public static class StoreReplica implements Serializable {
        public final long dataKey;
        public final String key;
        public final String value;

        public StoreReplica(long dataKey, String key, String value) {
            this.dataKey = dataKey;
            this.key = key;
            this.value = value;
        }
    }

    // Request to read a replica for a specific dataKey/key (used when searching responsible nodes)
    public static class GetReplica implements Serializable {
        public final long dataKey;
        public final String key;

        public GetReplica(long dataKey, String key) {
            this.dataKey = dataKey;
            this.key = key;
        }
    }

    // Reply for GetReplica
    public static class GetReplicaResponse implements Serializable {
        public final String key;
        public final String value; // null if not present

        public GetReplicaResponse(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    // Optional ack for a successful put
    public static class PutAck implements Serializable {
        public final boolean ok;
        public PutAck(boolean ok) { this.ok = ok; }
    }

    // Manager messages (from outside main)
    public static class AddNode implements Serializable {
        public final long nodeKey;
        public AddNode(long nodeKey) { this.nodeKey = nodeKey; }
    }

    public static class RemoveNode implements Serializable {
        public final long nodeKey;
        public RemoveNode(long nodeKey) { this.nodeKey = nodeKey; }
    }

    // Manager forwarding APIs (client uses manager to target a node)
    public static class ManagerPut implements Serializable {
        public final long originNodeKey;
        public final long dataKey;
        public final String key;
        public final String value;
        public ManagerPut(long originNodeKey, long dataKey, String key, String value) {
            this.originNodeKey = originNodeKey;
            this.dataKey = dataKey;
            this.key = key;
            this.value = value;
        }
    }

    public static class ManagerGet implements Serializable {
        public final long originNodeKey;
        public final long dataKey;
        public final String key;
        public ManagerGet(long originNodeKey, long dataKey, String key) {
            this.originNodeKey = originNodeKey;
            this.dataKey = dataKey;
            this.key = key;
        }
    }

    // Client-level API
    public static class ClientUpdate implements Serializable {
        public final long dataKey;
        public final String key;
        public final String value;
        public ClientUpdate(long dataKey, String key, String value) {
            this.dataKey = dataKey;
            this.key = key;
            this.value = value;
        }
    }

    public static class ClientGet implements Serializable {
        public final long dataKey;
        public final String key;
        public ClientGet(long dataKey, String key) {
            this.dataKey = dataKey;
            this.key = key;
        }
    }

}
