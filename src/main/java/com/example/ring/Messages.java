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
        public final DataItem item;
        public final int replicationFactor;

        public DataPutRequest(DataItem item, int replicationFactor) {
            this.item = item;
            this.replicationFactor = replicationFactor;
        }
    }

    // Get request (client asks a node to find the key among responsible nodes)
    public static class DataGetRequest implements Serializable {
        public final Long key;
        public final int replicationFactor;

        public DataGetRequest(Long key, int replicationFactor) {
            this.key = key;
            this.replicationFactor = replicationFactor;
        }
    }

    // Response to DataGetRequest
    public static class DataGetResponse implements Serializable {
        public final DataItem item;

        public DataGetResponse(DataItem item) {
            this.item = item;
        }
    }

    // ---------------------------
    // Internal replication messages
    // ---------------------------
    // Instruct a node to store a replica of a specific dataKey/key/value
    public static class StoreReplica implements Serializable {
        public final DataItem item;

        public StoreReplica(DataItem item) {
            this.item = item;
        }
    }

    // Request to read a replica for a specific dataKey/key (used when searching responsible nodes)
    public static class GetReplica implements Serializable {
        public final Long key;

        public GetReplica(Long key) {
            this.key = key;
        }
    }

    // Reply for GetReplica
    public static class GetReplicaResponse implements Serializable {
        public final DataItem item;

        public GetReplicaResponse(DataItem item) {
            this.item  = item;
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
        public final DataItem item;

        public ManagerPut(long originNodeKey, DataItem item) {
            this.originNodeKey = originNodeKey;
            this.item = item;
        }
    }

    public static class ManagerGet implements Serializable {
        public final long originNodeKey;
        public final Long key;

        public ManagerGet(long originNodeKey, Long key) {
            this.originNodeKey = originNodeKey;
            this.key = key;
        }
    }

    // Client-level API
    public static class ClientUpdate implements Serializable {
        public final DataItem item;

        public ClientUpdate(DataItem item) {
            this.item = item;
        }
    }

    public static class ClientGet implements Serializable {
        public final Long key;

        public ClientGet(Long key) {
            this.key = key;
        }
    }

    // conferma singola replica
    public static class WriteAck implements Serializable {
        public final boolean ok;
        public WriteAck(boolean ok) { this.ok = ok; }
    }

    // risposta di lettura da una replica
    public static class ReadResponse implements Serializable {
        public final DataItem item;

        public ReadResponse(DataItem item) {
            this.item = item;
        }
    }

}
