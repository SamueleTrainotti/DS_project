package com.example.ring;

import akka.actor.ActorRef;

public class Messages {
    public static class Start {
        public final String message;
        public Start(String message) { this.message = message; }
    }

    public static class Pass {
        public final String message;
        public final int hops;
        public Pass(String message, int hops) {
            this.message = message;
            this.hops = hops;
        }
    }

    public static class SetNext {
        public final ActorRef next;
        public SetNext(ActorRef next) { this.next = next; }
    }

    // === Key-Value operations ===
    public static class Put {
        public final String key;
        public final String value;
        public Put(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class Get {
        public final String key;
        public Get(String key) { this.key = key; }
    }

    public static class GetResponse {
        public final String key;
        public final String value;
        public GetResponse(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
