package com.example.ring;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.Duration;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for simulating network latency in communications between Actors
 */
public class NetworkSimulator {

    private static final boolean ENABLED = true;
    private static final int MIN_DELAY_MS = 10;
    private static final int MAX_DELAY_MS = 200;
    private static final Random random = new Random();

    /**
     * Sends a message with random delay to a target actor.
     */
    public static void sendWithDelay(ActorSystem system, ActorRef target, Object message, ActorRef sender) {
        if (ENABLED) {
            int delay = MIN_DELAY_MS + random.nextInt(MAX_DELAY_MS - MIN_DELAY_MS + 1);

            system.scheduler().scheduleOnce(
                    Duration.create(delay, TimeUnit.MILLISECONDS),
                    () -> sendWithDelay(system, target, message, sender),
                    system.dispatcher()
            );
        } else {
            sendWithDelay(system, target, message, sender);
        }
    }

    /**
     * Sends a message with random delay to a target actor (context aware).
     */
    public static void sendWithDelay(akka.actor.ActorContext context, ActorRef target, Object message, ActorRef sender) {
        sendWithDelay(context.system(), target, message, sender);
    }
}