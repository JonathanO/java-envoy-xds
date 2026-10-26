package net.woaf.jono.envoy.testing;

import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;

/**
 * Conditions to wait on until a control plane and Envoy have stopped talking, e.g. with Awaitility:
 * {@code await().until(Settled.settled(conversation))}.
 *
 * <p>If the control plane under test can say when it is idle, pass that as a {@link Hook}: it knows whether more is
 * coming. Otherwise we can only see that every response was answered and the conversation has gone quiet.
 */
public final class Settled {

    /** How long a conversation must be silent to count as quiet, unless you say otherwise. */
    public static final Duration DEFAULT_QUIET_PERIOD = Duration.ofMillis(500);

    /**
     * Provided by the control plane under test, for one stream.
     */
    @FunctionalInterface
    public interface Hook {

        /**
         * Whether the control plane has nothing in flight or waiting to be sent on this stream.
         * @return true if idle
         */
        boolean isIdle();
    }

    private Settled() {
    }

    /**
     * Every response was answered and nothing has been sent either way for {@link #DEFAULT_QUIET_PERIOD}.
     * @param conversation the stream to watch
     * @return condition
     */
    public static Callable<Boolean> settled(Conversation conversation) {
        return settled(conversation, DEFAULT_QUIET_PERIOD);
    }

    /**
     * Every response was answered and nothing has been sent either way for quietPeriod.
     * @param conversation the stream to watch
     * @param quietPeriod how long to see nothing for
     * @return condition
     */
    public static Callable<Boolean> settled(Conversation conversation, Duration quietPeriod) {
        return () -> allAnswered(conversation) && conversation.quietFor(quietPeriod);
    }

    /**
     * The control plane says it is idle and every response was answered.
     * @param conversation the stream to watch
     * @param hook the control plane's idle signal for this stream
     * @return condition
     */
    public static Callable<Boolean> settled(Conversation conversation, Hook hook) {
        return () -> hook.isIdle() && allAnswered(conversation);
    }

    /**
     * Something was sent after fromMessage, then settled. Use this to wait for a change: when you start waiting the
     * control plane may not have begun sending it yet, and look idle.
     * @param conversation the stream to watch
     * @param fromMessage conversation size before the change
     * @param settled one of the settled() conditions
     * @return condition
     */
    public static Callable<Boolean> settledAfter(Conversation conversation, int fromMessage, Callable<Boolean> settled) {
        return () -> sentSince(conversation, fromMessage) && settled.call();
    }

    private static boolean sentSince(Conversation conversation, int fromMessage) {
        List<Object> messages = conversation.getMessages();
        return messages.subList(fromMessage, messages.size()).stream()
            .anyMatch(m -> m instanceof DiscoveryResponse || m instanceof DeltaDiscoveryResponse);
    }

    /** Every response nonce was ACKed or NACKed. */
    private static boolean allAnswered(Conversation conversation) {
        Set<String> unanswered = new HashSet<>();
        for (Object message : conversation.getMessages()) {
            if (message instanceof DiscoveryResponse r) {
                unanswered.add(r.getNonce());
            } else if (message instanceof DeltaDiscoveryResponse r) {
                unanswered.add(r.getNonce());
            } else if (message instanceof DiscoveryRequest r) {
                unanswered.remove(r.getResponseNonce());
            } else if (message instanceof DeltaDiscoveryRequest r) {
                unanswered.remove(r.getResponseNonce());
            }
        }
        return unanswered.isEmpty();
    }
}
