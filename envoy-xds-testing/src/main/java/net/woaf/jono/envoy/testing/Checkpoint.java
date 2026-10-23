package net.woaf.jono.envoy.testing;

import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import net.woaf.jono.envoy.testing.assertions.EnvoyAssert;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import net.woaf.jono.envoy.testing.utils.EnvoyContainer;

/**
 * Where a conversation and Envoy were at one moment: its message count, Envoy's log length and Envoy's stats. Take one
 * once Envoy has synced, make changes, then check that everything since was sane.
 *
 * <pre>{@code
 * Checkpoint checkpoint = Checkpoint.take(envoy, conversation);
 * controlPlane.addService("foo");
 * await().until(Settled.settledAfter(conversation, checkpoint.message(), Settled.settled(conversation)));
 * checkpoint.assertSane();
 * }</pre>
 */
public final class Checkpoint {

    private final EnvoyContainer envoy;
    private final Conversation conversation;
    private final int message;
    private final int logLine;
    private final Map<String, Long> stats;

    private Checkpoint(EnvoyContainer envoy, Conversation conversation, int message, int logLine, Map<String, Long> stats) {
        this.envoy = envoy;
        this.conversation = conversation;
        this.message = message;
        this.logLine = logLine;
        this.stats = stats;
    }

    /**
     * Mark where things are now.
     * @param envoy the running Envoy
     * @param conversation its current stream
     * @return checkpoint
     */
    public static Checkpoint take(EnvoyContainer envoy, Conversation conversation) {
        try {
            return new Checkpoint(envoy, conversation, conversation.size(), envoy.logSize(), envoy.admin().stats());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**

     * Conversation size when taken, e.g. for {@link Settled#settledAfter}.

     */
    public int message() {
        return message;
    }

    /**

     * Envoy's log length when taken.

     */
    public int logLine() {
        return logLine;
    }

    /**

     * Envoy's stats when taken.

     */
    public Map<String, Long> stats() {
        return stats;
    }

    /**
     * Everything that should hold once things have settled after changes since this checkpoint. Envoy ACKed every
     * response and was sent nothing it didn't ask for. Since the checkpoint: no empty responses, make before break, and
     * see {@link #assertEnvoyHealthy()}.
     */
    public void assertSane() {
        EnvoyAssert.of(conversation).nothingSentUnasked().allWereAcked().sentNoEmptyResponses(message).wasMakeBeforeBreak(message);
        assertEnvoyHealthy();
    }

    /**
     * As {@link #assertSane()}, for a conversation that started with Envoy reconnecting. We can't know what config Envoy
     * kept, so make before break isn't checked, and Envoy before 1.19 may resend resource names for wildcard types.
     */
    public void assertSaneAfterReconnect() {
        EnvoyAssert.of(conversation).nothingSentUnasked(envoyMinorVersion() < 19).allWereAcked().sentNoEmptyResponses(message);
        assertEnvoyHealthy();
    }

    /**
     * Envoy is connected, has nothing warming, and since this checkpoint rejected nothing, timed out no initial fetches
     * and logged no warnings or errors. See {@link EnvoyChecks}.
     */
    public void assertEnvoyHealthy() {
        EnvoyChecks.assertHealthy(envoy, stats);
        EnvoyChecks.assertNoWarningsSince(envoy, logLine);
    }

    /**

     * Envoy's minor version, from the node in its first request, or a recent one if it didn't say.

     */
    private int envoyMinorVersion() {
        for (Object m : conversation.getMessages()) {
            Node node = m instanceof DiscoveryRequest r ? r.getNode() : m instanceof DeltaDiscoveryRequest r ? r.getNode() : null;
            if (node != null && node.hasUserAgentBuildVersion()) {
                return node.getUserAgentBuildVersion().getVersion().getMinorNumber();
            }
        }
        return Integer.MAX_VALUE;
    }
}
