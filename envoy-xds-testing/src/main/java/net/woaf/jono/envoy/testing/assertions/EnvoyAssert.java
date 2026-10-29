package net.woaf.jono.envoy.testing.assertions;

import org.jspecify.annotations.Nullable;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.assertj.core.api.AbstractAssert;

/**
 * Checks on a recorded xDS conversation, either protocol.
 *
 * @param <SelfT> the concrete assert
 */
public abstract class EnvoyAssert<SelfT extends EnvoyAssert<SelfT>> extends AbstractAssert<SelfT, Conversation> {

    /**
     * For subclasses.
     * @param conversation the conversation
     * @param selfType the concrete assert's class
     */
    protected EnvoyAssert(Conversation conversation, Class<SelfT> selfType) {
        super(conversation, selfType);
    }

    /**
     * Checks for a conversation, SotW or delta, whichever it is.
     * @param conversation recorded stream
     * @return SotwConversationAssert or DeltaConversationAssert
     */
    public static EnvoyAssert<?> of(Conversation conversation) {
        boolean delta = conversation.getMessages().stream().anyMatch(m -> m instanceof DeltaDiscoveryRequest || m instanceof DeltaDiscoveryResponse);
        return delta ? DeltaConversationAssert.assertThat(conversation) : SotwConversationAssert.assertThat(conversation);
    }

    /**
     * Envoy ACKed every response we sent.
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT allWereAcked();

    /**
     * We never sent a resource Envoy hadn't subscribed to.
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT nothingSentUnasked();

    /**
     * Envoy subscribed to a resource, then unsubscribed from it.
     * @param typeUrl the type, e.g. {@link net.woaf.jono.envoy.testing.XdsTypes#RDS}
     * @param resourceName the resource
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT unsubscribedFrom(String typeUrl, String resourceName);

    /**
     * No response from fromMessage on was empty. Earlier ones may be: an empty first response for a wildcard type tells
     * Envoy there is nothing yet.
     * @param fromMessage index in the conversation to start checking from
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT sentNoEmptyResponses(int fromMessage);

    /**
     * From fromMessage on, no route was sent before Envoy ACKed the clusters and endpoints it uses, and no cluster was
     * removed while an ACKed route still used it. Start after the initial sync: answers to Envoy's first subscriptions
     * aren't ordered, Envoy's own startup sequencing covers those.
     * @param fromMessage index in the conversation to start checking from
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT wasMakeBeforeBreak(int fromMessage);

    /**
     * As {@link #wasMakeBeforeBreak(int)}, for a conversation Envoy started by reconnecting. Envoy keeps its config
     * across a disconnect, so it starts from what it last ACKed on the previous stream, and the answers to its first
     * requests on this one are checked too.
     * @param fromMessage index in this conversation to start checking from, usually 0
     * @param before the previous conversation with this Envoy, or null to start from nothing
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT wasMakeBeforeBreak(int fromMessage, @Nullable Conversation before);

    /**
     * Envoy subscribed to the route config of every listener it ACKed. Until it has, it hasn't synced, though it may
     * look done: a rate limited Envoy can ACK a listener well before it asks for the listener's routes.
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT askedForEveryRouteConfigInUse();

    /**
     * Every type Envoy asked for got at least one response. Without one Envoy waits out its initial fetch timeout, or
     * forever if that is disabled.
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT answeredEveryType();

    /**
     * Envoy sent requests for each of these types.
     * @param typeUrls e.g. {@link net.woaf.jono.envoy.testing.XdsTypes#CDS}
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT hadConversationsWith(String... typeUrls);

}
