package net.woaf.jono.envoy.e2e.assertions;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import net.woaf.jono.envoy.e2e.utils.ConversationLogger.Conversation;
import org.assertj.core.api.AbstractAssert;

public abstract class EnvoyAssert<SelfT extends EnvoyAssert<SelfT>> extends AbstractAssert<SelfT, Conversation> {

    protected EnvoyAssert(Conversation conversation, Class<SelfT> selfType) {
        super(conversation, selfType);
    }

    @CanIgnoreReturnValue
    public abstract SelfT allWereAcked();

    @CanIgnoreReturnValue
    public abstract SelfT nothingSentUnaskedPre119Reconnect();

    @CanIgnoreReturnValue
    public abstract SelfT nothingSentUnasked();

    @CanIgnoreReturnValue
    public abstract SelfT nothingSentUnasked(boolean allowBrokenReconnectWildcardBehaviourPre119);

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
     * Every type Envoy asked for got at least one response. Without one Envoy waits out its initial fetch timeout, or
     * forever if that is disabled.
     * @return this
     */
    @CanIgnoreReturnValue
    public abstract SelfT answeredEveryType();

    @CanIgnoreReturnValue
    public abstract SelfT hadConversationsWith(String... typeUrls);

}
