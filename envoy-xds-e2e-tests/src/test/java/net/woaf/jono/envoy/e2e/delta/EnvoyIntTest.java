package net.woaf.jono.envoy.e2e.delta;

import static net.woaf.jono.envoy.testing.assertions.DeltaConversationAssert.DELTA_CONVERSATION;

import net.woaf.jono.envoy.e2e.BaseEnvoyIntTest;
import net.woaf.jono.envoy.testing.assertions.DeltaConversationAssert;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.assertj.core.api.InstanceOfAssertFactory;


public abstract class EnvoyIntTest extends BaseEnvoyIntTest<DeltaConversationAssert> {

    @Override
    protected String getEnvoyConfigFileName() {
        return "envoy-delta-1.17.yaml";
    }

    @Override
    protected InstanceOfAssertFactory<Conversation, DeltaConversationAssert> assertFactory() {
        return DELTA_CONVERSATION;
    }

    @Override
    protected DeltaConversationAssert envoyAssertThat(Conversation conversation) {
        return DeltaConversationAssert.assertThat(conversation);
    }
}
