package net.woaf.jono.envoy.e2e.sotw;

import static net.woaf.jono.envoy.testing.assertions.SotwConversationAssert.SOTW_CONVERSATION;

import net.woaf.jono.envoy.e2e.BaseEnvoyIntTest;
import net.woaf.jono.envoy.testing.assertions.SotwConversationAssert;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.assertj.core.api.InstanceOfAssertFactory;

public abstract class EnvoyIntTest extends BaseEnvoyIntTest<SotwConversationAssert> {

    @Override
    protected String getEnvoyConfigFileName() {
        return "envoy-sotw-1.17.yaml";
    }

    @Override
    protected InstanceOfAssertFactory<Conversation, SotwConversationAssert> assertFactory() {
        return SOTW_CONVERSATION;
    }

    @Override
    protected SotwConversationAssert envoyAssertThat(Conversation conversation) {
        return SotwConversationAssert.assertThat(conversation);
    }
}