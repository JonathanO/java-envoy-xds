package net.woaf.jono.envoy.testing.assertions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.junit.jupiter.api.Test;

public class SotwConversationAssertTest {

    @Test
    public void catchesANonceEchoedFromThePreviousStream() {
        Conversation conversation = new Conversation();
        conversation.onRequest(DiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.CDS).setResponseNonce("1").build()); // Envoy reconnecting
        conversation.onResponse(DiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.CDS).setNonce("1").build());
        conversation.onRequest(DiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.CDS).setResponseNonce("1").build());

        assertThatThrownBy(() -> SotwConversationAssert.assertThat(conversation).allWereAcked())
            .hasMessageContaining("which Envoy had echoed from its previous stream");
    }
}
