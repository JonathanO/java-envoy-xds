package net.woaf.jono.envoy.testing;

import static org.assertj.core.api.Assertions.assertThat;

import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import java.time.Duration;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.junit.jupiter.api.Test;

public class SettledTest {

    @Test
    public void conversationAloneSettlesOnceEverythingIsAnsweredAndQuiet() throws Exception {
        Conversation conversation = new Conversation();
        conversation.onRequest(DiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.CDS).build());
        conversation.onResponse(DiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.CDS).setNonce("1").build());
        Thread.sleep(30);
        assertThat(Settled.settled(conversation, Duration.ofMillis(20)).call()).as("unanswered").isFalse();

        conversation.onRequest(DiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.CDS).setResponseNonce("1").build());
        assertThat(Settled.settled(conversation, Duration.ofMinutes(1)).call()).as("not quiet yet").isFalse();
        Thread.sleep(30);
        assertThat(Settled.settled(conversation, Duration.ofMillis(20)).call()).isTrue();
    }

    @Test
    public void hookMustBeIdleAndEverythingAnswered() throws Exception {
        Conversation conversation = new Conversation();
        conversation.onResponse(DeltaDiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.CDS).setNonce("1").build());
        assertThat(Settled.settled(conversation, () -> true).call()).as("unanswered").isFalse();

        conversation.onRequest(DeltaDiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.CDS).setResponseNonce("1").build());
        assertThat(Settled.settled(conversation, () -> false).call()).as("control plane busy").isFalse();
        assertThat(Settled.settled(conversation, () -> true).call()).as("no quiet period needed").isTrue();
    }

    @Test
    public void settledAfterWaitsForSomethingToBeSent() throws Exception {
        Conversation conversation = new Conversation();
        int mark = conversation.size();
        assertThat(Settled.settledAfter(conversation, mark, Settled.settled(conversation, () -> true)).call()).as("nothing sent yet").isFalse();

        conversation.onResponse(DeltaDiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.CDS).setNonce("1").build());
        conversation.onRequest(DeltaDiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.CDS).setResponseNonce("1").build());
        assertThat(Settled.settledAfter(conversation, mark, Settled.settled(conversation, () -> true)).call()).isTrue();
    }
}
