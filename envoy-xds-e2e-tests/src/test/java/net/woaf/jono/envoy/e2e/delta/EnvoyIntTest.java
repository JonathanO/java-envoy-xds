package net.woaf.jono.envoy.e2e.delta;

import static net.woaf.jono.envoy.testing.assertions.DeltaConversationAssert.DELTA_CONVERSATION;
import static net.woaf.jono.envoy.testing.configdump.EnvoyAdminClient.sneakyUnpack;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import net.woaf.jono.envoy.e2e.BaseEnvoyIntTest;
import net.woaf.jono.envoy.e2e.utils.DiscoveryServiceManagerEventListener;
import net.woaf.jono.envoy.example.ClusterManager;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.assertions.DeltaConversationAssert;
import net.woaf.jono.envoy.testing.utils.ConversationLogger;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import net.woaf.jono.envoy.xds.TypeUrl;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;


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

    /**
     * A service removed while Envoy was away must be removed in ADS remove order once it reconnects: the route that used
     * it is replaced and ACKed before the cluster goes, and the cluster before its endpoints. Envoy's initial resource
     * versions tell us it still has bar.
     */
    @Test
    public void reconnectRemovalOrdering() throws IOException, InterruptedException {
        ClusterManager clusterManager = new ClusterManager();
        List<DiscoveryServiceManagerEventListener> streams = Collections.synchronizedList(new ArrayList<>());
        var ads = configureAds(clusterManager, streams, TypeUrl.RDS);
        ConversationLogger conversations = new ConversationLogger();
        startServer(ads, conversations);
        clusterManager.setEndpoints(ImmutableMap.of(
            "foo", ImmutableList.of(HostAndPort.fromString("127.0.0.1:9901")),
            "bar", ImmutableList.of(HostAndPort.fromString("127.0.0.2:9901"))
        ));
        // The listener resets connections until its routes arrive.
        await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> assertThat(httpStatus("bar")).isEqualTo(200));

        stopAds();
        clusterManager.removeService("bar");
        startServer(ads, conversations);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertThat(conversations.getConversations()).hasSize(2)
            .last(assertFactory()).hadConversationsWith(XdsTypes.CDS, XdsTypes.RDS, XdsTypes.EDS));
        await().atMost(30, TimeUnit.SECONDS).until(() -> streams.size() > 1 && streams.get(1).isIdle());
        List<Object> messages = conversations.getConversations().get(1).getMessages();

        int rdsWithoutBar = responseIndex(messages, XdsTypes.RDS, r -> !r.getResourcesList().isEmpty() && r.getResourcesList().stream()
            .map(res -> sneakyUnpack(res.getResource(), RouteConfiguration.class))
            .flatMap(rc -> rc.getVirtualHostsList().stream())
            .flatMap(vh -> vh.getRoutesList().stream())
            .noneMatch(route -> route.getRoute().getCluster().equals("bar")));
        int cdsRemoval = responseIndex(messages, XdsTypes.CDS, r -> r.getRemovedResourcesList().contains("bar"));
        assertThat(rdsWithoutBar).as("route away from bar sent").isNotNegative();
        assertThat(cdsRemoval).as("bar removed").isNotNegative();
        assertThat(ackIndex(messages, rdsWithoutBar)).as("route away from bar ACKed before bar is removed").isLessThan(cdsRemoval);
        int edsRemoval = responseIndex(messages, XdsTypes.EDS, r -> r.getRemovedResourcesList().contains("bar"));
        if (edsRemoval >= 0) { // Envoy may drop its EDS subscription itself once the cluster has gone
            assertThat(cdsRemoval).as("cluster removed before its endpoints").isLessThan(edsRemoval);
        }
        assertThat(httpStatus("bar")).isEqualTo(404);
        assertThat(httpStatus("foo")).isEqualTo(200);
    }

    /** Index of the first matching response, or -1. */
    private static int responseIndex(List<Object> messages, String typeUrl, Predicate<DeltaDiscoveryResponse> match) {
        for (int i = 0; i < messages.size(); i++) {
            if (messages.get(i) instanceof DeltaDiscoveryResponse r && r.getTypeUrl().equals(typeUrl) && match.test(r)) {
                return i;
            }
        }
        return -1;
    }

    private static int ackIndex(List<Object> messages, int responseIndex) {
        String nonce = ((DeltaDiscoveryResponse) messages.get(responseIndex)).getNonce();
        for (int i = responseIndex + 1; i < messages.size(); i++) {
            if (messages.get(i) instanceof DeltaDiscoveryRequest r && r.getResponseNonce().equals(nonce)) {
                return i;
            }
        }
        throw new AssertionError("No ACK of response " + nonce);
    }
}
