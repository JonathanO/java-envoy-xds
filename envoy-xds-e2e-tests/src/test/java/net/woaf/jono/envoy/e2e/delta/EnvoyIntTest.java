package net.woaf.jono.envoy.e2e.delta;

import static net.woaf.jono.envoy.testing.assertions.DeltaConversationAssert.DELTA_CONVERSATION;
import static org.assertj.core.api.Assertions.assertThat;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.ackOf;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.containing;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.hasInOrder;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.hasMessage;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.ofType;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.removing;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.response;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.routeConfig;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.routingTo;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.woaf.jono.envoy.e2e.BaseEnvoyIntTest;
import net.woaf.jono.envoy.e2e.utils.DiscoveryServiceManagerEventListener;
import net.woaf.jono.envoy.example.ClusterManager;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.assertions.DeltaConversationAssert;
import net.woaf.jono.envoy.testing.utils.ConversationLogger;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import net.woaf.jono.envoy.xds.TypeUrl;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
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
        Conversation reconnect = conversations.getConversations().get(1);
        Matcher<Object> barRemoved = response(ofType(XdsTypes.CDS), removing("bar"));
        MatcherAssert.assertThat(reconnect, hasInOrder(
            ackOf(response(ofType(XdsTypes.RDS), containing(routeConfig(not(routingTo("bar")))))),
            barRemoved));
        Matcher<Object> barEndpointsRemoved = response(ofType(XdsTypes.EDS), removing("bar"));
        if (hasMessage(barEndpointsRemoved).matches(reconnect)) { // Envoy may drop its EDS subscription itself once the cluster has gone
            MatcherAssert.assertThat(reconnect, hasInOrder(barRemoved, barEndpointsRemoved));
        }
        assertThat(httpStatus("bar")).isEqualTo(404);
        assertThat(httpStatus("foo")).isEqualTo(200);
    }
}
