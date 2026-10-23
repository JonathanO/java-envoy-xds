package net.woaf.jono.envoy.testing.matchers;

import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.cluster;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.nack;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.response;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.routeConfig;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.ackOf;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.containing;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.hasInOrder;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.named;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.ofType;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.removing;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.routing;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.routingTo;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.withMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import com.google.protobuf.Any;
import com.google.rpc.Status;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.jupiter.api.Test;

public class XdsMatchersTest {

    private static final Cluster BAR = Cluster.newBuilder().setName("bar").setType(Cluster.DiscoveryType.EDS).build();

    private static RouteConfiguration routes(String virtualHost, String cluster) {
        return RouteConfiguration.newBuilder().setName("all_routes").addVirtualHosts(VirtualHost.newBuilder().setName(virtualHost)
            .addRoutes(Route.newBuilder().setRoute(RouteAction.newBuilder().setCluster(cluster)))).build();
    }

    private static DeltaDiscoveryResponse deltaResponse(String typeUrl, String nonce, RouteConfiguration route, String... removed) {
        DeltaDiscoveryResponse.Builder r = DeltaDiscoveryResponse.newBuilder().setTypeUrl(typeUrl).setNonce(nonce).addAllRemovedResources(java.util.List.of(removed));
        if (route != null) {
            r.addResources(Resource.newBuilder().setName(route.getName()).setResource(Any.pack(route)));
        }
        return r.build();
    }

    private static DeltaDiscoveryRequest ack(String typeUrl, String nonce) {
        return DeltaDiscoveryRequest.newBuilder().setTypeUrl(typeUrl).setResponseNonce(nonce).build();
    }

    private static final Matcher<Conversation> ROUTE_AWAY_ACKED_BEFORE_REMOVAL = hasInOrder(
        ackOf(response(ofType(XdsTypes.RDS), containing(routeConfig(not(routingTo("bar")))))),
        response(ofType(XdsTypes.CDS), removing("bar")));

    @Test
    public void matchesResources() {
        assertThat(cluster(named("bar"), ofType(Cluster.DiscoveryType.EDS)).matches(BAR)).isTrue();
        assertThat(cluster(named("foo")).matches(BAR)).isFalse();
        assertThat(routeConfig(routing("foo", "foo-v2")).matches(routes("foo", "foo-v2"))).isTrue();
        assertThat(routeConfig(routing("bar", "foo-v2")).matches(routes("foo", "foo-v2"))).as("other virtual host").isFalse();
        assertThat(cluster().matches(routes("foo", "foo"))).isFalse();
    }

    @Test
    public void ordersAnAckBeforeALaterResponse() {
        Conversation inOrder = new Conversation();
        inOrder.onResponse(deltaResponse(XdsTypes.RDS, "1", routes("foo", "foo")));
        inOrder.onRequest(ack(XdsTypes.RDS, "1"));
        inOrder.onResponse(deltaResponse(XdsTypes.CDS, "2", null, "bar"));
        assertThat(ROUTE_AWAY_ACKED_BEFORE_REMOVAL.matches(inOrder)).isTrue();

        Conversation removedFirst = new Conversation();
        removedFirst.onResponse(deltaResponse(XdsTypes.RDS, "1", routes("foo", "foo")));
        removedFirst.onResponse(deltaResponse(XdsTypes.CDS, "2", null, "bar"));
        removedFirst.onRequest(ack(XdsTypes.RDS, "1"));
        assertThat(ROUTE_AWAY_ACKED_BEFORE_REMOVAL.matches(removedFirst)).isFalse();
        StringDescription why = new StringDescription();
        ROUTE_AWAY_ACKED_BEFORE_REMOVAL.describeMismatch(removedFirst, why);
        assertThat(why.toString()).contains("step 2, a response").contains("removing \"bar\"").contains("not found after message 2");
    }

    @Test
    public void matchesNacks() {
        Object nack = DiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.CDS).setResponseNonce("1")
            .setErrorDetail(Status.newBuilder().setMessage("bad connect_timeout")).build();
        assertThat(nack(ofType(XdsTypes.CDS), withMessage(containsString("connect_timeout"))).matches(nack)).isTrue();
        assertThat(nack().matches(ack(XdsTypes.CDS, "1"))).isFalse();
    }

    @Test
    public void sotwRemovesByLeavingOutOfAWildcardType() {
        Object cdsWithoutBar = DiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.CDS).addResources(Any.pack(Cluster.newBuilder().setName("foo").build())).build();
        Object rdsWithoutBar = DiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.RDS).build();
        assertThat(response(removing("bar")).matches(cdsWithoutBar)).isTrue();
        assertThat(response(removing("foo")).matches(cdsWithoutBar)).isFalse();
        assertThat(response(removing("bar")).matches(rdsWithoutBar)).as("not a wildcard type").isFalse();
    }
}
