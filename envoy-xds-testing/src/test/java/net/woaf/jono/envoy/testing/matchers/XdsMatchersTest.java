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
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.activeClusterNames;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.endpointAssignment;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.hasActiveCluster;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.hasMessage;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.hasRouteConfig;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.hasStat;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.request;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.subscribingTo;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.unsubscribingFrom;
import static net.woaf.jono.envoy.testing.matchers.XdsMatchers.withEndpoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

import com.google.protobuf.Any;
import com.google.rpc.Status;
import io.envoyproxy.envoy.admin.v3.BootstrapConfigDump;
import io.envoyproxy.envoy.admin.v3.ClustersConfigDump;
import io.envoyproxy.envoy.admin.v3.EndpointsConfigDump;
import io.envoyproxy.envoy.admin.v3.ListenersConfigDump;
import io.envoyproxy.envoy.admin.v3.RoutesConfigDump;
import io.envoyproxy.envoy.admin.v3.ScopedRoutesConfigDump;
import io.envoyproxy.envoy.admin.v3.SecretsConfigDump;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Address;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.endpoint.v3.Endpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LbEndpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LocalityLbEndpoints;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.config.route.v3.WeightedCluster;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import java.util.Map;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.configdump.TypedEnvoyConfigDump;
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

    private static String mismatch(Matcher<?> matcher, Object item) {
        StringDescription why = new StringDescription();
        matcher.describeMismatch(item, why);
        return why.toString();
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
        RouteConfiguration weighted = RouteConfiguration.newBuilder().addVirtualHosts(VirtualHost.newBuilder().addRoutes(Route.newBuilder()
            .setRoute(RouteAction.newBuilder().setWeightedClusters(WeightedCluster.newBuilder().addClusters(WeightedCluster.ClusterWeight.newBuilder().setName("foo-v3")))))).build();
        assertThat(routeConfig(routingTo("foo-v3")).matches(weighted)).isTrue();
        assertThat(routeConfig(routingTo("foo")).matches(weighted)).isFalse();
    }

    @Test
    public void matchesEndpoints() {
        ClusterLoadAssignment foo = ClusterLoadAssignment.newBuilder().setClusterName("foo").addEndpoints(LocalityLbEndpoints.newBuilder().addLbEndpoints(LbEndpoint.newBuilder()
            .setEndpoint(Endpoint.newBuilder().setAddress(Address.newBuilder().setSocketAddress(SocketAddress.newBuilder().setAddress("10.0.0.1").setPortValue(8080)))))).build();
        assertThat(endpointAssignment(named("foo"), withEndpoint("10.0.0.1", 8080)).matches(foo)).isTrue();
        assertThat(endpointAssignment().matches(BAR)).isFalse();
        assertThat(mismatch(withEndpoint("10.0.0.1", 80), foo)).isEqualTo("endpoints were [\"10.0.0.1:8080\"]");
    }

    @Test
    public void matchesRequests() {
        Object delta = DeltaDiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.EDS).addResourceNamesSubscribe("foo").addResourceNamesUnsubscribe("bar").build();
        Object sotw = DiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.EDS).addResourceNames("foo").build();
        assertThat(request(ofType(XdsTypes.EDS), subscribingTo("foo"), unsubscribingFrom("bar")).matches(delta)).isTrue();
        assertThat(request(subscribingTo("foo")).matches(sotw)).isTrue();
        assertThat(request().matches(deltaResponse(XdsTypes.EDS, "1", null))).isFalse();
        assertThat(mismatch(request(subscribingTo("bar")), delta)).contains("names were [\"foo\"]");
        assertThat(mismatch(request(unsubscribingFrom("foo")), delta)).contains("unsubscribes were [\"bar\"]");
    }

    @Test
    public void matchesAMessageInAConversation() {
        Conversation conversation = new Conversation();
        conversation.onRequest(ack(XdsTypes.CDS, "1"));
        assertThat(hasMessage(request(ofType(XdsTypes.CDS))).matches(conversation)).isTrue();
        assertThat(hasMessage(nack()).matches(conversation)).isFalse();
    }

    @Test
    public void matchesWhatEnvoyHolds() {
        TypedEnvoyConfigDump dump = new TypedEnvoyConfigDump(BootstrapConfigDump.getDefaultInstance(),
            ClustersConfigDump.newBuilder()
                .addDynamicActiveClusters(ClustersConfigDump.DynamicCluster.newBuilder().setCluster(Any.pack(BAR)))
                .addDynamicWarmingClusters(ClustersConfigDump.DynamicCluster.newBuilder().setCluster(Any.pack(Cluster.newBuilder().setName("warming").build())))
                .build(),
            EndpointsConfigDump.getDefaultInstance(), ListenersConfigDump.getDefaultInstance(), ScopedRoutesConfigDump.getDefaultInstance(),
            RoutesConfigDump.newBuilder().addDynamicRouteConfigs(RoutesConfigDump.DynamicRouteConfig.newBuilder().setRouteConfig(Any.pack(routes("foo", "foo")))).build(),
            SecretsConfigDump.getDefaultInstance());
        assertThat(hasActiveCluster(named("bar")).matches(dump)).isTrue();
        assertThat(hasActiveCluster(named("warming")).matches(dump)).isFalse();
        assertThat(activeClusterNames(contains("bar")).matches(dump)).isTrue();
        assertThat(activeClusterNames(empty()).matches(dump)).isFalse();
        assertThat(hasRouteConfig(routingTo("foo")).matches(dump)).isTrue();
        assertThat(hasRouteConfig(routingTo("bar")).matches(dump)).isFalse();
    }

    @Test
    public void matchesStats() {
        Map<String, Long> stats = Map.of("cluster.foo.upstream_rq_total", 3L);
        assertThat(hasStat("cluster.foo.upstream_rq_total", greaterThan(0L)).matches(stats)).isTrue();
        assertThat(hasStat("cluster.bar.upstream_rq_total", greaterThan(0L)).matches(stats)).isFalse();
        assertThat(hasStat("cluster.bar.upstream_rq_total", not(greaterThan(0L))).matches(stats)).as("a stat Envoy doesn't have").isFalse();
        assertThat(mismatch(hasStat("cluster.bar.upstream_rq_total", not(greaterThan(0L))), stats)).isEqualTo("Envoy has no stat cluster.bar.upstream_rq_total");
        assertThat(mismatch(hasStat("cluster.foo.upstream_rq_total", greaterThan(5L)), stats)).isEqualTo("cluster.foo.upstream_rq_total <3L> was less than <5L>");
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
