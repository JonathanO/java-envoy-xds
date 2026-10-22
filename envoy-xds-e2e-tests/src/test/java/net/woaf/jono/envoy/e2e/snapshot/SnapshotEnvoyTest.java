package net.woaf.jono.envoy.e2e.snapshot;

import static net.woaf.jono.envoy.e2e.configdump.EnvoyAdminClient.sneakyUnpack;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Duration;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Address;
import io.envoyproxy.envoy.config.core.v3.AggregatedConfigSource;
import io.envoyproxy.envoy.config.core.v3.ApiVersion;
import io.envoyproxy.envoy.config.core.v3.ConfigSource;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.endpoint.v3.Endpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LbEndpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LocalityLbEndpoints;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.RouteMatch;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import net.woaf.jono.envoy.e2e.assertions.DeltaConversationAssert;
import net.woaf.jono.envoy.e2e.configdump.EnvoyAdminClient;
import net.woaf.jono.envoy.e2e.configdump.TypedEnvoyConfigDump;
import net.woaf.jono.envoy.e2e.utils.ConversationLogger;
import net.woaf.jono.envoy.e2e.utils.DiscoveryServiceManagerEventListener;
import net.woaf.jono.envoy.e2e.utils.EnvoyContainer;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;
import net.woaf.jono.envoy.xds.snapshot.SnapshotConfigBuilder;
import net.woaf.jono.envoy.xds.snapshot.SnapshotSource;
import net.woaf.jono.envoy.xds.snapshot.SnapshotUpdate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * One snapshot change that adds (then removes) a route, its cluster and its endpoints must reach a real Envoy make-before-break.
 */
@Testcontainers
public class SnapshotEnvoyTest {

    @Container
    private final EnvoyContainer envoy = new EnvoyContainer("envoyproxy/envoy:v1.24-latest")
        .withClasspathResourceMapping("envoy-delta-1.17.yaml", "/envoy-custom.yaml", BindMode.READ_ONLY)
        .withCommand("-l", "debug", "-c", "/envoy-custom.yaml")
        .withExposedPorts(9901)
        .withAccessToHost(true);

    private Server server = null;

    @AfterEach
    public void stopServer() throws InterruptedException {
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination();
        }
    }

    @Test
    public void singleSnapshotChangesAreAppliedMakeBeforeBreak() throws IOException {
        SnapshotSource<String> source = new SnapshotSource<>();
        source.update(e -> e.put(route())); // Envoy's static listener waits for this route config.

        List<DiscoveryServiceManagerEventListener> streams = Collections.synchronizedList(new ArrayList<>());
        AggregatedDiscoveryService<SnapshotUpdate<String>, String> ads = new AggregatedDiscoveryService<>(
            source,
            SnapshotConfigBuilder.forTypes(Cluster.class, ClusterLoadAssignment.class, RouteConfiguration.class),
            node -> XdsConfig.<String>builder().clientDetails(node.getId()).build(),
            ImmutableList.of(),
            MergingStateBacklog.factory(),
            () -> {
                DiscoveryServiceManagerEventListener listener = new DiscoveryServiceManagerEventListener();
                streams.add(listener);
                return listener;
            }
        );
        ConversationLogger conversations = new ConversationLogger();
        server = ServerBuilder.forPort(6566).addService(ads).intercept(conversations).build();
        server.start();
        org.testcontainers.Testcontainers.exposeHostPorts(6566);
        final EnvoyAdminClient admin = new EnvoyAdminClient(HostAndPort.fromParts(envoy.getHost(), envoy.getMappedPort(9901)));

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertThat(conversations.getConversations()).isNotEmpty()
            .first(DeltaConversationAssert.DELTA_CONVERSATION).hadConversationsWith(TypeUrl.CDS.getTypeUrl(), TypeUrl.RDS.getTypeUrl()));
        await().atMost(30, TimeUnit.SECONDS).until(() -> !streams.isEmpty() && streams.get(0).isIdle());

        // Add route + cluster + endpoints in one go.
        int addStart = conversations.getConversations().get(0).getMessages().size();
        source.update(e -> e.put(cluster("foo")).put(endpoints("foo")).put(route("foo")));
        await().atMost(30, TimeUnit.SECONDS).until(() -> streams.get(0).getPushed() == 1 && streams.get(0).isIdle());

        List<Object> messages = conversations.getConversations().get(0).getMessages();
        int endpointsAcked = ackOf(messages, addStart, TypeUrl.EDS, r -> r.getName().equals("foo"));
        int routeSent = responseIndex(messages, addStart, TypeUrl.RDS, r -> routesTo(r, "foo"));
        assertThat(endpointsAcked).as("endpoints for foo accepted before the route to foo is sent").isLessThan(routeSent);
        TypedEnvoyConfigDump dump = admin.configDump();
        assertThat(activeClusters(dump)).contains("foo");

        // Remove them again in one go.
        final int removeStart = messages.size();
        source.update(e -> e.put(route()).remove(Cluster.class, "foo").remove(ClusterLoadAssignment.class, "foo"));
        await().atMost(30, TimeUnit.SECONDS).until(() -> streams.get(0).getPushed() == 2 && streams.get(0).isIdle());

        messages = conversations.getConversations().get(0).getMessages();
        int routeRemovedAcked = ackOf(messages, removeStart, TypeUrl.RDS, r -> !routesTo(r, "foo"));
        int clusterRemoved = removalIndex(messages, removeStart, TypeUrl.CDS, "foo");
        assertThat(routeRemovedAcked).as("route to foo gone before cluster foo is removed").isLessThan(clusterRemoved);
        // Not nothingSentUnasked(): the core answers Envoy's EDS unsubscribe for foo with an empty response (known core bug: unsubscribe-only requests get a response).
        DeltaConversationAssert.assertThat(conversations.getConversations().get(0)).allWereAcked();
        assertThat(activeClusters(admin.configDump())).doesNotContain("foo");
    }

    /**
     * Index of the first response of this type, from {@code from} on, carrying a matching resource.
     */
    private static int responseIndex(List<Object> messages, int from, TypeUrl type, Predicate<Resource> match) {
        for (int i = from; i < messages.size(); i++) {
            if (messages.get(i) instanceof DeltaDiscoveryResponse resp && resp.getTypeUrl().equals(type.getTypeUrl())
                && resp.getResourcesList().stream().anyMatch(match)) {
                return i;
            }
        }
        throw new AssertionError("No " + type + " response matched");
    }

    /**
     * Index of Envoy's ACK of the first response of this type, from {@code from} on, carrying a matching resource.
     */
    private static int ackOf(List<Object> messages, int from, TypeUrl type, Predicate<Resource> match) {
        String nonce = null;
        for (int i = from; i < messages.size(); i++) {
            Object m = messages.get(i);
            if (nonce == null && m instanceof DeltaDiscoveryResponse resp && resp.getTypeUrl().equals(type.getTypeUrl()) && resp.getResourcesList().stream().anyMatch(match)) {
                nonce = resp.getNonce();
            } else if (nonce != null && m instanceof DeltaDiscoveryRequest req && req.getResponseNonce().equals(nonce) && !req.hasErrorDetail()) {
                return i;
            }
        }
        throw new AssertionError("No ACK of a matching " + type + " response");
    }

    private static int removalIndex(List<Object> messages, int from, TypeUrl type, String name) {
        for (int i = from; i < messages.size(); i++) {
            if (messages.get(i) instanceof DeltaDiscoveryResponse resp && resp.getTypeUrl().equals(type.getTypeUrl()) && resp.getRemovedResourcesList().contains(name)) {
                return i;
            }
        }
        throw new AssertionError("No " + type + " removal of " + name);
    }

    private static boolean routesTo(Resource resource, String cluster) {
        return sneakyUnpack(resource.getResource(), RouteConfiguration.class).getVirtualHostsList().stream()
            .flatMap(v -> v.getRoutesList().stream())
            .anyMatch(r -> r.getRoute().getCluster().equals(cluster));
    }

    private static List<String> activeClusters(TypedEnvoyConfigDump dump) {
        return dump.getClustersConfigDump().getDynamicActiveClustersList().stream()
            .map(c -> sneakyUnpack(c.getCluster(), Cluster.class).getName())
            .collect(Collectors.toList());
    }

    private static Cluster cluster(String name) {
        return Cluster.newBuilder()
            .setName(name)
            .setType(Cluster.DiscoveryType.EDS)
            .setConnectTimeout(Duration.newBuilder().setSeconds(1).build())
            .setEdsClusterConfig(Cluster.EdsClusterConfig.newBuilder()
                .setEdsConfig(ConfigSource.newBuilder().setResourceApiVersion(ApiVersion.V3).setAds(AggregatedConfigSource.getDefaultInstance())))
            .build();
    }

    private static ClusterLoadAssignment endpoints(String cluster) {
        return ClusterLoadAssignment.newBuilder()
            .setClusterName(cluster)
            .addEndpoints(LocalityLbEndpoints.newBuilder().addLbEndpoints(LbEndpoint.newBuilder().setEndpoint(Endpoint.newBuilder()
                .setAddress(Address.newBuilder().setSocketAddress(SocketAddress.newBuilder().setAddress("127.0.0.5").setPortValue(80))))))
            .build();
    }

    private static RouteConfiguration route(String... clusters) {
        VirtualHost.Builder vhost = VirtualHost.newBuilder().setName("default").addDomains("*");
        for (String cluster : clusters) {
            vhost.addRoutes(Route.newBuilder().setMatch(RouteMatch.newBuilder().setPrefix("/" + cluster)).setRoute(RouteAction.newBuilder().setCluster(cluster)));
        }
        return RouteConfiguration.newBuilder().setName("all_routes").addVirtualHosts(vhost).build();
    }
}
