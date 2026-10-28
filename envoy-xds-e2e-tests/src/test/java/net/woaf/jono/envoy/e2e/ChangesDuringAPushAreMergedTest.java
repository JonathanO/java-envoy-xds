package net.woaf.jono.envoy.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
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
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.woaf.jono.envoy.e2e.utils.DiscoveryServiceManagerEventListener;
import net.woaf.jono.envoy.testing.Checkpoint;
import net.woaf.jono.envoy.testing.EnvoyChecks;
import net.woaf.jono.envoy.testing.Settled;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.assertions.DeltaConversationAssert;
import net.woaf.jono.envoy.testing.utils.ConversationLogger;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import net.woaf.jono.envoy.testing.utils.EnvoyContainer;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;
import net.woaf.jono.envoy.xds.snapshot.Snapshot;
import net.woaf.jono.envoy.xds.snapshot.SnapshotConfigBuilder;
import net.woaf.jono.envoy.xds.snapshot.SnapshotSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Changes that arrive while a push waits for Envoy's ACKs are merged by {@link MergingStateBacklog}, so Envoy gets
 * fewer pushes than changes. The conformance suite checks where Envoy ends up; this checks the merging.
 */
class ChangesDuringAPushAreMergedTest {

    private static final int LISTENER_PORT = 10000;

    private final EnvoyContainer envoy = new EnvoyContainer("envoyproxy/envoy:v1.40-latest")
        .withExposedPorts(EnvoyContainer.ADMIN_PORT, LISTENER_PORT)
        .withAccessToHost(true);
    private final SnapshotSource<String, String, String> source = new SnapshotSource<>(node -> "envoy");
    private final List<DiscoveryServiceManagerEventListener> streams = Collections.synchronizedList(new ArrayList<>());
    private final ConversationLogger conversations = new ConversationLogger();
    private Server server;

    @AfterEach
    void stop() throws InterruptedException {
        envoy.stop();
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination();
        }
    }

    @Test
    void changesDuringAPushAreMerged() throws IOException {
        source.update(c -> c.view("envoy", "envoy").group("envoy", foo("127.0.0.1")));
        // Our server first, then Envoy, so Envoy doesn't back off past its initial fetch timeout.
        server = ServerBuilder.forPort(0).addService(new AggregatedDiscoveryService<>(
                source,
                SnapshotConfigBuilder.forTypes(Cluster.class, ClusterLoadAssignment.class, RouteConfiguration.class),
                node -> XdsConfig.<String>of(node.getId()),
                ImmutableList.of(),
                MergingStateBacklog.factory(),
                () -> {
                    DiscoveryServiceManagerEventListener listener = new DiscoveryServiceManagerEventListener();
                    streams.add(listener);
                    return listener;
                }))
            .intercept(conversations).build().start();
        org.testcontainers.Testcontainers.exposeHostPorts(server.getPort());
        envoy.withBootstrap("envoy-delta.yaml", server.getPort()).start();

        await().atMost(60, TimeUnit.SECONDS).until(() -> !streams.isEmpty() && !conversations.getConversations().isEmpty());
        DiscoveryServiceManagerEventListener stream = streams.get(0);
        Conversation conversation = conversations.getConversations().get(0);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
            () -> DeltaConversationAssert.assertThat(conversation).hadConversationsWith(XdsTypes.CDS, XdsTypes.RDS).answeredEveryType());
        await().atMost(30, TimeUnit.SECONDS).until(Settled.settled(conversation, stream));
        Checkpoint checkpoint = Checkpoint.take(envoy, conversation);
        final long pushed = stream.getPushed();

        // Back to back: they all arrive long before Envoy can ACK the first, so the rest wait in the backlog.
        for (int i = 2; i <= 21; i++) {
            Snapshot next = foo("127.0.0." + i);
            source.update(c -> c.group("envoy", next));
        }
        await().atMost(30, TimeUnit.SECONDS).until(Settled.settledAfter(conversation, checkpoint.message(), Settled.settled(conversation, stream)));
        checkpoint.assertSane();
        assertThat(envoy.admin().clusters().keySet()).as("foo's endpoints")
            .filteredOn(k -> k.startsWith("foo::") && k.endsWith("::rq_total")).containsExactly("foo::127.0.0.21:9901::rq_total");
        EnvoyChecks.assertRoutedVia(envoy, LISTENER_PORT, "foo", "foo");
        // A FIFO backlog would push all 20.
        assertThat(stream.getPushed() - pushed).as("pushes").isLessThan(20);
    }

    /** Cluster foo, with one endpoint at address on Envoy's admin port, and a virtual host foo routing to it. */
    private static Snapshot foo(String address) {
        ConfigSource ads = ConfigSource.newBuilder().setResourceApiVersion(ApiVersion.V3).setAds(AggregatedConfigSource.getDefaultInstance()).build();
        return Snapshot.empty().edit()
            .put(Cluster.newBuilder().setName("foo").setType(Cluster.DiscoveryType.EDS).setConnectTimeout(Duration.newBuilder().setSeconds(1))
                .setEdsClusterConfig(Cluster.EdsClusterConfig.newBuilder().setEdsConfig(ads)).build())
            // Envoy's own admin port stands in for the upstream: every 127/8 address in its container reaches it.
            .put(ClusterLoadAssignment.newBuilder().setClusterName("foo")
                .addEndpoints(LocalityLbEndpoints.newBuilder().addLbEndpoints(LbEndpoint.newBuilder().setEndpoint(Endpoint.newBuilder()
                    .setAddress(Address.newBuilder().setSocketAddress(SocketAddress.newBuilder().setAddress(address).setPortValue(EnvoyContainer.ADMIN_PORT))))))
                .build())
            .put(RouteConfiguration.newBuilder().setName("all_routes").addVirtualHosts(VirtualHost.newBuilder().setName("foo").addDomains("foo")
                .addRoutes(Route.newBuilder().setMatch(RouteMatch.newBuilder().setPrefix("/")).setRoute(RouteAction.newBuilder().setCluster("foo")))).build())
            .build();
    }
}
