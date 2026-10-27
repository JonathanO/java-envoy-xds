package net.woaf.jono.envoy.example;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import net.woaf.jono.envoy.example.config.ClientConfig;
import net.woaf.jono.envoy.example.config.StaticClientConfigSource;
import net.woaf.jono.envoy.example.configbuilder.ClusterConfigBuilder;
import net.woaf.jono.envoy.example.configbuilder.ClusterLoadAssignmentConfigBuilder;
import net.woaf.jono.envoy.example.configbuilder.RouteConfigurationConfigBuilder;
import net.woaf.jono.envoy.example.state.Service;
import net.woaf.jono.envoy.example.state.SimpleUpdate;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.NamedMessage;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.Resources;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.Response;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The example's config builders, driven by its ClusterManager: each change goes in the right phase, adds and changes in
 * add order and removes in remove order, and an ADS stream sends them make-before-break.
 */
public class XdsExampleTest {

    private static final ClientConfig CLIENT = new ClientConfig(ClientConfig.DEFAULT_LISTEN_PORT);

    private static final Predicate<String> ALL = Predicates.alwaysTrue();

    private final ClusterManager clusterManager = new ClusterManager();

    private final ClusterConfigBuilder clusters = new ClusterConfigBuilder();

    private final ClusterLoadAssignmentConfigBuilder endpoints = new ClusterLoadAssignmentConfigBuilder();

    private final RouteConfigurationConfigBuilder routes = new RouteConfigurationConfigBuilder();

    @Test
    public void addingAServiceAddsItsClusterEndpointsAndRouteInAddOrder() {
        clusterManager.addEndpoint("a", ip("10.0.0.1"));

        SimpleUpdate update = change(() -> clusterManager.addEndpoint("b", ip("10.0.0.2")));

        assertThat(names(clusters.addOrder(update, ALL, CLIENT))).containsExactly("b");
        assertThat(endpoints.addOrder(update, ALL, CLIENT).addAndUpdates()).singleElement()
            .satisfies(cla -> assertThat(addresses(cla)).containsExactly("10.0.0.2:80"));
        assertThat(virtualHosts(routes.addOrder(update, ALL, CLIENT))).containsExactlyInAnyOrder("a", "b");

        assertThat(clusters.removeOrder(update, ALL, CLIENT).isNoop()).isTrue();
        assertThat(endpoints.removeOrder(update, ALL, CLIENT).isNoop()).isTrue();
        assertThat(routes.removeOrder(update, ALL, CLIENT).isNoop()).isTrue();
    }

    @Test
    public void replacingAnEndpointAddsTheNewOneBeforeRemovingTheOld() {
        clusterManager.addEndpoint("a", ip("10.0.0.1"));

        SimpleUpdate update = change(() -> clusterManager.setEndpoints("a", ip("10.0.0.2")));

        assertThat(endpoints.addOrder(update, ALL, CLIENT).addAndUpdates()).singleElement()
            .satisfies(cla -> assertThat(addresses(cla)).containsExactlyInAnyOrder("10.0.0.1:80", "10.0.0.2:80"));
        assertThat(endpoints.removeOrder(update, ALL, CLIENT).addAndUpdates()).singleElement()
            .satisfies(cla -> assertThat(addresses(cla)).containsExactly("10.0.0.2:80"));

        assertThat(clusters.addOrder(update, ALL, CLIENT).isNoop()).isTrue();
        assertThat(clusters.removeOrder(update, ALL, CLIENT).isNoop()).isTrue();
        assertThat(routes.addOrder(update, ALL, CLIENT).isNoop()).isTrue();
        assertThat(routes.removeOrder(update, ALL, CLIENT).isNoop()).isTrue();
    }

    @Test
    public void removingAServiceRemovesItsRouteClusterAndEndpointsInRemoveOrder() {
        clusterManager.addEndpoint("a", ip("10.0.0.1"));
        clusterManager.addEndpoint("b", ip("10.0.0.2"));

        SimpleUpdate update = change(() -> clusterManager.removeService("a"));

        assertThat(clusters.addOrder(update, ALL, CLIENT).isNoop()).isTrue();
        assertThat(endpoints.addOrder(update, ALL, CLIENT).isNoop()).isTrue();
        assertThat(routes.addOrder(update, ALL, CLIENT).isNoop()).isTrue();

        // The core sends the route before the cluster in remove order, so nothing is left pointing at a.
        assertThat(virtualHosts(routes.removeOrder(update, ALL, CLIENT))).containsExactly("b");
        assertThat(clusters.removeOrder(update, ALL, CLIENT).removes()).containsExactly("a");
        assertThat(endpoints.removeOrder(update, ALL, CLIENT)).isEqualTo(new Response<>(List.of(), List.of("a")));
    }

    @Test
    public void subscribingMidChangeGetsTheStateOfThatPhase() {
        clusterManager.addEndpoint("a", ip("10.0.0.1"));

        SimpleUpdate update = change(() -> {
            clusterManager.removeService("a");
            clusterManager.addEndpoint("b", ip("10.0.0.2"));
        });

        // Until remove order, a is still there; b is there from add order on.
        assertThat(names(clusters.getResourcesAddOrder(update, ALL, CLIENT))).containsExactlyInAnyOrder("a", "b");
        assertThat(names(clusters.getResourcesRemoveOrder(update, ALL, CLIENT))).containsExactly("b");
        assertThat(names(endpoints.getResourcesAddOrder(update, "a"::equals, CLIENT))).containsExactly("a");
        assertThat(names(endpoints.getResourcesRemoveOrder(update, "a"::equals, CLIENT))).isEmpty();
        assertThat(routes.getResourcesAddOrder(update, ALL, CLIENT).resources()).singleElement()
            .satisfies(route -> assertThat(virtualHosts(route)).containsExactlyInAnyOrder("a", "b"));
        assertThat(routes.getResourcesRemoveOrder(update, ALL, CLIENT).resources()).singleElement()
            .satisfies(route -> assertThat(virtualHosts(route)).containsExactly("b"));
        assertThat(routes.getResourcesAddOrder(update, "other"::equals, CLIENT).resources()).isEmpty();
    }

    @Test
    public void sotwStreamGetsChangesMakeBeforeBreak() throws InterruptedException {
        clusterManager.addEndpoint("a", ip("10.0.0.1"));
        clusterManager.addEndpoint("b", ip("10.0.0.2"));
        AggregatedDiscoveryService<SimpleUpdate, ClientConfig> ads = new AggregatedDiscoveryService<>(clusterManager,
            ImmutableList.of(clusters, endpoints, routes), new StaticClientConfigSource(), ImmutableList.of(),
            MergingStateBacklog.factory(), DiscoveryServiceManagerMetrics.METRICS_DISABLED);
        BlockingQueue<DiscoveryResponse> responses = new LinkedBlockingQueue<>();
        StreamObserver<DiscoveryRequest> requests = ads.streamAggregatedResources(new StreamObserver<>() {
            @Override
            public void onNext(DiscoveryResponse value) {
                responses.add(value);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        });
        Envoy envoy = new Envoy(requests, responses);

        assertThat(envoy.request(TypeUrl.CDS)).isEqualTo("CDS [a, b]");
        assertThat(envoy.request(TypeUrl.EDS, "a", "b")).isEqualTo("EDS [a, b]");
        assertThat(envoy.request(TypeUrl.RDS, "all_routes")).isEqualTo("RDS [a, b]");

        // Make: the cluster goes before the route that sends traffic to it.
        clusterManager.addEndpoint("c", ip("10.0.0.3"));
        assertThat(envoy.ackNext()).isEqualTo("CDS [a, b, c]");
        assertThat(envoy.ackNext()).isEqualTo("RDS [a, b, c]");

        // Break: the route stops sending traffic to the cluster before it goes.
        clusterManager.removeService("a");
        assertThat(envoy.ackNext()).isEqualTo("RDS [b, c]");
        assertThat(envoy.ackNext()).isEqualTo("CDS [b, c]");
        assertThat(envoy.ackNext()).isEqualTo("EDS [b]");

        requests.onCompleted();
    }

    /** Envoy's end of a SotW stream: it asks for names of a type, and ACKs what it's sent. */
    private static final class Envoy {

        private final StreamObserver<DiscoveryRequest> requests;

        private final BlockingQueue<DiscoveryResponse> responses;

        private final List<DiscoveryRequest> subscriptions = new ArrayList<>();

        Envoy(StreamObserver<DiscoveryRequest> requests, BlockingQueue<DiscoveryResponse> responses) {
            this.requests = requests;
            this.responses = responses;
        }

        String request(TypeUrl type, String... names) throws InterruptedException {
            DiscoveryRequest request = DiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy"))
                .setTypeUrl(type.getTypeUrl()).addAllResourceNames(List.of(names)).build();
            subscriptions.add(request);
            requests.onNext(request);
            return ackNext();
        }

        /** Wait for the next response, ACK it with the names subscribed to, and describe it. */
        String ackNext() throws InterruptedException {
            DiscoveryResponse response = responses.poll(10, TimeUnit.SECONDS);
            assertThat(response).as("a response").isNotNull();
            DiscoveryRequest subscription = subscriptions.stream()
                .filter(s -> s.getTypeUrl().equals(response.getTypeUrl())).findFirst().orElseThrow();
            requests.onNext(subscription.toBuilder().setVersionInfo(response.getVersionInfo()).setResponseNonce(response.getNonce()).build());
            return describe(response);
        }
    }

    /** Its type, and the clusters it names. */
    private static String describe(DiscoveryResponse response) {
        List<String> names = new ArrayList<>();
        for (Any any : response.getResourcesList()) {
            try {
                if (any.is(Cluster.class)) {
                    names.add(any.unpack(Cluster.class).getName());
                } else if (any.is(ClusterLoadAssignment.class)) {
                    names.add(any.unpack(ClusterLoadAssignment.class).getClusterName());
                } else {
                    names.addAll(virtualHosts(any.unpack(RouteConfiguration.class)));
                }
            } catch (InvalidProtocolBufferException e) {
                throw new AssertionError(e);
            }
        }
        return Objects.requireNonNull(TypeUrl.of(response.getTypeUrl())).name() + " " + names.stream().sorted().toList();
    }

    /** The update the cluster manager sends its subscribers for a change. */
    private SimpleUpdate change(Runnable change) {
        ImmutableMap<String, Service> before = clusterManager.getState();
        change.run();
        return new SimpleUpdate(before, clusterManager.getState());
    }

    private static HostAndPort ip(String host) {
        return HostAndPort.fromParts(host, 80);
    }

    private static List<String> names(Response<?> response) {
        return response.addAndUpdates().stream().map(NamedMessage::name).toList();
    }

    private static List<String> names(Resources<?> resources) {
        return resources.resources().stream().map(NamedMessage::name).toList();
    }

    private static List<String> addresses(NamedMessage<ClusterLoadAssignment> cla) {
        return cla.message().getEndpoints(0).getLbEndpointsList().stream()
            .map(e -> e.getEndpoint().getAddress().getSocketAddress())
            .map(a -> a.getAddress() + ":" + a.getPortValue()).toList();
    }

    private static List<String> virtualHosts(Response<RouteConfiguration> response) {
        assertThat(response.removes()).isEmpty();
        assertThat(response.addAndUpdates()).hasSize(1);
        return virtualHosts(response.addAndUpdates().get(0));
    }

    private static List<String> virtualHosts(NamedMessage<RouteConfiguration> route) {
        return virtualHosts(route.message());
    }

    private static List<String> virtualHosts(RouteConfiguration route) {
        return route.getVirtualHostsList().stream().map(VirtualHost::getName).toList();
    }
}
