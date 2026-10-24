package net.woaf.jono.envoy.testing.conformance;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import org.jspecify.annotations.Nullable;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.protobuf.Any;
import io.envoyproxy.envoy.config.listener.v3.Filter;
import io.envoyproxy.envoy.config.listener.v3.FilterChain;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.extensions.filters.http.router.v3.Router;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpFilter;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.Rds;
import java.util.function.Supplier;
import org.hamcrest.Matchers;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
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
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.Objects;
import java.nio.charset.StandardCharsets;
import java.io.UncheckedIOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import java.util.Set;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.EnumSet;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import net.woaf.jono.envoy.testing.Checkpoint;
import net.woaf.jono.envoy.testing.EnvoyChecks;
import net.woaf.jono.envoy.testing.Settled;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.assertions.DeltaConversationAssert;
import net.woaf.jono.envoy.testing.assertions.EnvoyAssert;
import net.woaf.jono.envoy.testing.assertions.SotwConversationAssert;
import net.woaf.jono.envoy.testing.utils.ConversationLogger;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import net.woaf.jono.envoy.testing.utils.EnvoyContainer;
import net.woaf.jono.envoy.testing.matchers.XdsMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;

/**
 * Conformance tests for an xDS library: give it desired config, and check a real Envoy gets there safely.
 *
 * <p>This tests the layer that serves config to Envoy (ordering, ACKs and NACKs, subscriptions, reconnects), not how a
 * control plane decides what config to serve. Extend it once and implement {@link #newControlPlane()}. The suite runs
 * every scenario on each protocol in {@link #protocols()}, on each of {@link #ENVOY_IMAGES}, with and without rate
 * limiting Envoy's requests. Record known deviations with {@link #knownDeviations()}.
 *
 * <p>Envoy connects to port 6566 on the host, so don't run these in parallel.
 */
public abstract class XdsConformanceTest {

    /** Which ADS flavour Envoy uses. */
    public enum Protocol { SOTW, DELTA }

    /** The Envoy versions conformance means: the oldest Envoy release upstream still supports, and the newest. */
    public static final List<String> ENVOY_IMAGES = List.of("envoyproxy/envoy:v1.36-latest", "envoyproxy/envoy:v1.40-latest");

    /**
     * The library under test, set up afresh for each test.
     */
    public interface ControlPlane {

        /**
         * The control plane's ADS service. Called once per test; the same instance is served again after a reconnect.
         * @return gRPC service serving SotW and delta ADS, or just the protocols in {@link #protocols()}
         */
        BindableService service();

        /**
         * Make these resources the whole desired config for every Envoy, replacing what was there before.
         * @param resources Clusters, ClusterLoadAssignments and RouteConfigurations
         */
        void setConfig(Collection<? extends Message> resources);

        /**
         * Whether the control plane has nothing in flight or waiting to be sent on its latest stream. Without one, the
         * suite waits for every response to be answered and a quiet period.
         * @return hook, or null if the control plane can't tell
         */
        default Settled.@Nullable Hook idleHook() {
            return null;
        }

        /**
         * Make Envoy drop its stream and sync from scratch, e.g. to get a fixed config to an Envoy that NACKed.
         * @return false if the control plane can't, the default
         */
        default boolean resync() {
            return false;
        }
    }

    /**
     * A fresh control plane, for one test.
     * @return control plane
     */
    protected abstract ControlPlane newControlPlane();

    /**
     * The protocols the library supports; conformance results should say if it's not both.
     * @return protocols, by default both
     */
    protected Set<Protocol> protocols() {
        return EnumSet.allOf(Protocol.class);
    }

    /**
     * Known deviations, which are skipped with their reason. A key is a test's path, or part of it: any of protocol
     * ({@code sotw}, {@code delta}), Envoy version ({@code v1.36}), mode ({@code normal}, {@code rate-limited}) and
     * scenario ({@code changesWhileDisconnected}), joined by {@code /}. {@code "sotw/changesWhileDisconnected"} skips
     * that scenario on SotW everywhere.
     * @return path to reason
     */
    protected Map<String, String> knownDeviations() {
        return Map.of();
    }

    private static final Map<String, Scenario> SCENARIOS = scenarios();

    @FunctionalInterface
    private interface Scenario {
        void run(Harness harness) throws Exception;
    }

    private static Map<String, Scenario> scenarios() {
        Map<String, Scenario> scenarios = new LinkedHashMap<>();
        scenarios.put("startsEmpty", Harness::startsEmpty);
        scenarios.put("addsAndRemovesServices", Harness::addsAndRemovesServices);
        scenarios.put("movesRouteToNewCluster", Harness::movesRouteToNewCluster);
        scenarios.put("addsAndRemovesListener", Harness::addsAndRemovesListener);
        scenarios.put("churns", Harness::churns);
        scenarios.put("containsRejectedConfig", Harness::containsRejectedConfig);
        scenarios.put("reconnects", Harness::reconnects);
        scenarios.put("changesWhileDisconnected", Harness::changesWhileDisconnected);
        scenarios.put("controlPlaneRestarts", Harness::controlPlaneRestarts);
        return Collections.unmodifiableMap(scenarios);
    }

    /**
     * Every scenario, for each protocol, Envoy version, and with and without rate limiting. Each test is named by its
     * path, e.g. {@code sotw/v1.36/rate-limited/startsEmpty}, which is what {@link #knownDeviations()} matches.
     * @return the tests
     */
    @TestFactory
    public Stream<DynamicTest> conformance() {
        return protocols().stream().flatMap(protocol -> ENVOY_IMAGES.stream().flatMap(image -> Stream.of(false, true).flatMap(rateLimited ->
            SCENARIOS.entrySet().stream().map(scenario -> {
                List<String> path = List.of(protocol.name().toLowerCase(Locale.ROOT), version(image), rateLimited ? "rate-limited" : "normal", scenario.getKey());
                return dynamicTest(String.join("/", path), () -> run(path, protocol, image, rateLimited, scenario.getValue()));
            }))));
    }

    private void run(List<String> path, Protocol protocol, String image, boolean rateLimited, Scenario scenario) throws Exception {
        for (Map.Entry<String, String> deviation : knownDeviations().entrySet()) {
            if (path.containsAll(Splitter.on('/').splitToList(deviation.getKey()))) {
                Assumptions.abort(deviation.getValue());
            }
        }
        Harness harness = new Harness(this::newControlPlane, image, protocol, rateLimited);
        try {
            scenario.run(harness);
        } finally {
            harness.stop();
        }
    }

    private static String version(String image) {
        return image.substring(image.lastIndexOf(':') + 1).replace("-latest", "");
    }



    private static final int ADS_PORT = 6566;
    private static final int LISTENER_PORT = 10000;
    private static final int DYNAMIC_LISTENER_PORT = 10001; // the listener addsAndRemovesListener adds
    private static final String ROUTES = "all_routes"; // Envoy's static listener asks for this

    private static String bootstrap(Protocol protocol, boolean rateLimitedAds) {
        String resource = "net/woaf/jono/envoy/testing/conformance/envoy-" + protocol.name().toLowerCase(Locale.ROOT) + ".yaml";
        String yaml;
        try (InputStream in = Objects.requireNonNull(XdsConformanceTest.class.getClassLoader().getResourceAsStream(resource), resource)) {
            yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (!rateLimitedAds) {
            return yaml;
        }
        // One request at a time, ten a second: enough to queue Envoy's reconnect burst behind the answers to it.
        String limited = yaml.replace("    transport_api_version: V3\n    set_node_on_first_message_only",
            "    transport_api_version: V3\n    rate_limit_settings:\n      max_tokens: 1\n      fill_rate: 10\n    set_node_on_first_message_only");
        Preconditions.checkState(!limited.equals(yaml), "couldn't add rate_limit_settings to %s", resource);
        return limited;
    }

    /** One service per name: a cluster and endpoints of that name, and a virtual host of that name routing to it. */
    private static List<Message> services(String... names) {
        List<Message> config = new ArrayList<>();
        RouteConfiguration.Builder routes = RouteConfiguration.newBuilder().setName(ROUTES);
        for (String name : names) {
            config.add(cluster(name));
            config.add(endpoints(name));
            routes.addVirtualHosts(virtualHost(name, name));
        }
        config.add(routes.build());
        return config;
    }

    /** Adds cluster empty, with no endpoints, and a virtual host empty routing to it. */
    private static List<Message> withEmptyCluster(List<Message> config) {
        List<Message> withEmpty = new ArrayList<>();
        for (Message m : config) {
            withEmpty.add(m instanceof RouteConfiguration r && r.getName().equals(ROUTES) ? r.toBuilder().addVirtualHosts(virtualHost("empty", "empty")).build() : m);
        }
        withEmpty.add(cluster("empty"));
        withEmpty.add(ClusterLoadAssignment.newBuilder().setClusterName("empty").build());
        return withEmpty;
    }

    /**
     * Adds listener dynamic on port 10001, getting route config dynamic by RDS, whose virtual host dynamic routes to
     * cluster dynamic.
     */
    private static List<Message> withListener(List<Message> config) {
        HttpConnectionManager hcm = HttpConnectionManager.newBuilder()
            .setStatPrefix("dynamic")
            .setRds(Rds.newBuilder().setRouteConfigName("dynamic")
                .setConfigSource(ConfigSource.newBuilder().setResourceApiVersion(ApiVersion.V3).setAds(AggregatedConfigSource.getDefaultInstance())))
            .addHttpFilters(HttpFilter.newBuilder().setName("envoy.filters.http.router").setTypedConfig(Any.pack(Router.getDefaultInstance())))
            .build();
        Listener listener = Listener.newBuilder()
            .setName("dynamic")
            .setAddress(Address.newBuilder().setSocketAddress(SocketAddress.newBuilder().setAddress("0.0.0.0").setPortValue(DYNAMIC_LISTENER_PORT)))
            .addFilterChains(FilterChain.newBuilder().addFilters(Filter.newBuilder()
                .setName("envoy.filters.network.http_connection_manager").setTypedConfig(Any.pack(hcm))))
            .build();
        List<Message> withListener = new ArrayList<>(config);
        withListener.add(listener);
        withListener.add(RouteConfiguration.newBuilder().setName("dynamic").addVirtualHosts(virtualHost("dynamic", "dynamic")).build());
        withListener.add(cluster("dynamic"));
        withListener.add(endpoints("dynamic"));
        return withListener;
    }

    private static List<Message> withBadCluster(List<Message> config) {
        List<Message> bad = new ArrayList<>(config);
        bad.add(cluster("bad").toBuilder().setConnectTimeout(Duration.newBuilder().setSeconds(-1)).build());
        return bad;
    }

    /** A single virtual host routing to a cluster of a different name. */
    private static List<Message> service(String virtualHost, String cluster) {
        return List.of(cluster(cluster), endpoints(cluster),
            RouteConfiguration.newBuilder().setName(ROUTES).addVirtualHosts(virtualHost(virtualHost, cluster)).build());
    }

    private static Cluster cluster(String name) {
        return Cluster.newBuilder()
            .setName(name)
            .setType(Cluster.DiscoveryType.EDS)
            .setConnectTimeout(Duration.newBuilder().setSeconds(1))
            .setEdsClusterConfig(Cluster.EdsClusterConfig.newBuilder()
                .setEdsConfig(ConfigSource.newBuilder().setResourceApiVersion(ApiVersion.V3).setAds(AggregatedConfigSource.getDefaultInstance())))
            .build();
    }

    /** Envoy's own admin port stands in for the upstream: every 127/8 address in its container reaches it. */
    private static ClusterLoadAssignment endpoints(String cluster) {
        return ClusterLoadAssignment.newBuilder()
            .setClusterName(cluster)
            .addEndpoints(LocalityLbEndpoints.newBuilder().addLbEndpoints(LbEndpoint.newBuilder().setEndpoint(Endpoint.newBuilder()
                .setAddress(Address.newBuilder().setSocketAddress(SocketAddress.newBuilder().setAddress("127.0.0.1").setPortValue(EnvoyContainer.ADMIN_PORT))))))
            .build();
    }

    private static VirtualHost virtualHost(String name, String cluster) {
        return VirtualHost.newBuilder().setName(name).addDomains(name)
            .addRoutes(Route.newBuilder().setMatch(RouteMatch.newBuilder().setPrefix("/")).setRoute(RouteAction.newBuilder().setCluster(cluster)))
            .build();
    }

    /**
     * One test's Envoy, gRPC server and conversations, and the scenarios.
     */
    private static final class Harness {

        private final Supplier<ControlPlane> newControlPlane;
        private ControlPlane controlPlane;
        private final Protocol protocol;
        private final boolean rateLimitedAds;
        private final EnvoyContainer envoy;
        private final ConversationLogger conversations = new ConversationLogger();
        private @Nullable BindableService service;
        private @Nullable Server server;
        private @Nullable Checkpoint checkpoint;

        Harness(Supplier<ControlPlane> newControlPlane, String envoyImage, Protocol protocol, boolean rateLimitedAds) {
            this.newControlPlane = newControlPlane;
            this.controlPlane = newControlPlane.get();
            this.protocol = protocol;
            this.rateLimitedAds = rateLimitedAds;
            this.envoy = new EnvoyContainer(envoyImage)
                .withCopyToContainer(Transferable.of(bootstrap(protocol, rateLimitedAds)), "/envoy.yaml")
                .withCommand("-l", "debug", "-c", "/envoy.yaml")
                .withExposedPorts(EnvoyContainer.ADMIN_PORT, LISTENER_PORT, DYNAMIC_LISTENER_PORT)
                .withAccessToHost(true)
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(XdsConformanceTest.class)));
        }

        private Checkpoint checkpoint() {
            return Preconditions.checkNotNull(checkpoint, "start() first");
        }

        void stop() throws InterruptedException {
            envoy.stop();
            stopServer();
        }

        /** Starting with no config, Envoy gets an answer for every type, then takes the first service. */
        void startsEmpty() throws IOException {
            controlPlane.setConfig(List.of());
            start();
            Conversation conversation = synced(1);
            checkpoint().assertSane();

            change(conversation, services("foo"));
            checkpoint().assertSane();
            assertStatus("foo", 200);
        }

        /** A route, its cluster and its endpoints added in one change, then removed in one change. */
        void addsAndRemovesServices() throws IOException {
            controlPlane.setConfig(services("foo"));
            start();
            Conversation conversation = synced(1);

            // empty has no endpoints: it still needs an answer, an empty assignment, or Envoy leaves it warming
            change(conversation, withEmptyCluster(services("foo", "bar")));
            checkpoint().assertSane();
            assertStatus("bar", 200);
            assertStatus("empty", 503);

            change(conversation, services("foo"));
            // Envoy drops its EDS subscription for bar, which needs no response.
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> envoyAssert(conversation).unsubscribedFrom(XdsTypes.EDS, "bar"));
            await().atMost(10, TimeUnit.SECONDS).until(settled(conversation));
            checkpoint().assertSane();
            assertStatus("bar", 404);
            assertStatus("foo", 200);
        }

        /** A route moved to a new cluster, and its old cluster removed, in one change. */
        void movesRouteToNewCluster() throws IOException {
            controlPlane.setConfig(service("foo", "foo-v1"));
            start();
            Conversation conversation = synced(1);
            EnvoyChecks.assertRoutedVia(envoy, LISTENER_PORT, "foo", "foo-v1");

            change(conversation, service("foo", "foo-v2"));
            checkpoint().assertSane();
            EnvoyChecks.assertRoutedVia(envoy, LISTENER_PORT, "foo", "foo-v2");
        }

        /**
         * Envoy rejects a bad cluster, and the library contains it. It doesn't send the bad cluster again, not even as part
         * of a later change, and doesn't drop Envoy into a reconnect loop: retrying is spam, and Envoy resets its reconnect
         * backoff whenever it gets a response. Envoy keeps serving its last good config. Whether a later fix gets through on
         * the same stream is up to the library, but if it can {@link ControlPlane#resync()}, the fix must get through after one.
         */
        void containsRejectedConfig() throws IOException {
            controlPlane.setConfig(services("foo"));
            start();
            synced(1);

            controlPlane.setConfig(withBadCluster(services("foo")));
            await().atMost(10, TimeUnit.SECONDS).until(this::envoyRejectedSomething);
            Checkpoint afterNack = Checkpoint.take(envoy, conversations.getConversations().get(0));

            controlPlane.setConfig(withBadCluster(services("foo", "bar"))); // still bad
            assertContained(afterNack);
            controlPlane.setConfig(services("foo", "bar")); // fixed
            assertContained(afterNack);
            assertStatus("foo", 200);
            afterNack.assertEnvoyHealthy();

            if (controlPlane.resync()) {
                synced(2);
                checkpoint().assertSaneAfterReconnect();
                assertStatus("bar", 200);
            }
        }

        /**
         * A route moved to a new cluster, and the old cluster removed, while Envoy was away. Envoy keeps its config across
         * the disconnect, so once it reconnects the move must be make before break from there.
         */
        void changesWhileDisconnected() throws IOException, InterruptedException {
            controlPlane.setConfig(service("foo", "foo-v1"));
            start();
            final Conversation before = synced(1);
            EnvoyChecks.assertRoutedVia(envoy, LISTENER_PORT, "foo", "foo-v1");

            stopServer();
            controlPlane.setConfig(service("foo", "foo-v2"));
            start();
            synced(2);
            assertRateLimitedIfAsked();
            checkpoint().assertSaneAfterReconnect(before);
            EnvoyChecks.assertRoutedVia(envoy, LISTENER_PORT, "foo", "foo-v2");
            // and foo-v1 goes, once nothing uses it
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> MatcherAssert.assertThat(conversations.getConversations().get(1),
                XdsMatchers.hasInOrder(XdsMatchers.ackOf(XdsMatchers.response(XdsMatchers.ofType(XdsTypes.CDS), XdsMatchers.removing("foo-v1"))))));
        }

        /**
         * A listener, its route config, cluster and endpoints added in one change, then removed in one change. The
         * listener mustn't be left warming, and a route config mustn't go while a listener Envoy has still uses it.
         */
        void addsAndRemovesListener() throws IOException {
            controlPlane.setConfig(services("foo"));
            start();
            Conversation conversation = synced(1);

            change(conversation, withListener(services("foo")));
            checkpoint().assertSane();
            EnvoyChecks.assertRoutedVia(envoy, DYNAMIC_LISTENER_PORT, "dynamic", "dynamic");

            change(conversation, services("foo"));
            await().atMost(10, TimeUnit.SECONDS).until(settled(conversation));
            checkpoint().assertSane();
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(envoy.admin().stats())
                .as("listeners").containsEntry("listener_manager.total_listeners_active", 1L)); // just the static one
            assertStatus("foo", 200);
        }

        /** Twenty changes in quick succession, each moving foo to a new cluster: Envoy ends on the last, safely. */
        void churns() throws IOException, InterruptedException {
            controlPlane.setConfig(service("foo", "foo-v0"));
            start();
            Conversation conversation = synced(1);

            for (int i = 1; i <= 20; i++) {
                controlPlane.setConfig(service("foo", "foo-v" + i));
                Thread.sleep(25); // so some changes arrive while others are on their way
            }
            await().atMost(60, TimeUnit.SECONDS).until(Settled.settledAfter(conversation, checkpoint().message(), settled(conversation)));
            checkpoint().assertSane();
            EnvoyChecks.assertRoutedVia(envoy, LISTENER_PORT, "foo", "foo-v20");
            await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> MatcherAssert.assertThat("no stale clusters left",
                envoy.admin().configDump(), XdsMatchers.activeClusterNames(Matchers.contains("foo-v20"))));
        }

        /**
         * Envoy reconnects to a control plane that restarted, with different config. Envoy kept its config, which the new
         * control plane never sent, so it can only go by what Envoy tells it. Traffic keeps flowing, and bar goes once
         * nothing uses it.
         */
        void controlPlaneRestarts() throws IOException, InterruptedException {
            controlPlane.setConfig(services("foo", "bar"));
            start();
            final Conversation before = synced(1);
            assertStatus("bar", 200);

            stopServer();
            controlPlane = newControlPlane.get();
            service = null;
            controlPlane.setConfig(services("foo", "baz"));
            start();
            synced(2);
            assertRateLimitedIfAsked();
            checkpoint().assertSaneAfterReconnect(before);
            assertStatus("foo", 200);
            assertStatus("baz", 200);
            assertStatus("bar", 404);
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> MatcherAssert.assertThat(conversations.getConversations().get(1),
                XdsMatchers.hasInOrder(XdsMatchers.ackOf(XdsMatchers.response(XdsMatchers.ofType(XdsTypes.CDS), XdsMatchers.removing("bar"))))));
        }

        /** Envoy reconnects after a service was removed while it was away: traffic keeps flowing and updates still work. */
        void reconnects() throws IOException, InterruptedException {
            controlPlane.setConfig(services("foo", "bar"));
            start();
            synced(1);
            assertStatus("bar", 200);

            stopServer();
            controlPlane.setConfig(services("foo"));
            start();
            final Conversation conversation = synced(2); // Ignores Envoy's warnings about the disconnect.
            assertRateLimitedIfAsked();
            checkpoint().assertSaneAfterReconnect();
            assertStatus("foo", 200);
            assertStatus("bar", 404);

            change(conversation, services("foo", "baz"));
            checkpoint().assertSaneAfterReconnect();
            assertStatus("baz", 200);
        }

        /** Start the control plane, then Envoy if it isn't running, so Envoy doesn't back off past its initial fetch timeout. */
        private void start() throws IOException {
            if (service == null) {
                service = controlPlane.service();
            }
            server = ServerBuilder.forPort(ADS_PORT).addService(service).intercept(conversations).build();
            server.start();
            org.testcontainers.Testcontainers.exposeHostPorts(ADS_PORT);
            if (!envoy.isRunning()) {
                envoy.start();
            }
        }

        private void stopServer() throws InterruptedException {
            if (server != null) {
                server.shutdownNow();
                server.awaitTermination();
                server = null;
            }
        }

        /**
         * Wait for Envoy's nth stream to sync. Every type it asked for must be answered well inside its 15s initial fetch
         * timeout. Later checks only look at what happens from here on.
         */
        @CanIgnoreReturnValue
        private Conversation synced(int nth) {
            await().atMost(60, TimeUnit.SECONDS).until(() -> conversations.getConversations().size() >= nth);
            Conversation conversation = conversations.getConversations().get(nth - 1);
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> envoyAssert(conversation).hadConversationsWith(XdsTypes.CDS, XdsTypes.LDS, XdsTypes.RDS).answeredEveryType());
            await().atMost(30, TimeUnit.SECONDS).until(settled(conversation));
            checkpoint = Checkpoint.take(envoy, conversation);
            return conversation;
        }

        private void change(Conversation conversation, Collection<? extends Message> config) {
            int mark = conversation.size();
            controlPlane.setConfig(config);
            await().atMost(30, TimeUnit.SECONDS).until(Settled.settledAfter(conversation, mark, settled(conversation)));
        }

        private Callable<Boolean> settled(Conversation conversation) {
            Settled.Hook hook = controlPlane.idleHook();
            return hook == null ? Settled.settled(conversation) : Settled.settled(conversation, hook);
        }

        private void assertStatus(String virtualHost, int status) {
            // Envoy ACKs once its main thread has the config; workers get it a moment later.
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(envoy.httpStatus(LISTENER_PORT, virtualHost)).as(virtualHost).isEqualTo(status));
        }

        /** For a while: no more rejections, and no reconnects. */
        private void assertContained(Checkpoint afterNack) {
            await().during(2, TimeUnit.SECONDS).atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(conversations.getConversations()).as("streams; dropping Envoy after a NACK makes it reconnect in a loop").hasSize(1);
                assertThat(envoy.admin().stats().entrySet().stream()
                    .filter(e -> e.getKey().endsWith(".update_rejected") && !e.getValue().equals(afterNack.stats().getOrDefault(e.getKey(), 0L)))
                    .toList()).as("rejections since the first NACK; the bad cluster was sent again").isEmpty();
            });
        }

        /** A rate limited Envoy's reconnect burst should have hit the limit, or the variant isn't testing anything. */
        private void assertRateLimitedIfAsked() throws IOException {
            if (rateLimitedAds) {
                assertThat(envoy.admin().stats()).as("rate limited requests").hasEntrySatisfying("control_plane.rate_limit_enforced", n -> assertThat(n).isPositive());
            }
        }

        private boolean envoyRejectedSomething() {
            return conversations.getConversations().stream().flatMap(c -> c.getMessages().stream())
                .anyMatch(m -> (m instanceof DiscoveryRequest r && r.hasErrorDetail()) || (m instanceof DeltaDiscoveryRequest d && d.hasErrorDetail()));
        }

        private EnvoyAssert<?> envoyAssert(Conversation conversation) {
            return protocol == Protocol.DELTA ? DeltaConversationAssert.assertThat(conversation) : SotwConversationAssert.assertThat(conversation);
        }
    }
}
