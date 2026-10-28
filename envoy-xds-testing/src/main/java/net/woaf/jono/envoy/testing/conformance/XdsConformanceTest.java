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
import io.envoyproxy.envoy.config.route.v3.ScopedRouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.Vhds;
import io.envoyproxy.envoy.extensions.filters.http.on_demand.v3.OnDemand;
import io.envoyproxy.envoy.extensions.filters.http.on_demand.v3.OnDemandCds;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.ScopedRds;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.ScopedRoutes;
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
import org.testcontainers.images.builder.Transferable;

/**
 * Conformance tests for an xDS library: give it desired config, and check a real Envoy gets there safely.
 *
 * <p>This tests the layer that serves config to Envoy (ordering, ACKs and NACKs, subscriptions, reconnects), not how a
 * control plane decides what config to serve. Extend it once and implement {@link #newControlPlane()}. The suite runs
 * every scenario on each protocol in {@link #protocols()}, on each of {@link #envoyImages()}, with and without rate
 * limiting Envoy's requests. Record known deviations with {@link #knownDeviations()}.
 *
 * <p>Each test's control plane listens on a free port, so tests can run in parallel.
 */
public abstract class XdsConformanceTest {

    /** Which ADS flavour Envoy uses. */
    public enum Protocol {
        /** State of the world. */
        SOTW,
        /** Incremental. */
        DELTA
    }

    /** For subclasses, one per library under test. */
    protected XdsConformanceTest() {
    }

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
         * @param resources Clusters, ClusterLoadAssignments, Listeners, RouteConfigurations, ScopedRouteConfigurations
         *     and VirtualHosts (for VHDS, named {@code <route config>/<host>})
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
     * The Envoy images to run on; conformance results should say if it's not {@link #ENVOY_IMAGES}.
     * @return images, tagged {@code v1.NN...}, by default {@link #ENVOY_IMAGES}
     */
    protected List<String> envoyImages() {
        return ENVOY_IMAGES;
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
    private interface Steps {
        void run(Harness harness) throws Exception;
    }

    /**
     * A scenario, the protocols Envoy supports it on, whether Envoy subscribes to every cluster (else it only asks for
     * clusters on demand), and the oldest Envoy minor version that supports it, with why, if not all do.
     */
    private record Scenario(Steps steps, Set<Protocol> protocols, boolean allClusters, int minEnvoyMinor, String why) {
        static Scenario everywhere(Steps steps) {
            return new Scenario(steps, EnumSet.allOf(Protocol.class), true, 0, "");
        }
    }

    private static Map<String, Scenario> scenarios() {
        Map<String, Scenario> scenarios = new LinkedHashMap<>();
        scenarios.put("startsEmpty", Scenario.everywhere(Harness::startsEmpty));
        scenarios.put("addsAndRemovesServices", Scenario.everywhere(Harness::addsAndRemovesServices));
        scenarios.put("changesEndpoints", Scenario.everywhere(Harness::changesEndpoints));
        scenarios.put("movesRouteToNewCluster", Scenario.everywhere(Harness::movesRouteToNewCluster));
        scenarios.put("addsAndRemovesListener", Scenario.everywhere(Harness::addsAndRemovesListener));
        scenarios.put("churns", Scenario.everywhere(Harness::churns));
        scenarios.put("containsRejectedConfig", Scenario.everywhere(Harness::containsRejectedConfig));
        scenarios.put("reconnects", Scenario.everywhere(Harness::reconnects));
        scenarios.put("changesWhileDisconnected", Scenario.everywhere(Harness::changesWhileDisconnected));
        scenarios.put("controlPlaneRestarts", Scenario.everywhere(Harness::controlPlaneRestarts));
        scenarios.put("scopedRoutes", Scenario.everywhere(Harness::scopedRoutes));
        scenarios.put("onDemandScopes", Scenario.everywhere(Harness::onDemandScopes));
        // Envoy only does on-demand clusters and virtual hosts over delta.
        scenarios.put("onDemandClusters", new Scenario(Harness::onDemandClusters, EnumSet.of(Protocol.DELTA), false, 0, ""));
        scenarios.put("onDemandVirtualHosts", new Scenario(Harness::onDemandVirtualHosts, EnumSet.of(Protocol.DELTA), true, 40,
            "Envoy 1.36 only takes VHDS from a DELTA_GRPC config source of its own, not over ADS"));
        return Collections.unmodifiableMap(scenarios);
    }

    /**
     * Every scenario, for each protocol, Envoy version, and with and without rate limiting. Each test is named by its
     * path, e.g. {@code sotw/v1.36/rate-limited/startsEmpty}, which is what {@link #knownDeviations()} matches.
     * @return the tests
     */
    @TestFactory
    public Stream<DynamicTest> conformance() {
        return protocols().stream().flatMap(protocol -> envoyImages().stream().flatMap(image -> Stream.of(false, true).flatMap(rateLimited ->
            SCENARIOS.entrySet().stream().filter(scenario -> scenario.getValue().protocols().contains(protocol)).map(scenario -> {
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
        Assumptions.assumeTrue(Integer.parseInt(version(image).substring(version(image).indexOf('.') + 1)) >= scenario.minEnvoyMinor(), scenario.why());
        Harness harness = new Harness(this::newControlPlane, image, protocol, rateLimited, scenario.allClusters());
        try {
            scenario.steps().run(harness);
        } catch (Throwable t) {
            // To stderr, which the test report keeps: SLF4J may have nowhere to go in a test.
            System.err.println("Envoy's log for " + String.join("/", path) + ":\n" + String.join("\n", harness.envoy.logSince(0)));
            throw t;
        } finally {
            harness.stop();
        }
    }

    private static String version(String image) {
        return image.substring(image.lastIndexOf(':') + 1).replace("-latest", "");
    }



    private static final int LISTENER_PORT = 10000;
    private static final int DYNAMIC_LISTENER_PORT = 10001; // the listener addsAndRemovesListener adds
    private static final String ROUTES = "all_routes"; // Envoy's static listener asks for this

    private static String bootstrap(Protocol protocol, boolean rateLimitedAds, boolean allClusters, int adsPort) {
        String resource = "net/woaf/jono/envoy/testing/conformance/envoy-" + protocol.name().toLowerCase(Locale.ROOT) + ".yaml";
        String yaml;
        try (InputStream in = Objects.requireNonNull(XdsConformanceTest.class.getClassLoader().getResourceAsStream(resource), resource)) {
            yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Preconditions.checkState(yaml.contains(EnvoyContainer.ADS_PORT_PLACEHOLDER), "%s has no ADS port placeholder", resource);
        yaml = yaml.replace(EnvoyContainer.ADS_PORT_PLACEHOLDER, Integer.toString(adsPort));
        if (!allClusters) {
            String noCds = yaml.replace("  cds_config:\n    ads: { }\n    resource_api_version: V3\n", "");
            Preconditions.checkState(!noCds.equals(yaml), "couldn't remove cds_config from %s", resource);
            yaml = noCds;
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

    private static ConfigSource ads() {
        return ConfigSource.newBuilder().setResourceApiVersion(ApiVersion.V3).setAds(AggregatedConfigSource.getDefaultInstance()).build();
    }

    private static HttpFilter router() {
        return HttpFilter.newBuilder().setName("envoy.filters.http.router").setTypedConfig(Any.pack(Router.getDefaultInstance())).build();
    }

    /** Holds a request while Envoy fetches what it needs on demand: a scope's route config, a virtual host, or a cluster. */
    private static HttpFilter onDemand(OnDemand config) {
        return HttpFilter.newBuilder().setName("envoy.filters.http.on_demand").setTypedConfig(Any.pack(config)).build();
    }

    /** Listener name on port 10001, with this HTTP connection manager. */
    private static Listener listener(String name, HttpConnectionManager hcm) {
        return Listener.newBuilder()
            .setName(name)
            .setAddress(Address.newBuilder().setSocketAddress(SocketAddress.newBuilder().setAddress("0.0.0.0").setPortValue(DYNAMIC_LISTENER_PORT)))
            .addFilterChains(FilterChain.newBuilder().addFilters(Filter.newBuilder()
                .setName("envoy.filters.network.http_connection_manager").setTypedConfig(Any.pack(hcm))))
            .build();
    }

    /**
     * Adds listener scoped on port 10001, which picks a scope by the Host header, and for each name a scope of that
     * name whose route config scoped-name routes to the cluster of that name. On demand, Envoy only fetches a scope's
     * route config once a request needs it.
     */
    private static List<Message> withScopes(List<Message> config, boolean onDemand, String... names) {
        HttpConnectionManager.Builder hcm = HttpConnectionManager.newBuilder()
            .setStatPrefix("scoped")
            .setScopedRoutes(ScopedRoutes.newBuilder()
                .setName("scopes")
                .setScopeKeyBuilder(ScopedRoutes.ScopeKeyBuilder.newBuilder().addFragments(ScopedRoutes.ScopeKeyBuilder.FragmentBuilder.newBuilder()
                    .setHeaderValueExtractor(ScopedRoutes.ScopeKeyBuilder.FragmentBuilder.HeaderValueExtractor.newBuilder()
                        .setName(":authority").setElementSeparator(":").setIndex(0))))
                .setRdsConfigSource(ads())
                .setScopedRds(ScopedRds.newBuilder().setScopedRdsConfigSource(ads())));
        if (onDemand) {
            hcm.addHttpFilters(onDemand(OnDemand.getDefaultInstance()));
        }
        hcm.addHttpFilters(router());
        List<Message> scoped = new ArrayList<>(config);
        scoped.add(listener("scoped", hcm.build()));
        for (String name : names) {
            scoped.add(ScopedRouteConfiguration.newBuilder().setName(name).setRouteConfigurationName("scoped-" + name).setOnDemand(onDemand)
                .setKey(ScopedRouteConfiguration.Key.newBuilder().addFragments(ScopedRouteConfiguration.Key.Fragment.newBuilder().setStringKey(name)))
                .build());
            scoped.add(RouteConfiguration.newBuilder().setName("scoped-" + name).addVirtualHosts(virtualHost(name, name)).build());
        }
        return scoped;
    }

    /**
     * Listener on-demand on port 10001, whose route config routes each name to the cluster of that name, plus those
     * clusters and their endpoints, for an Envoy that only gets clusters when a request needs them. The static
     * listener's route config is empty.
     */
    private static List<Message> clustersOnDemand(String... names) {
        HttpConnectionManager hcm = HttpConnectionManager.newBuilder()
            .setStatPrefix("on-demand")
            .setRds(Rds.newBuilder().setRouteConfigName("on-demand").setConfigSource(ads()))
            .addHttpFilters(onDemand(OnDemand.newBuilder().setOdcds(OnDemandCds.newBuilder().setSource(ads())
                .setTimeout(Duration.newBuilder().setSeconds(5))).build()))
            .addHttpFilters(router())
            .build();
        List<Message> config = new ArrayList<>();
        config.add(RouteConfiguration.newBuilder().setName(ROUTES).build());
        config.add(listener("on-demand", hcm));
        RouteConfiguration.Builder routes = RouteConfiguration.newBuilder().setName("on-demand");
        for (String name : names) {
            config.add(cluster(name));
            config.add(endpoints(name));
            routes.addVirtualHosts(virtualHost(name, name));
        }
        config.add(routes.build());
        return config;
    }

    /**
     * Adds listener vhds on port 10001, whose route config vhds has no virtual hosts of its own, and for each name a
     * VHDS virtual host vhds/name routing to the cluster of that name.
     */
    private static List<Message> withVirtualHostsOnDemand(List<Message> config, String... names) {
        HttpConnectionManager hcm = HttpConnectionManager.newBuilder()
            .setStatPrefix("vhds")
            .setRds(Rds.newBuilder().setRouteConfigName("vhds").setConfigSource(ads()))
            .addHttpFilters(onDemand(OnDemand.getDefaultInstance()))
            .addHttpFilters(router())
            .build();
        List<Message> withVhds = new ArrayList<>(config);
        withVhds.add(listener("vhds", hcm));
        withVhds.add(RouteConfiguration.newBuilder().setName("vhds").setVhds(Vhds.newBuilder().setConfigSource(ads())).build());
        for (String name : names) {
            withVhds.add(virtualHost(name, name).toBuilder().setName("vhds/" + name).build());
        }
        return withVhds;
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

    private static ClusterLoadAssignment endpoints(String cluster) {
        return endpoints(cluster, "127.0.0.1");
    }

    /** Envoy's own admin port stands in for the upstream: every 127/8 address in its container reaches it. */
    private static ClusterLoadAssignment endpoints(String cluster, String... addresses) {
        LocalityLbEndpoints.Builder locality = LocalityLbEndpoints.newBuilder();
        for (String address : addresses) {
            locality.addLbEndpoints(LbEndpoint.newBuilder().setEndpoint(Endpoint.newBuilder()
                .setAddress(Address.newBuilder().setSocketAddress(SocketAddress.newBuilder().setAddress(address).setPortValue(EnvoyContainer.ADMIN_PORT)))));
        }
        return ClusterLoadAssignment.newBuilder().setClusterName(cluster).addEndpoints(locality).build();
    }

    /** Moves cluster's endpoints to these 127/8 addresses, changing nothing else. */
    private static List<Message> withEndpoints(List<Message> config, String cluster, String... addresses) {
        return config.stream()
            .map(m -> m instanceof ClusterLoadAssignment c && c.getClusterName().equals(cluster) ? endpoints(cluster, addresses) : m)
            .toList();
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
        private final boolean allClusters;
        /** The types Envoy asks for as it starts. */
        private final String[] syncTypes;
        private final EnvoyContainer envoy;
        private final ConversationLogger conversations = new ConversationLogger();
        private @Nullable BindableService service;
        private @Nullable Server server;
        /** The control plane's port, picked on the first start; 0 until then. */
        private int adsPort;
        private @Nullable Checkpoint checkpoint;

        Harness(Supplier<ControlPlane> newControlPlane, String envoyImage, Protocol protocol, boolean rateLimitedAds, boolean allClusters) {
            this.newControlPlane = newControlPlane;
            this.syncTypes = allClusters ? new String[]{XdsTypes.CDS, XdsTypes.LDS, XdsTypes.RDS} : new String[]{XdsTypes.LDS, XdsTypes.RDS};
            this.controlPlane = newControlPlane.get();
            this.protocol = protocol;
            this.rateLimitedAds = rateLimitedAds;
            this.allClusters = allClusters;
            this.envoy = new EnvoyContainer(envoyImage)
                .withCommand("-c", "/envoy.yaml")
                .withExposedPorts(EnvoyContainer.ADMIN_PORT, LISTENER_PORT, DYNAMIC_LISTENER_PORT)
                .withAccessToHost(true);
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

        /**
         * Only a service's endpoints change, the commonest change in a mesh: one replaced and one added, then one removed.
         * Envoy must route to the new endpoints, without rebuilding the cluster.
         */
        void changesEndpoints() throws IOException {
            controlPlane.setConfig(services("foo"));
            start();
            Conversation conversation = synced(1);
            assertRoutedToEndpoints("foo", "127.0.0.1");
            final long clustersModified = envoy.admin().stats().getOrDefault("cluster_manager.cluster_modified", 0L);

            change(conversation, withEndpoints(services("foo"), "foo", "127.0.0.2", "127.0.0.3"));
            checkpoint().assertSane();
            assertRoutedToEndpoints("foo", "127.0.0.2", "127.0.0.3");

            change(conversation, withEndpoints(services("foo"), "foo", "127.0.0.3"));
            checkpoint().assertSane();
            assertRoutedToEndpoints("foo", "127.0.0.3");
            assertThat(envoy.admin().stats()).as("clusters modified; only endpoints changed")
                .containsEntry("cluster_manager.cluster_modified", clustersModified);
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

        /**
         * Scoped routes: a listener picking a scope by Host, each naming its own route config. A scope, its route
         * config and cluster added in one change, then removed in one change: the scope must go before its route config,
         * and the route config before its cluster.
         */
        void scopedRoutes() throws IOException {
            controlPlane.setConfig(withScopes(services("foo"), false, "foo"));
            start();
            Conversation conversation = synced(1);
            EnvoyChecks.assertRoutedVia(envoy, DYNAMIC_LISTENER_PORT, "foo", "foo");

            change(conversation, withScopes(services("foo", "bar"), false, "foo", "bar"));
            checkpoint().assertSane();
            EnvoyChecks.assertRoutedVia(envoy, DYNAMIC_LISTENER_PORT, "bar", "bar");

            change(conversation, withScopes(services("foo"), false, "foo"));
            await().atMost(10, TimeUnit.SECONDS).until(settled(conversation));
            checkpoint().assertSane();
            assertStatus(DYNAMIC_LISTENER_PORT, "bar", 404);
            EnvoyChecks.assertRoutedVia(envoy, DYNAMIC_LISTENER_PORT, "foo", "foo");
        }

        /** On-demand scopes: Envoy asks for a scope's route config when the first request for it arrives. */
        void onDemandScopes() throws IOException {
            controlPlane.setConfig(withScopes(services("foo", "bar"), true, "foo", "bar"));
            start();
            Conversation conversation = synced(1);

            assertFirstRequestRoutedVia(conversation, "foo", "foo");
            assertFirstRequestRoutedVia(conversation, "bar", "bar");
        }

        /**
         * On-demand clusters (ODCDS): Envoy subscribes to no clusters as it starts, and asks for one, then its
         * endpoints, when the first request routed to it arrives.
         */
        void onDemandClusters() throws IOException {
            controlPlane.setConfig(clustersOnDemand("foo", "bar"));
            start();
            Conversation conversation = synced(1);

            assertFirstRequestRoutedVia(conversation, "foo", "foo");
            assertFirstRequestRoutedVia(conversation, "bar", "bar");
        }

        /**
         * Virtual hosts by VHDS: Envoy's route config has none of its own. Envoy holds the route config until it gets
         * virtual hosts under the route config's name, so a library sends some (here, all) to start, and the rest when
         * Envoy asks for one on the first request for its host. Either way, requests for each host get routed.
         */
        void onDemandVirtualHosts() throws IOException {
            controlPlane.setConfig(withVirtualHostsOnDemand(services("foo", "bar"), "foo", "bar"));
            start();
            Conversation conversation = synced(1);

            for (String name : List.of("foo", "bar")) {
                // Unlike a held on-demand request, one for a virtual host sent ahead can race Envoy's workers getting it,
                // so retry.
                EnvoyChecks.assertRoutedVia(envoy, DYNAMIC_LISTENER_PORT, name, name);
                await().atMost(10, TimeUnit.SECONDS).until(settled(conversation));
                checkpoint().assertSane();
            }
        }

        /** Start the control plane, then Envoy if it isn't running, so Envoy doesn't back off past its initial fetch timeout. */
        private void start() throws IOException {
            if (service == null) {
                service = controlPlane.service();
            }
            server = ServerBuilder.forPort(adsPort).addService(service).intercept(conversations).build();
            server.start();
            if (adsPort == 0) {
                // A free port, kept for restarts: Envoy's bootstrap names it.
                adsPort = server.getPort();
                org.testcontainers.Testcontainers.exposeHostPorts(adsPort);
                envoy.withCopyToContainer(Transferable.of(bootstrap(protocol, rateLimitedAds, allClusters, adsPort)), "/envoy.yaml");
            }
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
                () -> envoyAssert(conversation).hadConversationsWith(syncTypes).answeredEveryType());
            // A warming listener isn't listening yet, so a first request would be reset. Its last subscription, e.g. SRDS,
            // can still be queued in Envoy when everything sent so far is ACKed and the control plane looks idle.
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(envoy.admin().stats())
                .as("Envoy's listeners have warmed").containsEntry("listener_manager.total_listeners_warming", 0L));
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
            assertStatus(LISTENER_PORT, virtualHost, status);
        }

        private void assertStatus(int port, String virtualHost, int status) {
            // Envoy ACKs once its main thread has the config; workers get it a moment later.
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(envoy.httpStatus(port, virtualHost)).as(virtualHost).isEqualTo(status));
        }

        /**
         * The cluster of this name has just these endpoints, and a request for the virtual host of this name went to one
         * of them: its rq_total went up. Retries while Envoy's workers catch up.
         */
        private void assertRoutedToEndpoints(String name, String... addresses) {
            List<String> hosts = Stream.of(addresses).map(a -> name + "::" + a + ":" + EnvoyContainer.ADMIN_PORT).toList();
            await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                Map<String, Long> before = envoy.admin().clusters();
                assertThat(before.keySet().stream().filter(k -> k.startsWith(name + "::") && k.endsWith("::rq_total")).map(k -> k.replace("::rq_total", "")))
                    .as(name + "'s endpoints").containsExactlyInAnyOrderElementsOf(hosts);
                assertThat(envoy.httpStatus(LISTENER_PORT, name)).as("status for " + name).isEqualTo(200);
                Map<String, Long> after = envoy.admin().clusters();
                assertThat(hosts.stream().mapToLong(h -> after.getOrDefault(h + "::rq_total", 0L) - before.getOrDefault(h + "::rq_total", 0L)).sum())
                    .as("requests to " + hosts).isPositive();
            });
        }

        /**
         * The first request for something Envoy gets on demand is held until it arrives, so it succeeds first time: no
         * retrying. Then everything Envoy was sent for it must have been make before break.
         */
        private void assertFirstRequestRoutedVia(Conversation conversation, String virtualHost, String cluster) throws IOException {
            assertThat(envoy.httpStatus(DYNAMIC_LISTENER_PORT, virtualHost)).as("first request for " + virtualHost).isEqualTo(200);
            await().atMost(10, TimeUnit.SECONDS).until(settled(conversation));
            checkpoint().assertSane();
            EnvoyChecks.assertRoutedVia(envoy, DYNAMIC_LISTENER_PORT, virtualHost, cluster);
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
