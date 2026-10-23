package net.woaf.jono.envoy.testing.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.woaf.jono.envoy.testing.EnvoyChecks;
import net.woaf.jono.envoy.testing.Settled;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.assertions.DeltaConversationAssert;
import net.woaf.jono.envoy.testing.assertions.EnvoyAssert;
import net.woaf.jono.envoy.testing.assertions.SotwConversationAssert;
import net.woaf.jono.envoy.testing.utils.ConversationLogger;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import net.woaf.jono.envoy.testing.utils.EnvoyContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

/**
 * Conformance tests for an xDS library: give it desired config, and check a real Envoy gets there safely.
 *
 * <p>This tests the layer that serves config to Envoy (ordering, ACKs and NACKs, subscriptions, reconnects), not how a
 * control plane decides what config to serve. Extend it once per Envoy version and protocol you support, implement
 * {@link #controlPlane()} and {@link #setConfig(Collection)}, and optionally {@link #idleHook()}. Fields of your
 * subclass are fresh for every test. Override a test with {@code @Disabled} to record a known deviation.
 *
 * <p>Envoy connects to port 6566 on the host, so don't run these in parallel.
 */
public abstract class XdsConformanceTest {

    /** Which ADS flavour Envoy uses. */
    public enum Protocol { SOTW, DELTA }

    private static final int ADS_PORT = 6566;
    private static final int LISTENER_PORT = 10000;
    private static final String ROUTES = "all_routes"; // Envoy's static listener asks for this
    private static final Pattern MINOR_VERSION = Pattern.compile(":v1\\.(\\d+)");

    private final Protocol protocol;
    private final boolean brokenWildcardReconnect;
    private final EnvoyContainer envoy;
    private final ConversationLogger conversations = new ConversationLogger();
    private BindableService service;
    private Server server;
    private int logMark;
    private int messageMark;
    private Map<String, Long> statsMark = Map.of();

    protected XdsConformanceTest(String envoyImage, Protocol protocol) {
        this.protocol = protocol;
        Matcher minor = MINOR_VERSION.matcher(envoyImage);
        // Before 1.19 Envoy resent resource names for wildcard types on reconnect.
        this.brokenWildcardReconnect = minor.find() && Integer.parseInt(minor.group(1)) < 19;
        this.envoy = new EnvoyContainer(envoyImage)
            .withCopyFileToContainer(MountableFile.forClasspathResource(
                "net/woaf/jono/envoy/testing/conformance/envoy-" + protocol.name().toLowerCase(Locale.ROOT) + ".yaml"), "/envoy.yaml")
            .withCommand("-l", "debug", "-c", "/envoy.yaml")
            .withExposedPorts(EnvoyContainer.ADMIN_PORT, LISTENER_PORT)
            .withAccessToHost(true)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(XdsConformanceTest.class)));
    }

    /**
     * The control plane's ADS service. Called once per test; the same instance is served again after a reconnect.
     * @return gRPC service serving SotW or delta ADS, whichever this test uses
     */
    protected abstract BindableService controlPlane();

    /**
     * Make these resources the whole desired config for every Envoy, replacing what was there before.
     * @param resources Clusters, ClusterLoadAssignments and RouteConfigurations
     */
    protected abstract void setConfig(Collection<? extends Message> resources);

    /**
     * Whether the control plane has nothing in flight or waiting to be sent on its latest stream. Without one, the suite
     * waits for every response to be answered and a quiet period.
     * @return hook, or null if the control plane can't tell
     */
    protected Settled.Hook idleHook() {
        return null;
    }

    /**
     * Make Envoy drop its stream and sync from scratch, e.g. to get a fixed config to an Envoy that NACKed.
     * @return false if the control plane can't, the default
     */
    protected boolean resync() {
        return false;
    }

    @AfterEach
    public void stop() throws InterruptedException {
        envoy.stop();
        stopServer();
    }

    /** Starting with no config, Envoy gets an answer for every type, then takes the first service. */
    @Test
    public void startsEmpty() throws IOException {
        setConfig(List.of());
        start();
        Conversation conversation = synced(1);
        assertSettled(conversation, false);

        change(conversation, services("foo"));
        assertSettled(conversation, false);
        assertStatus("foo", 200);
    }

    /** A route, its cluster and its endpoints added in one change, then removed in one change. */
    @Test
    public void addsAndRemovesServices() throws IOException {
        setConfig(services("foo"));
        start();
        Conversation conversation = synced(1);

        change(conversation, services("foo", "bar"));
        assertSettled(conversation, false);
        assertStatus("bar", 200);

        change(conversation, services("foo"));
        // Envoy drops its EDS subscription for bar, which needs no response.
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> envoyAssert(conversation).unsubscribedFrom(XdsTypes.EDS, "bar"));
        await().atMost(10, TimeUnit.SECONDS).until(settled(conversation));
        assertSettled(conversation, false);
        assertStatus("bar", 404);
        assertStatus("foo", 200);
    }

    /** A route moved to a new cluster, and its old cluster removed, in one change. */
    @Test
    public void movesRouteToNewCluster() throws IOException {
        setConfig(service("foo", "foo-v1"));
        start();
        Conversation conversation = synced(1);
        assertStatus("foo", 200);

        change(conversation, service("foo", "foo-v2"));
        assertSettled(conversation, false);
        assertStatus("foo", 200);
    }

    /**
     * Envoy rejects a bad cluster, and the library contains it. It doesn't send the bad cluster again, not even as part
     * of a later change, and doesn't drop Envoy into a reconnect loop: retrying is spam, and Envoy resets its reconnect
     * backoff whenever it gets a response. Envoy keeps serving its last good config. Whether a later fix gets through on
     * the same stream is up to the library, but if it can {@link #resync()}, the fix must get through after one.
     */
    @Test
    public void containsRejectedConfig() throws IOException {
        setConfig(services("foo"));
        start();
        synced(1);

        setConfig(withBadCluster(services("foo")));
        await().atMost(10, TimeUnit.SECONDS).until(this::envoyRejectedSomething);
        logMark = envoy.logSize();
        statsMark = envoy.admin().stats();

        setConfig(withBadCluster(services("foo", "bar"))); // still bad
        assertContained();
        setConfig(services("foo", "bar")); // fixed
        assertContained();
        assertStatus("foo", 200);
        EnvoyChecks.assertHealthy(envoy, statsMark);
        EnvoyChecks.assertNoWarningsSince(envoy, logMark);

        if (resync()) {
            Conversation conversation = synced(2);
            assertSettled(conversation, true);
            assertStatus("bar", 200);
        }
    }

    /** Envoy reconnects after a service was removed while it was away: traffic keeps flowing and updates still work. */
    @Test
    public void reconnects() throws IOException, InterruptedException {
        setConfig(services("foo", "bar"));
        start();
        synced(1);
        assertStatus("bar", 200);

        stopServer();
        setConfig(services("foo"));
        start();
        Conversation conversation = synced(2); // Ignores Envoy's warnings about the disconnect.
        assertSettled(conversation, true);
        assertStatus("foo", 200);
        assertStatus("bar", 404);

        change(conversation, services("foo", "baz"));
        assertSettled(conversation, true);
        assertStatus("baz", 200);
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

    /** Start the control plane, then Envoy if it isn't running, so Envoy doesn't back off past its initial fetch timeout. */
    private void start() throws IOException {
        if (service == null) {
            service = controlPlane();
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
    private Conversation synced(int nth) {
        await().atMost(60, TimeUnit.SECONDS).until(() -> conversations.getConversations().size() >= nth);
        Conversation conversation = conversations.getConversations().get(nth - 1);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
            () -> envoyAssert(conversation).hadConversationsWith(XdsTypes.CDS, XdsTypes.RDS).answeredEveryType());
        await().atMost(30, TimeUnit.SECONDS).until(settled(conversation));
        logMark = envoy.logSize();
        messageMark = conversation.size();
        return conversation;
    }

    private void change(Conversation conversation, Collection<? extends Message> config) {
        int mark = conversation.size();
        setConfig(config);
        await().atMost(30, TimeUnit.SECONDS).until(Settled.settledAfter(conversation, mark, settled(conversation)));
    }

    private Callable<Boolean> settled(Conversation conversation) {
        Settled.Hook hook = idleHook();
        return hook == null ? Settled.settled(conversation) : Settled.settled(conversation, hook);
    }

    /**
     * Everything we expect once settled: a sane conversation, and a healthy Envoy that logged no warnings since synced().
     * @param reconnected Envoy reconnected to start this conversation, so we don't know what config it kept and can't
     *     check make before break
     */
    private void assertSettled(Conversation conversation, boolean reconnected) {
        EnvoyAssert<?> asserter = envoyAssert(conversation)
            .nothingSentUnasked(reconnected && brokenWildcardReconnect)
            .allWereAcked()
            .sentNoEmptyResponses(messageMark);
        if (!reconnected) {
            asserter.wasMakeBeforeBreak(messageMark);
        }
        EnvoyChecks.assertHealthy(envoy, statsMark);
        EnvoyChecks.assertNoWarningsSince(envoy, logMark);
    }

    private void assertStatus(String virtualHost, int status) {
        // Envoy ACKs once its main thread has the config; workers get it a moment later.
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(envoy.httpStatus(LISTENER_PORT, virtualHost)).as(virtualHost).isEqualTo(status));
    }

    /** For a while: no more rejections, and no reconnects. */
    private void assertContained() {
        await().during(2, TimeUnit.SECONDS).atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(conversations.getConversations()).as("streams; dropping Envoy after a NACK makes it reconnect in a loop").hasSize(1);
            assertThat(envoy.admin().stats().entrySet().stream()
                .filter(e -> e.getKey().endsWith(".update_rejected") && !e.getValue().equals(statsMark.getOrDefault(e.getKey(), 0L)))
                .toList()).as("rejections since the first NACK; the bad cluster was sent again").isEmpty();
        });
    }

    private boolean envoyRejectedSomething() {
        return conversations.getConversations().stream().flatMap(c -> c.getMessages().stream())
            .anyMatch(m -> (m instanceof DiscoveryRequest r && r.hasErrorDetail()) || (m instanceof DeltaDiscoveryRequest d && d.hasErrorDetail()));
    }

    private EnvoyAssert<?> envoyAssert(Conversation conversation) {
        return protocol == Protocol.DELTA ? DeltaConversationAssert.assertThat(conversation) : SotwConversationAssert.assertThat(conversation);
    }
}
