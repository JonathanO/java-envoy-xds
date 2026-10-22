package net.woaf.jono.envoy.e2e;

import static net.woaf.jono.envoy.testing.configdump.EnvoyAdminClient.sneakyUnpack;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import net.woaf.jono.envoy.testing.assertions.EnvoyAssert;
import net.woaf.jono.envoy.testing.EnvoyChecks;
import net.woaf.jono.envoy.testing.Settled;
import net.woaf.jono.envoy.testing.configdump.EnvoyAdminClient;
import net.woaf.jono.envoy.testing.configdump.TypedEnvoyConfigDump;
import net.woaf.jono.envoy.testing.utils.ConversationLogger;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import net.woaf.jono.envoy.e2e.utils.DiscoveryServiceManagerEventListener;
import net.woaf.jono.envoy.testing.utils.EnvoyContainer;
import net.woaf.jono.envoy.testing.utils.GrpcXdsV3ProtocolLoggingInterceptor;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;
import net.woaf.jono.envoy.example.ClusterManager;
import net.woaf.jono.envoy.example.config.ClientConfig;
import net.woaf.jono.envoy.example.config.StaticClientConfigSource;
import net.woaf.jono.envoy.example.configbuilder.ClusterConfigBuilder;
import net.woaf.jono.envoy.example.configbuilder.ClusterLoadAssignmentConfigBuilder;
import net.woaf.jono.envoy.example.configbuilder.RouteConfigurationConfigBuilder;
import net.woaf.jono.envoy.example.state.SimpleUpdate;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.output.Slf4jLogConsumer;

@ExtendWith(MockitoExtension.class)
public abstract class BaseEnvoyIntTest<AsserterT extends EnvoyAssert<AsserterT>> {

    private static final int LISTENER_PORT = 10000;

    private final EnvoyContainer envoy = new EnvoyContainer(getEnvoyImageName())
        .withClasspathResourceMapping(getEnvoyConfigFileName(), "/envoy-custom.yaml", BindMode.READ_ONLY)
        .withCommand("-l", "debug", "-c", "/envoy-custom.yaml")
        .withExposedPorts(EnvoyContainer.ADMIN_PORT, LISTENER_PORT)
        .withAccessToHost(true)
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(BaseEnvoyIntTest.class)));
    Server server = null;
    EnvoyAdminClient adminClient = null;
    private volatile int logMark = 0;
    private volatile int messageMark = 0;

    protected abstract String getEnvoyImageName();

    protected abstract String getEnvoyConfigFileName();

    protected abstract boolean hasBrokenWildcardReconnect();

    protected abstract InstanceOfAssertFactory<Conversation, AsserterT> assertFactory();

    @AfterEach
    public void stopServer() throws InterruptedException {
        envoy.stop();
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination();
            server = null;
        }
    }

    /**
     * Start our ADS, then Envoy if it isn't running yet. In that order so Envoy connects at once, rather than backing
     * off for long enough to time out its initial fetches.
     */
    protected void startServer(AggregatedDiscoveryService<?, ?> ads, ConversationLogger conversationInterceptor) throws IOException {
        server = ServerBuilder.forPort(6566).addService(ads).intercept(conversationInterceptor).intercept(new GrpcXdsV3ProtocolLoggingInterceptor()).build();
        server.start();
        org.testcontainers.Testcontainers.exposeHostPorts(6566);
        if (!envoy.isRunning()) {
            envoy.start();
            adminClient = envoy.admin();
        }
    }

    protected abstract AsserterT envoyAssertThat(Conversation conversation);

    /**
     * Call once Envoy has synced: everything it asked for must have been answered by now, or it sits waiting out its
     * initial fetch timeout. The stricter assertSettled() checks start from here, and Envoy's log so far is ignored,
     * e.g. warnings from startup or from a deliberate disconnect.
     */
    protected void startChecking(Conversation conversation) {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> envoyAssertThat(conversation).answeredEveryType());
        logMark = envoy.logSize();
        messageMark = conversation.size();
    }

    /**
     * Everything we expect once Envoy has settled after a change: a sane conversation, nothing rejected, nothing stuck
     * warming or waiting on an initial fetch, still connected, and no warnings or errors logged since startChecking().
     * @param conversation the current conversation
     * @param reconnected true if Envoy reconnected to start this conversation. We can't know what config it kept, so
     *     make before break can't be checked, and old Envoys may wrongly resend resource names for wildcard types.
     */
    protected void assertSettled(Conversation conversation, boolean reconnected) {
        AsserterT asserter = envoyAssertThat(conversation)
            .nothingSentUnasked(reconnected && hasBrokenWildcardReconnect())
            .allWereAcked()
            .sentNoEmptyResponses(messageMark);
        if (!reconnected) {
            asserter.wasMakeBeforeBreak(messageMark);
        }
        EnvoyChecks.assertHealthy(envoy);
        EnvoyChecks.assertNoWarningsSince(envoy, logMark);
    }

    /**
     * Status code of a request through Envoy's listener to a virtual host.
     */
    protected int httpStatus(String virtualHost) throws IOException {
        return envoy.httpStatus(LISTENER_PORT, virtualHost);
    }

    /**
     * Wait for a change made after the conversation had fromMessage messages to be sent and settle.
     */
    private static void awaitChange(Conversation conversation, int fromMessage, Settled.Hook controlPlane) {
        await().atMost(30, TimeUnit.SECONDS).until(Settled.settledAfter(conversation, fromMessage, Settled.settled(conversation, controlPlane)));
    }

    protected AggregatedDiscoveryService<SimpleUpdate, ClientConfig> configureAds(ClusterManager clusterManager, List<DiscoveryServiceManagerEventListener> conversations) {
        return new AggregatedDiscoveryService<>(
            clusterManager, // Cluster managers track the network state and tell us when it changes
            ImmutableList.of(// ConfigBuilders generate envoy configuration (xDS messages) based on network state changes and the client config
                new ClusterConfigBuilder(),
                new ClusterLoadAssignmentConfigBuilder(),
                new RouteConfigurationConfigBuilder()
            ),
            new StaticClientConfigSource(), // ClientConfigProviders provide per-client configuration to the config builders
            ImmutableList.of(), // Our example has no event listeners, but you can use these for things like tracking connected clients
            MergingStateBacklog.factory(), // Strategy for handling a backlog of network state updates.
            () -> {
                DiscoveryServiceManagerEventListener listener = new DiscoveryServiceManagerEventListener();
                conversations.add(listener);
                return listener;
            }
        );
    }

    @Test
    public void simpleConversation() throws IOException {
        ClusterManager clusterManager = new ClusterManager();

        List<DiscoveryServiceManagerEventListener> conversations = Collections.synchronizedList(new ArrayList<>());

        AggregatedDiscoveryService<SimpleUpdate, ClientConfig> ads = configureAds(clusterManager, conversations);

        ConversationLogger conversationInterceptor = new ConversationLogger();
        startServer(ads, conversationInterceptor);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(
            () -> assertThat(conversationInterceptor.getConversations())
                .isNotEmpty()
                .first(assertFactory())
                    .hadConversationsWith(TypeUrl.CDS.getTypeUrl(), TypeUrl.RDS.getTypeUrl())
        );
        await().atMost(30, TimeUnit.SECONDS).until(() -> conversations.size() > 0 && conversations.get(0).isIdle());

        envoyAssertThat(conversationInterceptor.getConversations().get(0)).nothingSentUnasked().allWereAcked();
        assertThat(conversations.get(0).getPushed()).isZero(); // We have not pushed any changes yet.
        startChecking(conversationInterceptor.getConversations().get(0));

        clusterManager.setEndpoints("foo", HostAndPort.fromString("127.0.0.5"), HostAndPort.fromString("127.0.0.6"));

        await().atMost(30, TimeUnit.SECONDS).until(() -> conversations.get(0).getPushed() > 0 && conversations.get(0).isIdle());

        assertThat(conversations).hasSize(1);
        assertThat(conversationInterceptor.getConversations()).hasSize(1);

        assertSettled(conversationInterceptor.getConversations().get(0), false);

        TypedEnvoyConfigDump dump = adminClient.configDump();

        assertThat(dump.getRoutesConfigDump().getDynamicRouteConfigsCount()).isEqualTo(1);
        assertThat(getClusters(dump)).extracting(Cluster::getName).containsExactlyInAnyOrder("foo");

    }

    @Test
    public void envoyReconnect() throws IOException, InterruptedException {
        ClusterManager clusterManager = new ClusterManager();

        List<DiscoveryServiceManagerEventListener> conversations = Collections.synchronizedList(new ArrayList<>());

        AggregatedDiscoveryService<SimpleUpdate, ClientConfig> ads = configureAds(clusterManager, conversations);

        ConversationLogger conversationInterceptor = new ConversationLogger();
        startServer(ads, conversationInterceptor);
        {
            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertThat(conversationInterceptor.getConversations()).isNotEmpty().first(assertFactory()).hadConversationsWith(TypeUrl.CDS.getTypeUrl(), TypeUrl.RDS.getTypeUrl()));
            await().atMost(30, TimeUnit.SECONDS).until(() -> conversations.size() > 0 && conversations.get(0).isIdle());

            envoyAssertThat(conversationInterceptor.getConversations().get(0)).nothingSentUnasked().allWereAcked();

            TypedEnvoyConfigDump dump = adminClient.configDump();
            assertThat(dump.getRoutesConfigDump().getDynamicRouteConfigsCount()).isEqualTo(1);
            assertThat(dump.getClustersConfigDump().getDynamicActiveClustersCount()).isEqualTo(0);
            assertThat(conversations.get(0).getPushed()).isZero();
            startChecking(conversationInterceptor.getConversations().get(0));
        }

        clusterManager.setEndpoints(ImmutableMap.of(
            "foo", ImmutableList.of(HostAndPort.fromString("127.0.0.5"), HostAndPort.fromString("127.0.0.6")),
            "bar", ImmutableList.of(HostAndPort.fromString("127.0.0.7"), HostAndPort.fromString("127.0.0.8"))
        ));

        {
            await().atMost(30, TimeUnit.SECONDS).until(() -> conversations.get(0).getPushed() > 0 && conversations.get(0).isIdle());

            assertThat(conversations).hasSize(1);
            assertThat(conversationInterceptor.getConversations()).hasSize(1);

            assertSettled(conversationInterceptor.getConversations().get(0), false);

            TypedEnvoyConfigDump dump = adminClient.configDump();
            assertThat(dump.getRoutesConfigDump().getDynamicRouteConfigsCount()).isEqualTo(1);
            assertThat(getClusters(dump)).extracting(Cluster::getName).containsExactlyInAnyOrder("foo", "bar");
        }

        // Restart the grpc service, keeping the cluster manager, etc, intact.
        // This forces envoy to reconnect to us.
        server.shutdownNow();
        server.awaitTermination();

        // While envoy's definitely not connected, remove a service entirely.
        // When envoy reconnects and sends us the list of stuff it knew about, we should immediately tell it remove the lost service.
        clusterManager.removeService("bar");

        startServer(ads, conversationInterceptor);

        {
            await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(conversationInterceptor.getConversations()).hasSize(2).last(assertFactory()).hadConversationsWith(TypeUrl.CDS.getTypeUrl(), TypeUrl.RDS.getTypeUrl(), TypeUrl.EDS.getTypeUrl()));
            await().atMost(30, TimeUnit.SECONDS).until(() -> conversations.size() > 1 && conversations.get(1).isIdle());

            envoyAssertThat(conversationInterceptor.getConversations().get(1)).nothingSentUnasked().allWereAcked();

            TypedEnvoyConfigDump dump = adminClient.configDump();
            assertThat(dump.getRoutesConfigDump().getDynamicRouteConfigsCount()).isEqualTo(1);
            assertThat(getClusters(dump)).extracting(Cluster::getName).containsExactlyInAnyOrder("foo");
            assertThat(conversations.get(1).getPushed()).isZero();
            startChecking(conversationInterceptor.getConversations().get(1)); // We caused the disconnect, so ignore Envoy's warnings about it.
        }

        clusterManager.setEndpoints("baz", HostAndPort.fromString("127.0.0.9"), HostAndPort.fromString("127.0.0.10"));

        {
            await().atMost(30, TimeUnit.SECONDS).until(() -> conversations.get(1).getPushed() > 0 && conversations.get(1).isIdle());

            assertSettled(conversationInterceptor.getConversations().get(1), true);

            TypedEnvoyConfigDump dump = adminClient.configDump();
            assertThat(getClusters(dump)).extracting(Cluster::getName).containsExactlyInAnyOrder("foo", "baz");
        }

    }

    @Test
    public void changesWhileConnected() throws IOException {
        ClusterManager clusterManager = new ClusterManager();

        List<DiscoveryServiceManagerEventListener> conversations = Collections.synchronizedList(new ArrayList<>());

        AggregatedDiscoveryService<SimpleUpdate, ClientConfig> ads = configureAds(clusterManager, conversations);

        ConversationLogger conversationInterceptor = new ConversationLogger();
        startServer(ads, conversationInterceptor);

        // Envoy's own admin port stands in for upstream servers: every 127/8 address in its container reaches it.
        clusterManager.setEndpoints(ImmutableMap.of(
            "foo", ImmutableList.of(HostAndPort.fromString("127.0.0.1:9901")),
            "bar", ImmutableList.of(HostAndPort.fromString("127.0.0.2:9901"))
        ));
        await().atMost(30, TimeUnit.SECONDS).until(() -> !conversations.isEmpty() && conversations.get(0).isIdle()
            && getClusters(adminClient.configDump()).size() == 2);
        final DiscoveryServiceManagerEventListener listener = conversations.get(0);
        final Conversation conversation = conversationInterceptor.getConversations().get(0);
        startChecking(conversation);
        assertSettled(conversation, false);
        assertThat(httpStatus("foo")).isEqualTo(200);
        assertThat(httpStatus("bar")).isEqualTo(200);

        // Change only the endpoints.
        int mark = conversation.size();
        clusterManager.setEndpoints("foo", HostAndPort.fromString("127.0.0.3:9901"), HostAndPort.fromString("127.0.0.4:9901"));
        awaitChange(conversation, mark, listener);
        assertSettled(conversation, false);
        assertThat(httpStatus("foo")).isEqualTo(200);

        // Remove a service. Envoy drops its EDS subscription, which needs no response.
        mark = conversation.size();
        clusterManager.removeService("bar");
        awaitChange(conversation, mark, listener);
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> envoyAssertThat(conversation).unsubscribedFrom(TypeUrl.EDS.getTypeUrl(), "bar"));
        await().atMost(30, TimeUnit.SECONDS).until(listener::isIdle);
        assertSettled(conversation, false);
        assertThat(httpStatus("bar")).isEqualTo(404);
        assertThat(httpStatus("foo")).isEqualTo(200);

        // Envoy still takes updates afterwards.
        mark = conversation.size();
        clusterManager.setEndpoints("baz", HostAndPort.fromString("127.0.0.5:9901"));
        awaitChange(conversation, mark, listener);
        assertSettled(conversation, false);
        assertThat(getClusters(adminClient.configDump())).extracting(Cluster::getName).containsExactlyInAnyOrder("foo", "baz");
        assertThat(httpStatus("baz")).isEqualTo(200);

        assertThat(conversationInterceptor.getConversations()).hasSize(1);
    }

    private List<Cluster> getClusters(TypedEnvoyConfigDump dump) {
        return dump.getClustersConfigDump().getDynamicActiveClustersList().stream()
            .map(c -> sneakyUnpack(c.getCluster(), Cluster.class)) // Collect the names of each of the clusters.
            .collect(Collectors.toList());
    }

}
