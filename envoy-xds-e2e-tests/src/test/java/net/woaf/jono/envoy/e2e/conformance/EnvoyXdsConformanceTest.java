package net.woaf.jono.envoy.e2e.conformance;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.grpc.BindableService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import net.woaf.jono.envoy.e2e.utils.DiscoveryServiceManagerEventListener;
import net.woaf.jono.envoy.testing.Settled;
import net.woaf.jono.envoy.testing.conformance.XdsConformanceTest;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.ClientHandle;
import net.woaf.jono.envoy.xds.api.XdsEventListener;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;
import net.woaf.jono.envoy.xds.snapshot.Snapshot;
import net.woaf.jono.envoy.xds.snapshot.SnapshotConfigBuilder;
import net.woaf.jono.envoy.xds.snapshot.SnapshotSource;

/**
 * Runs the xDS conformance suite against envoy-xds, through the snapshot module, on the oldest and newest Envoy we test.
 */
abstract class EnvoyXdsConformanceTest extends XdsConformanceTest {

    private final SnapshotSource<String> source = new SnapshotSource<>();

    private final List<ClientHandle> clients = Collections.synchronizedList(new ArrayList<>());

    private final List<DiscoveryServiceManagerEventListener> streams = Collections.synchronizedList(new ArrayList<>());

    EnvoyXdsConformanceTest(String envoyImage, Protocol protocol) {
        super(envoyImage, protocol);
    }

    @Override
    protected BindableService controlPlane() {
        return new AggregatedDiscoveryService<>(
            source,
            SnapshotConfigBuilder.forTypes(Cluster.class, ClusterLoadAssignment.class, RouteConfiguration.class),
            node -> XdsConfig.<String>builder().clientDetails(node.getId()).build(),
            ImmutableList.of(new XdsEventListener<>() {
                @Override
                public void onNewClient(ClientHandle clientHandle, Node node, XdsConfig<String> config) {
                    clients.add(clientHandle);
                }

                @Override
                public void onClientDisconnected(ClientHandle clientHandle, Node node) {
                }
            }),
            MergingStateBacklog.factory(),
            () -> {
                DiscoveryServiceManagerEventListener listener = new DiscoveryServiceManagerEventListener();
                streams.add(listener);
                return listener;
            });
    }

    @Override
    protected void setConfig(Collection<? extends Message> resources) {
        Snapshot.Editor editor = Snapshot.empty().edit();
        resources.forEach(editor::put);
        source.publish(editor.build());
    }

    @Override
    protected Settled.Hook idleHook() {
        return () -> {
            synchronized (streams) {
                return streams.isEmpty() || streams.get(streams.size() - 1).isIdle();
            }
        };
    }

    @Override
    protected boolean resync() {
        clients.get(clients.size() - 1).resync();
        return true;
    }

    static class SotwOldestTest extends EnvoyXdsConformanceTest {
        SotwOldestTest() {
            super("envoyproxy/envoy:v1.17-latest", Protocol.SOTW);
        }
    }

    static class SotwNewestTest extends EnvoyXdsConformanceTest {
        SotwNewestTest() {
            super("envoyproxy/envoy:v1.40-latest", Protocol.SOTW);
        }
    }

    static class DeltaOldestTest extends EnvoyXdsConformanceTest {
        DeltaOldestTest() {
            super("envoyproxy/envoy:v1.17-latest", Protocol.DELTA);
        }
    }

    static class DeltaNewestTest extends EnvoyXdsConformanceTest {
        DeltaNewestTest() {
            super("envoyproxy/envoy:v1.40-latest", Protocol.DELTA);
        }
    }
}
