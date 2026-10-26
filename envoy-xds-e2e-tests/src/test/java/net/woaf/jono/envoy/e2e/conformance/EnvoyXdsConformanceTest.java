package net.woaf.jono.envoy.e2e.conformance;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.grpc.BindableService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
 * Runs the xDS conformance suite against envoy-xds, through the snapshot module.
 */
class EnvoyXdsConformanceTest extends XdsConformanceTest {

    /**
     * SotW's CDS answer is the whole set, so on a reconnect it removes a cluster Envoy kept and its kept routes still use.
     * We no longer have that cluster's config to send again.
     */
    private static final String SOTW_RECONNECT = "Known limit: after a SotW reconnect, a cluster Envoy kept is removed while its kept routes still use it";

    @Override
    protected ControlPlane newControlPlane() {
        return new EnvoyXds();
    }

    @Override
    protected Map<String, String> knownDeviations() {
        return Map.of("sotw/changesWhileDisconnected", SOTW_RECONNECT, "sotw/controlPlaneRestarts", SOTW_RECONNECT);
    }

    private static final class EnvoyXds implements ControlPlane {

        // Every Envoy sees one view of two groups, so real Envoys check that a view merges its groups.
        private final SnapshotSource<String, String, String> source = new SnapshotSource<>(node -> "envoy");

        {
            source.update(c -> c.view("envoy", "routing", "clusters"));
        }

        private final List<ClientHandle> clients = Collections.synchronizedList(new ArrayList<>());

        private final List<DiscoveryServiceManagerEventListener> streams = Collections.synchronizedList(new ArrayList<>());

        @Override
        public BindableService service() {
            return new AggregatedDiscoveryService<>(
                source,
                SnapshotConfigBuilder.forTypes(Cluster.class, ClusterLoadAssignment.class, Listener.class, RouteConfiguration.class),
                node -> XdsConfig.<String>of(node.getId()),
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
        public void setConfig(Collection<? extends Message> resources) {
            Snapshot.Editor routing = Snapshot.empty().edit();
            Snapshot.Editor clusters = Snapshot.empty().edit();
            resources.forEach(r -> (r instanceof Listener || r instanceof RouteConfiguration ? routing : clusters).put(r));
            source.update(c -> c.group("routing", routing.build()).group("clusters", clusters.build()));
        }

        @Override
        public Settled.Hook idleHook() {
            return () -> {
                synchronized (streams) {
                    return streams.isEmpty() || streams.get(streams.size() - 1).isIdle();
                }
            };
        }

        @Override
        public boolean resync() {
            clients.get(clients.size() - 1).resync();
            return true;
        }
    }
}
