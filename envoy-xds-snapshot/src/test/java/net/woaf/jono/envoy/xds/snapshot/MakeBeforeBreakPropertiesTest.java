package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import dev.hegel.HegelTest;
import dev.hegel.Invariant;
import dev.hegel.Rule;
import dev.hegel.Stateful;
import dev.hegel.TestCase;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.BuildVersion;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import io.envoyproxy.envoy.type.v3.SemanticVersion;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;

import static dev.hegel.Generators.booleans;
import static dev.hegel.Generators.sampledFrom;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cla;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cluster;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.listener;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.route;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Make before break, against a model of what an Envoy holds. A delta ADS stream runs in-process on the real stack.
 * Between random snapshot changes, the model applies our responses one at a time, as Envoy does: it ACKs each, then
 * subscribes to the endpoints of the clusters it holds and the route configs of its listeners. After every response,
 * what it serves from must refer only to config it holds, and once everything is delivered it holds the snapshot.
 */
public class MakeBeforeBreakPropertiesTest {

    private static final List<String> CLUSTERS = List.of("a", "b", "c");
    private static final List<String> ROUTE_CONFIGS = List.of("r1", "r2");
    private static final List<String> LISTENERS = List.of("l1", "l2");

    @HegelTest
    void envoyOnlyServesFromConfigItHolds(TestCase tc) {
        Stateful.run(new Machine(), tc);
    }

    /** A consistent snapshot: every reference resolves. */
    private static Snapshot drawSnapshot(TestCase tc) {
        Snapshot.Editor editor = Snapshot.empty().edit();
        List<String> clusters = CLUSTERS.stream().filter(c -> tc.draw(booleans(), "has cluster " + c)).toList();
        for (String c : clusters) {
            editor.put(cluster(c)).put(cla(c, tc.draw(sampledFrom("10.0.0.1", "10.0.0.2"), "endpoint of " + c)));
        }
        List<String> routeConfigs = new ArrayList<>();
        for (String r : ROUTE_CONFIGS) {
            if (tc.draw(booleans(), "has route config " + r)) {
                editor.put(route(r, clusters.stream().filter(c -> tc.draw(booleans(), r + " routes to " + c)).toArray(String[]::new)));
                routeConfigs.add(r);
            }
        }
        for (String l : LISTENERS) {
            if (!routeConfigs.isEmpty() && tc.draw(booleans(), "has listener " + l)) {
                editor.put(listener(l, tc.draw(sampledFrom(routeConfigs), l + " uses")));
            }
        }
        return editor.build();
    }

    /** Tells subscribers about each snapshot straight away, so the test controls every step. */
    private static final class SyncSource implements ClusterEventSource<SnapshotUpdate<Object>> {
        private final List<ClusterManagerEventListener<SnapshotUpdate<Object>>> subscribers = new ArrayList<>();
        private Published<Object> current = Published.none();

        @Override
        public SnapshotUpdate<Object> subscribe(ClusterManagerEventListener<SnapshotUpdate<Object>> listener) {
            subscribers.add(listener);
            return new SnapshotUpdate<>(Published.none(), current, null);
        }

        @Override
        public void unsubscribe(ClusterManagerEventListener<SnapshotUpdate<Object>> listener) {
            subscribers.remove(listener);
        }

        void publish(Snapshot next) {
            Published<Object> published = new Published<>(next, null);
            SnapshotUpdate<Object> update = new SnapshotUpdate<>(current, published, null);
            current = published;
            List.copyOf(subscribers).forEach(s -> s.onNetworkChange(update));
        }

        Snapshot current() {
            return current.base();
        }
    }

    /** What an Envoy holds, and the route config each listener serves from. */
    private static final class Envoy implements StreamObserver<DeltaDiscoveryResponse> {
        private final Deque<DeltaDiscoveryResponse> inbox = new ArrayDeque<>();
        private final Map<String, Cluster> clusters = new HashMap<>();
        private final Map<String, ClusterLoadAssignment> endpoints = new HashMap<>();
        private final Map<String, Listener> listeners = new HashMap<>();
        private final Map<String, RouteConfiguration> routeConfigs = new HashMap<>();
        // A listener whose route config hasn't arrived warms; until then Envoy keeps serving the one it had.
        private final Map<String, String> serving = new HashMap<>();
        private final Set<String> edsSubscriptions = new HashSet<>();
        private final Set<String> rdsSubscriptions = new HashSet<>();
        // Clusters that have had an EDS answer; until then they warm.
        private final Set<String> edsAnswered = new HashSet<>();
        // Envoy pauses CDS while clusters warm (ClusterManagerImpl), so its ACK waits for their endpoints.
        private @Nullable DeltaDiscoveryResponse unackedCds = null;
        private final List<String> problems = new ArrayList<>();
        private StreamObserver<DeltaDiscoveryRequest> server = new NoServer();

        void connect(StreamObserver<DeltaDiscoveryRequest> requests) {
            server = requests;
            Node node = Node.newBuilder().setId("envoy").setCluster("test").setUserAgentName("envoy")
                .setUserAgentBuildVersion(BuildVersion.newBuilder().setVersion(SemanticVersion.newBuilder().setMajorNumber(1).setMinorNumber(36)))
                .build();
            // Wildcard subscriptions, as Envoy's first requests.
            server.onNext(DeltaDiscoveryRequest.newBuilder().setNode(node).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());
            server.onNext(DeltaDiscoveryRequest.newBuilder().setNode(node).setTypeUrl(TypeUrl.LDS.getTypeUrl()).build());
        }

        @Override
        public void onNext(DeltaDiscoveryResponse response) {
            inbox.add(response);
        }

        @Override
        public void onError(Throwable t) {
            problems.add("stream failed: " + t);
        }

        @Override
        public void onCompleted() {
            problems.add("stream closed");
        }

        boolean hasMail() {
            return !inbox.isEmpty();
        }

        /** Apply the next response, ACK it, then follow the new config's references with subscriptions. */
        void deliver() {
            DeltaDiscoveryResponse response = inbox.remove();
            apply(response);
            check(response);
            if (response.getTypeUrl().equals(TypeUrl.CDS.getTypeUrl())) {
                unackedCds = response;
            } else {
                ack(response);
            }
            resubscribe(TypeUrl.EDS, edsSubscriptions, clusters.keySet(), endpoints);
            edsAnswered.retainAll(clusters.keySet());
            if (unackedCds != null && edsAnswered.containsAll(clusters.keySet())) {
                ack(unackedCds);
                unackedCds = null;
            }
            Set<String> wantedRoutes = new HashSet<>(serving.values());
            listeners.values().forEach(l -> wantedRoutes.add(routeConfigOf(l)));
            resubscribe(TypeUrl.RDS, rdsSubscriptions, wantedRoutes, routeConfigs);
        }

        private void ack(DeltaDiscoveryResponse response) {
            server.onNext(DeltaDiscoveryRequest.newBuilder().setTypeUrl(response.getTypeUrl()).setResponseNonce(response.getNonce()).build());
        }

        void settle() {
            for (int i = 0; i < 1000 && hasMail(); i++) {
                deliver();
            }
            assertThat(inbox).as("still answering after 1000 responses").isEmpty();
        }

        private void apply(DeltaDiscoveryResponse response) {
            String type = response.getTypeUrl();
            if (type.equals(TypeUrl.CDS.getTypeUrl())) {
                update(response, clusters, Cluster.class, null, true);
            } else if (type.equals(TypeUrl.EDS.getTypeUrl())) {
                update(response, endpoints, ClusterLoadAssignment.class, edsSubscriptions, true);
                response.getResourcesList().forEach(r -> edsAnswered.add(r.getName()));
                edsAnswered.addAll(response.getRemovedResourcesList());
            } else if (type.equals(TypeUrl.LDS.getTypeUrl())) {
                update(response, listeners, Listener.class, null, true);
                serving.keySet().retainAll(listeners.keySet());
            } else if (type.equals(TypeUrl.RDS.getTypeUrl())) {
                // Envoy ignores delta RDS removals (RdsRouteConfigSubscription::onConfigUpdate): it drops a route config
                // when nothing uses it, by unsubscribing.
                update(response, routeConfigs, RouteConfiguration.class, rdsSubscriptions, false);
            } else {
                problems.add("sent " + type + ", which Envoy never asked for");
            }
            // A warming listener starts serving once its route config arrives.
            listeners.forEach((name, l) -> {
                if (routeConfigs.containsKey(routeConfigOf(l))) {
                    serving.put(name, routeConfigOf(l));
                }
            });
        }

        private void check(DeltaDiscoveryResponse after) {
            serving.forEach((listener, routeConfig) -> {
                RouteConfiguration rc = routeConfigs.get(routeConfig);
                if (rc == null) {
                    problems.add("after " + describe(after) + ", listener " + listener + " serves route config " + routeConfig + ", which Envoy doesn't have");
                    return;
                }
                clustersOf(rc).stream().filter(c -> !clusters.containsKey(c)).forEach(c ->
                    problems.add("after " + describe(after) + ", route config " + routeConfig + " routes to cluster " + c + ", which Envoy doesn't have"));
                // An EDS cluster is warming, and can't serve, until its endpoints arrive.
                clustersOf(rc).stream().filter(c -> clusters.containsKey(c) && !endpoints.containsKey(c)).forEach(c ->
                    problems.add("after " + describe(after) + ", route config " + routeConfig + " routes to cluster " + c + ", which has no endpoints"));
            });
        }

        private void resubscribe(TypeUrl type, Set<String> subscribed, Set<String> wanted, Map<String, ?> held) {
            Set<String> add = Sets.difference(wanted, subscribed).immutableCopy();
            Set<String> drop = Sets.difference(subscribed, wanted).immutableCopy();
            if (add.isEmpty() && drop.isEmpty()) {
                return;
            }
            subscribed.addAll(add);
            subscribed.removeAll(drop);
            held.keySet().removeAll(drop); // Envoy forgets what it no longer subscribes to.
            server.onNext(DeltaDiscoveryRequest.newBuilder().setTypeUrl(type.getTypeUrl())
                .addAllResourceNamesSubscribe(add).addAllResourceNamesUnsubscribe(drop).build());
        }

        void assertHolds(Snapshot snapshot) {
            assertThat(clusters).isEqualTo(snapshot.get(Cluster.class));
            assertThat(endpoints).isEqualTo(snapshot.get(ClusterLoadAssignment.class));
            assertThat(listeners).isEqualTo(snapshot.get(Listener.class));
            Set<String> used = snapshot.get(Listener.class).values().stream().map(Envoy::routeConfigOf).collect(Collectors.toSet());
            assertThat(routeConfigs).isEqualTo(Map.copyOf(Map.copyOf(snapshot.get(RouteConfiguration.class)).entrySet().stream()
                .filter(e -> used.contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
        }

        /** Apply a response; for a type Envoy subscribes to by name, it ignores names it no longer subscribes to. */
        private static <M extends com.google.protobuf.Message> void update(DeltaDiscoveryResponse response, Map<String, M> held, Class<M> type,
                                                                           @Nullable Set<String> subscribed, boolean honoursRemovals) {
            for (Resource resource : response.getResourcesList()) {
                if (subscribed != null && !subscribed.contains(resource.getName())) {
                    continue;
                }
                try {
                    held.put(resource.getName(), resource.getResource().unpack(type));
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException(e);
                }
            }
            if (honoursRemovals) {
                held.keySet().removeAll(response.getRemovedResourcesList());
            }
        }

        private static String routeConfigOf(Listener listener) {
            try {
                return listener.getFilterChains(0).getFilters(0).getTypedConfig().unpack(HttpConnectionManager.class).getRds().getRouteConfigName();
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
        }

        private static Set<String> clustersOf(RouteConfiguration rc) {
            return rc.getVirtualHostsList().stream().map(VirtualHost::getRoutesList).flatMap(List::stream)
                .map(Route::getRoute).map(r -> r.getCluster()).collect(Collectors.toSet());
        }

        private static String describe(DeltaDiscoveryResponse r) {
            return r.getTypeUrl().substring(r.getTypeUrl().lastIndexOf('.') + 1) + " "
                + r.getResourcesList().stream().map(Resource::getName).toList() + " removing " + r.getRemovedResourcesList();
        }
    }

    private static final class NoServer implements StreamObserver<DeltaDiscoveryRequest> {
        @Override
        public void onNext(DeltaDiscoveryRequest value) {
            throw new IllegalStateException("not connected");
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    }

    // Not private: Hegel calls its rules and invariants reflectively, which Error Prone can't see.
    static final class Machine {
        private final SyncSource source = new SyncSource();
        private final Envoy envoy = new Envoy();

        Machine() {
            AggregatedDiscoveryService<SnapshotUpdate<Object>, Object> ads = new AggregatedDiscoveryService<>(
                source,
                SnapshotConfigBuilder.<Object>forTypes(Cluster.class, ClusterLoadAssignment.class, Listener.class, RouteConfiguration.class),
                node -> XdsConfig.of(new Object()),
                ImmutableList.of(),
                MergingStateBacklog.factory(),
                DiscoveryServiceManagerMetrics.METRICS_DISABLED);
            envoy.connect(ads.deltaAggregatedResources(envoy));
        }

        @Rule(weight = 2)
        void publish(TestCase tc) {
            source.publish(drawSnapshot(tc));
        }

        @Rule(weight = 4)
        void deliver(TestCase tc) {
            tc.assume(envoy.hasMail());
            envoy.deliver();
        }

        @Rule
        void settle(TestCase tc) {
            envoy.settle();
            envoy.assertHolds(source.current());
        }

        @Invariant(alwaysRun = true)
        void envoyOnlyServesFromConfigItHolds(TestCase tc) {
            assertThat(envoy.problems).isEmpty();
        }
    }
}
