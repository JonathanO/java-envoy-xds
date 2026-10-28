package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
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
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.envoyproxy.envoy.type.v3.SemanticVersion;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
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
 * Make before break, against a model of what an Envoy holds. A delta or SotW ADS stream runs in-process on the real stack.
 * Between random snapshot changes, the model applies our responses one at a time, as Envoy does: it ACKs each, then
 * subscribes to the endpoints of the clusters it holds and the route configs of its listeners. After every response,
 * what it serves from must refer only to config it holds, and once everything is delivered it holds the snapshot.
 *
 * <p>The Envoy's view is one or two groups. Each snapshot is split across them at random, with decoys in the group
 * that should lose, and published with the view's list of groups as one change: whole groups or edits of the current
 * ones, so clients compare only the names touched. A group in another view also changes, which this Envoy must never
 * see, so the updates it does get skip versions. Updates wait until a rule delivers them, so several can merge on the
 * way.
 */
public class MakeBeforeBreakPropertiesTest {

    private static final List<String> CLUSTERS = List.of("a", "b", "c");
    private static final List<String> ROUTE_CONFIGS = List.of("r1", "r2");
    private static final List<String> LISTENERS = List.of("l1", "l2");
    private static final List<List<String>> VIEWS = List.of(List.of("own", "shared"), List.of("shared", "own"), List.of("own"), List.of("shared"));

    @HegelTest(testCases = 1000)
    void deltaEnvoyOnlyServesFromConfigItHolds(TestCase tc) {
        Stateful.run(new Machine(true), tc);
    }

    @HegelTest(testCases = 1000)
    void sotwEnvoyOnlyServesFromConfigItHolds(TestCase tc) {
        Stateful.run(new Machine(false), tc);
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

    /** The same resource, but not equal to it: what Envoy holds if a group that should lose wins. */
    private static Message decoy(Message resource) {
        return resource.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
            .addField(99999, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
    }

    /** Splits a snapshot across the view's groups: each resource in one of them, or in the first with a decoy after. */
    private static Map<String, Snapshot> split(TestCase tc, Snapshot snapshot, List<String> view) {
        Map<String, Snapshot.Editor> groups = new LinkedHashMap<>();
        List.of("own", "shared").forEach(g -> groups.put(g, Snapshot.empty().edit()));
        String hidden = view.size() == 1 ? (view.get(0).equals("own") ? "shared" : "own") : null;
        for (Class<? extends Message> type : snapshot.types()) {
            snapshot.get(type).forEach((name, resource) -> {
                String first = view.size() == 1 ? view.get(0) : tc.draw(sampledFrom(view), "group of " + name);
                groups.get(first).put(name, resource);
                String after = hidden != null ? hidden : view.get(1 - view.indexOf(first));
                if (view.indexOf(first) == 0 && tc.draw(booleans(), "decoy of " + name)) {
                    groups.get(after).put(name, decoy(resource));
                }
            });
        }
        Map<String, Snapshot> built = new LinkedHashMap<>();
        groups.forEach((name, editor) -> built.put(name, editor.build()));
        return built;
    }

    /** Edits current into next, so clients compare only the names that changed. */
    private static Snapshot.Editor editTo(Snapshot.Editor editor, Snapshot current, Snapshot next) {
        for (Class<? extends Message> type : Sets.union(current.types(), next.types())) {
            Sets.difference(current.get(type).keySet(), next.get(type).keySet()).immutableCopy().forEach(name -> editor.remove(type, name));
            next.get(type).forEach(editor::put);
        }
        return editor;
    }

    /** A response of either protocol. SotW has no removals: for LDS and CDS, what it leaves out is removed. */
    private record Received(String typeUrl, Map<String, Any> resources, List<String> removed, String version, String nonce) {
    }

    /** What an Envoy holds, and the route config each listener serves from. */
    private static final class Envoy implements StreamObserver<Message> {
        private final boolean delta;
        private final Deque<Received> inbox = new ArrayDeque<>();
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
        private @Nullable Received unackedCds = null;
        // SotW: the last response of each type ACKed, whose version and nonce later requests carry.
        private final Map<String, Received> acked = new HashMap<>();
        private final List<String> problems = new ArrayList<>();
        private StreamObserver<Message> server = new NoServer();

        Envoy(boolean delta) {
            this.delta = delta;
        }

        void connect(StreamObserver<Message> requests) {
            server = requests;
            Node node = Node.newBuilder().setId("envoy").setCluster("test").setUserAgentName("envoy")
                .setUserAgentBuildVersion(BuildVersion.newBuilder().setVersion(SemanticVersion.newBuilder().setMajorNumber(1).setMinorNumber(36)))
                .build();
            // Wildcard subscriptions, as Envoy's first requests.
            for (TypeUrl type : List.of(TypeUrl.CDS, TypeUrl.LDS)) {
                server.onNext(delta
                    ? DeltaDiscoveryRequest.newBuilder().setNode(node).setTypeUrl(type.getTypeUrl()).build()
                    : DiscoveryRequest.newBuilder().setNode(node).setTypeUrl(type.getTypeUrl()).build());
            }
        }

        @Override
        public void onNext(Message message) {
            Map<String, Any> resources = new LinkedHashMap<>();
            if (message instanceof DeltaDiscoveryResponse r) {
                r.getResourcesList().forEach(resource -> resources.put(resource.getName(), resource.getResource()));
                inbox.add(new Received(r.getTypeUrl(), resources, r.getRemovedResourcesList(), "", r.getNonce()));
            } else {
                DiscoveryResponse r = (DiscoveryResponse) message;
                Class<? extends Message> type = Objects.requireNonNull(TypeUrl.of(r.getTypeUrl()), r.getTypeUrl()).getMessageClazz();
                r.getResourcesList().forEach(any -> resources.put(Snapshot.nameOf(unpack(any, type)), any));
                inbox.add(new Received(r.getTypeUrl(), resources, List.of(), r.getVersionInfo(), r.getNonce()));
            }
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
            Received response = inbox.remove();
            apply(response);
            check(response);
            if (response.typeUrl().equals(TypeUrl.CDS.getTypeUrl())) {
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

        private void ack(Received response) {
            if (delta) {
                server.onNext(DeltaDiscoveryRequest.newBuilder().setTypeUrl(response.typeUrl()).setResponseNonce(response.nonce()).build());
                return;
            }
            acked.put(response.typeUrl(), response);
            server.onNext(sotwRequest(response.typeUrl()));
        }

        /** A SotW request lists every name subscribed to, and carries the last version ACKed. */
        private DiscoveryRequest sotwRequest(String typeUrl) {
            DiscoveryRequest.Builder request = DiscoveryRequest.newBuilder().setTypeUrl(typeUrl);
            if (typeUrl.equals(TypeUrl.EDS.getTypeUrl())) {
                request.addAllResourceNames(edsSubscriptions);
            } else if (typeUrl.equals(TypeUrl.RDS.getTypeUrl())) {
                request.addAllResourceNames(rdsSubscriptions);
            }
            Received last = acked.get(typeUrl);
            if (last != null) {
                request.setVersionInfo(last.version()).setResponseNonce(last.nonce());
            }
            return request.build();
        }

        void settle() {
            for (int i = 0; i < 1000 && hasMail(); i++) {
                deliver();
            }
            assertThat(inbox).as("still answering after 1000 responses").isEmpty();
        }

        private void apply(Received response) {
            String type = response.typeUrl();
            if (type.equals(TypeUrl.CDS.getTypeUrl())) {
                update(response, clusters, Cluster.class, null, true);
            } else if (type.equals(TypeUrl.EDS.getTypeUrl())) {
                update(response, endpoints, ClusterLoadAssignment.class, edsSubscriptions, true);
                edsAnswered.addAll(response.resources().keySet());
                edsAnswered.addAll(response.removed());
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

        private void check(Received after) {
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
            server.onNext(delta
                ? DeltaDiscoveryRequest.newBuilder().setTypeUrl(type.getTypeUrl()).addAllResourceNamesSubscribe(add).addAllResourceNamesUnsubscribe(drop).build()
                : sotwRequest(type.getTypeUrl()));
        }

        void assertHolds(Snapshot snapshot) {
            assertThat(clusters).isEqualTo(snapshot.get(Cluster.class));
            assertThat(endpoints).isEqualTo(snapshot.get(ClusterLoadAssignment.class));
            assertThat(listeners).isEqualTo(snapshot.get(Listener.class));
            Set<String> used = snapshot.get(Listener.class).values().stream().map(Envoy::routeConfigOf).collect(Collectors.toSet());
            assertThat(routeConfigs).isEqualTo(Map.copyOf(Map.copyOf(snapshot.get(RouteConfiguration.class)).entrySet().stream()
                .filter(e -> used.contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
        }

        /**
         * Apply a response; for a type Envoy subscribes to by name, it ignores names it no longer subscribes to. A SotW
         * response for a wildcard type (subscribed is null) is the whole set.
         */
        private <M extends Message> void update(Received response, Map<String, M> held, Class<M> type,
                                                @Nullable Set<String> subscribed, boolean honoursRemovals) {
            response.resources().forEach((name, any) -> {
                if (subscribed == null || subscribed.contains(name)) {
                    held.put(name, unpack(any, type));
                }
            });
            if (honoursRemovals) {
                held.keySet().removeAll(response.removed());
            }
            if (!delta && subscribed == null) {
                held.keySet().retainAll(response.resources().keySet());
            }
        }

        private static <M extends Message> M unpack(Any any, Class<M> type) {
            try {
                return any.unpack(type);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
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

        private static String describe(Received r) {
            return r.typeUrl().substring(r.typeUrl().lastIndexOf('.') + 1) + " " + r.resources().keySet() + " removing " + r.removed();
        }
    }

    private static final class NoServer implements StreamObserver<Message> {
        @Override
        public void onNext(Message value) {
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
        // Updates wait here until a rule delivers them, so they can pile up and merge.
        private final Queue<Runnable> updates = new ArrayDeque<>();
        private final SnapshotSource<Object, String, String> source = new SnapshotSource<>(details -> "envoy", updates::add);
        private final Envoy envoy;
        private Snapshot intended = Snapshot.empty();
        private int elsewhere;

        @SuppressWarnings("unchecked") // each protocol's stream takes that protocol's messages
        Machine(boolean delta) {
            envoy = new Envoy(delta);
            AggregatedDiscoveryService<SnapshotUpdate<Object>, Object> ads = new AggregatedDiscoveryService<>(
                source,
                SnapshotConfigBuilder.<Object>forTypes(Cluster.class, ClusterLoadAssignment.class, Listener.class, RouteConfiguration.class),
                node -> XdsConfig.of(new Object()),
                ImmutableList.of(),
                MergingStateBacklog.factory(),
                DiscoveryServiceManagerMetrics.METRICS_DISABLED);
            StreamObserver<?> requests = delta
                ? ads.deltaAggregatedResources((StreamObserver<DeltaDiscoveryResponse>) (StreamObserver<?>) envoy)
                : ads.streamAggregatedResources((StreamObserver<DiscoveryResponse>) (StreamObserver<?>) envoy);
            envoy.connect((StreamObserver<Message>) requests);
            source.update(c -> c.view("elsewhere", "other"));
        }

        @Rule(weight = 2)
        void publish(TestCase tc) {
            Snapshot next = drawSnapshot(tc);
            List<String> view = tc.draw(sampledFrom(VIEWS), "view");
            Map<String, Snapshot> groups = split(tc, next, view);
            boolean alsoElsewhere = tc.draw(booleans(), "also elsewhere");
            source.update(c -> {
                c.view("envoy", view);
                groups.forEach((name, group) -> {
                    if (tc.draw(booleans(), "edit " + name)) {
                        c.group(name, e -> editTo(e, c.group(name), group));
                    } else {
                        c.group(name, group);
                    }
                });
                if (alsoElsewhere) {
                    changeElsewhere(c);
                }
            });
            intended = next;
        }

        @Rule
        void publishElsewhere(TestCase tc) {
            source.update(this::changeElsewhere);
        }

        /** A change to a group the Envoy's view doesn't have, whatever its list. */
        private void changeElsewhere(SnapshotSource.Change<String, String> change) {
            change.group("other", e -> e.put(cluster("a").toBuilder().setAltStatName("elsewhere-" + elsewhere++).build()));
        }

        @Rule(weight = 2)
        void deliverUpdates(TestCase tc) {
            tc.assume(!updates.isEmpty());
            updates.remove().run();
        }

        @Rule(weight = 4)
        void deliver(TestCase tc) {
            tc.assume(envoy.hasMail());
            envoy.deliver();
        }

        @Rule
        void settle(TestCase tc) {
            while (!updates.isEmpty()) {
                updates.remove().run();
            }
            envoy.settle();
            envoy.assertHolds(intended);
        }

        @Invariant(alwaysRun = true)
        void envoyOnlyServesFromConfigItHolds(TestCase tc) {
            assertThat(envoy.problems).isEmpty();
        }
    }
}
