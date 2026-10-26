package net.woaf.jono.envoy.xds.snapshot;

import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Address;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.endpoint.v3.Endpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LbEndpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LocalityLbEndpoints;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.RouteMatch;
import io.envoyproxy.envoy.config.route.v3.ScopedRouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * A service mesh: each calling service has a view of the services it calls, shared by its replicas. Each Envoy has one
 * listener, and a virtual host per service it calls, routing to that service's cluster.
 *
 * <p>Three designs:
 * <ul>
 *   <li>{@code perViewSnapshots}, the base case: one SnapshotSource per view, each holding one group with the view's
 *   whole config, with the control plane keeping its own index of which views call which service.</li>
 *   <li>{@code groups}: one SnapshotSource, a group per service (its cluster and endpoints) and per caller (its listener
 *   and a route config with a virtual host per service it calls), and a view per caller listing its own group then
 *   those of the services it calls.</li>
 *   <li>{@code groupsSrds}: as groups, but with scoped routes: each service's group also has its scope and a route
 *   config of its own, so callers have no route config to build.</li>
 * </ul>
 * Clusters, endpoints and virtual hosts are built once and shared by reference.
 *
 * <p>Clients are delivered to on the publishing thread and each works out its changes for every type, as a stream
 * does. The core's own work per client (response building, serialisation) isn't included: it is the same for any
 * design that wakes the same clients and sends the same resources.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MeshBenchmark {

    private static final List<Class<? extends Message>> TYPES = List.of(Listener.class, ScopedRouteConfiguration.class,
        RouteConfiguration.class, Cluster.class, ClusterLoadAssignment.class);

    @Param("3000")
    int services;

    @Param("6")
    int replicas;

    /** Services each caller calls. */
    @Param("50")
    int calls;

    @Param("10")
    int endpoints;

    /** Callers of the service that changes. */
    @Param({"50", "3000"})
    int callers;

    @Param({"perViewSnapshots", "groups", "groupsSrds"})
    String design;

    /** Per view: the source a replica of it subscribes to. */
    private final List<SnapshotSource<Object, String, String>> sourceOf = new ArrayList<>();

    /** Per view: the services it calls. */
    private final List<TreeSet<Integer>> called = new ArrayList<>();

    /** The control plane's own bookkeeping: the views that call each service. */
    private final List<List<Integer>> callersOf = new ArrayList<>();

    /** Each service's virtual host as currently published. */
    private VirtualHost[] vhosts;

    private SnapshotSource<Object, String, String> groups;

    private final ClusterLoadAssignment[] changedEndpoints = new ClusterLoadAssignment[2];

    private final VirtualHost[] changedVhost = new VirtualHost[2];

    private int version;

    private int routeVersion;

    private static String service(int i) {
        return "svc-" + i;
    }

    private static String caller(int v) {
        return "caller-" + v;
    }

    private static Cluster cluster(int i) {
        return Cluster.newBuilder()
            .setName(service(i))
            .setType(Cluster.DiscoveryType.EDS)
            .setConnectTimeout(com.google.protobuf.Duration.newBuilder().setSeconds(1))
            .build();
    }

    private static ClusterLoadAssignment endpoints(int i, int count, int version) {
        LocalityLbEndpoints.Builder locality = LocalityLbEndpoints.newBuilder();
        for (int e = 0; e < count; e++) {
            locality.addLbEndpoints(LbEndpoint.newBuilder().setEndpoint(Endpoint.newBuilder().setAddress(Address.newBuilder()
                .setSocketAddress(SocketAddress.newBuilder().setAddress("10." + (i / 250) + "." + (i % 250) + "." + (e + version))
                    .setPortValue(8080)))));
        }
        return ClusterLoadAssignment.newBuilder().setClusterName(service(i)).addEndpoints(locality).build();
    }

    /** A service's virtual host; the version changes its route's timeout. */
    private static VirtualHost vhost(int i, int version) {
        return VirtualHost.newBuilder().setName(service(i)).addDomains(service(i))
            .addRoutes(Route.newBuilder().setMatch(RouteMatch.newBuilder().setPrefix("/"))
                .setRoute(RouteAction.newBuilder().setCluster(service(i))
                    .setTimeout(com.google.protobuf.Duration.newBuilder().setSeconds(15 + version))))
            .build();
    }

    private RouteConfiguration routes(Iterable<Integer> calls) {
        RouteConfiguration.Builder routes = RouteConfiguration.newBuilder().setName("mesh");
        for (int i : calls) {
            routes.addVirtualHosts(vhosts[i]);
        }
        return routes.build();
    }

    /** groupsSrds: a service's scope, keyed by its host name, and the route config it names. */
    private Snapshot.Editor scoped(Snapshot.Editor group, int i) {
        return group
            .put(ScopedRouteConfiguration.newBuilder().setName(service(i)).setRouteConfigurationName("rc-" + service(i))
                .setKey(ScopedRouteConfiguration.Key.newBuilder().addFragments(
                    ScopedRouteConfiguration.Key.Fragment.newBuilder().setStringKey(service(i))))
                .build())
            .put(RouteConfiguration.newBuilder().setName("rc-" + service(i)).addVirtualHosts(vhosts[i]).build());
    }

    /** One replica: works out what it would be sent, as its stream would. */
    private static final class Client implements ClusterManagerEventListener<SnapshotUpdate<Object>> {
        private final Object details;
        private final List<SnapshotConfigBuilder<?, Object>> builders =
            TYPES.stream().<SnapshotConfigBuilder<?, Object>>map(SnapshotConfigBuilder::new).toList();
        int sent;
        /** The core keeps its client's last update, and so whatever that update holds. */
        SnapshotUpdate<Object> last;

        Client(Object details) {
            this.details = details;
        }

        @Override
        public void onNetworkChange(SnapshotUpdate<Object> update) {
            last = update;
            for (SnapshotConfigBuilder<?, Object> builder : builders) {
                sent += builder.addOrder(update, name -> true, details).addAndUpdates().size();
                sent += builder.removeOrder(update, name -> true, details).removes().size();
            }
        }
    }

    @Setup
    public void setUp() {
        changedEndpoints[0] = endpoints(0, endpoints, 0);
        changedEndpoints[1] = endpoints(0, endpoints, 1);
        changedVhost[0] = vhost(0, 0);
        changedVhost[1] = vhost(0, 1);
        Cluster[] clusters = new Cluster[services];
        ClusterLoadAssignment[] assignments = new ClusterLoadAssignment[services];
        vhosts = new VirtualHost[services];
        for (int i = 0; i < services; i++) {
            clusters[i] = cluster(i);
            assignments[i] = i == 0 ? changedEndpoints[0] : endpoints(i, endpoints, 0);
            vhosts[i] = i == 0 ? changedVhost[0] : vhost(i, 0);
            callersOf.add(new ArrayList<>());
        }
        boolean srds = design.equals("groupsSrds");
        boolean shared = !design.equals("perViewSnapshots");

        groups = new SnapshotSource<>(details -> (String) details, Runnable::run);
        if (shared) {
            groups.update(c -> {
                for (int i = 0; i < services; i++) {
                    int s = i;
                    c.group(service(i), e -> {
                        e.put(clusters[s]).put(assignments[s]);
                        return srds ? scoped(e, s) : e;
                    });
                }
            });
        }
        Random random = new Random(42);
        for (int v = 0; v < services; v++) {
            TreeSet<Integer> calls = new TreeSet<>();
            if (v < callers) {
                calls.add(0);
            }
            while (calls.size() < this.calls) {
                calls.add(1 + random.nextInt(services - 1));
            }
            called.add(calls);
            String caller = caller(v);
            SnapshotSource<Object, String, String> source;
            if (shared) {
                source = groups;
                List<String> list = new ArrayList<>();
                list.add(caller);
                calls.forEach(i -> list.add(service(i)));
                source.update(c -> c.view(caller, list).group(caller, e -> {
                    e.put(Listener.newBuilder().setName("mesh").build());
                    return srds ? e : e.put(routes(calls));
                }));
            } else {
                source = new SnapshotSource<>(details -> (String) details, Runnable::run);
                Snapshot.Editor view = Snapshot.empty().edit();
                view.put(Listener.newBuilder().setName("mesh").build());
                view.put(routes(calls));
                for (int i : calls) {
                    view.put(clusters[i]);
                    view.put(assignments[i]);
                }
                Snapshot whole = view.build();
                source.update(c -> c.view(caller, caller).group(caller, whole));
            }
            for (int i : calls) {
                callersOf.get(i).add(v);
            }
            for (int r = 0; r < replicas; r++) {
                Client client = new Client(caller);
                client.onNetworkChange(source.subscribe(client, caller));
            }
            sourceOf.add(source);
        }
    }

    /** One service's endpoints change: perViewSnapshots edits every view that calls it, the others its group. */
    @Benchmark
    public void endpointChange() {
        version ^= 1;
        change(0, changedEndpoints[version]);
    }

    void change(int service, ClusterLoadAssignment next) {
        if (!design.equals("perViewSnapshots")) {
            groups.update(c -> c.group(service(service), e -> e.put(next)));
            return;
        }
        for (int v : callersOf.get(service)) {
            sourceOf.get(v).update(c -> c.group(caller(v), e -> e.put(next)));
        }
    }

    /**
     * One service's routing changes (its route timeout): perViewSnapshots and groups rebuild the route config of every
     * view that calls it, groupsSrds edits the service's own route config.
     */
    @Benchmark
    public void routeChange() {
        routeVersion ^= 1;
        vhosts[0] = changedVhost[routeVersion];
        switch (design) {
            case "groupsSrds" -> groups.update(c -> c.group(service(0), e -> scoped(e, 0)));
            case "groups" -> groups.update(c -> callersOf.get(0).forEach(v -> c.group(caller(v), e -> e.put(routes(called.get(v))))));
            default -> callersOf.get(0).forEach(v -> sourceOf.get(v).update(c -> c.group(caller(v), e -> e.put(routes(called.get(v))))));
        }
    }

    /** Every service's endpoints change once, so clients hold updates from many versions. */
    void churnEveryService() {
        for (int i = 0; i < services; i++) {
            change(i, endpoints(i, endpoints, 2));
        }
    }

    /** A replica (re)connects and is sent its whole view. */
    @Benchmark
    public int connect() {
        Client client = new Client(caller(1));
        client.onNetworkChange(sourceOf.get(1).subscribe(client, caller(1)));
        sourceOf.get(1).unsubscribe(client);
        return client.last == null ? -1 : client.sent;
    }
}
