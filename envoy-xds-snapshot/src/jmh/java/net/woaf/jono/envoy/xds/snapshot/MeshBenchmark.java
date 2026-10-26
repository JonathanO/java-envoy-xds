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
 * listener and one route config, with a virtual host per service it calls, routing to that service's cluster.
 *
 * <p>Two designs. {@code perViewSnapshots}, the base case: one SnapshotSource per view, each holding one group with the
 * view's whole config, with the control plane keeping its own index of which views call which service. {@code groups}:
 * one SnapshotSource, a group per service (its cluster and endpoints) and per caller (its listener and routes), and a
 * view per caller listing its own group then those of the services it calls. Either way, clusters and endpoints are
 * built once and shared by reference.
 *
 * <p>Clients are delivered to on the publishing thread and each works out its changes for every type, as a stream
 * does. The core's own work per client (response building, serialisation) isn't included: it is the same for any
 * design that wakes the same clients.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MeshBenchmark {

    private static final List<Class<? extends Message>> TYPES =
        List.of(Listener.class, RouteConfiguration.class, Cluster.class, ClusterLoadAssignment.class);

    @Param("3000")
    int services;

    @Param("6")
    int replicas;

    /** Services each caller calls. */
    @Param("50")
    int calls;

    @Param("10")
    int endpoints;

    /** Callers of the service whose endpoints change. */
    @Param({"50", "3000"})
    int callers;

    @Param({"perViewSnapshots", "groups"})
    String design;

    /** Per view: the source a replica of it subscribes to. */
    private final List<SnapshotSource<Object>> sourceOf = new ArrayList<>();

    /** perViewSnapshots: the control plane's own bookkeeping, the views that call service 0. */
    private final List<SnapshotSource<Object>> callersOfChanged = new ArrayList<>();

    private SnapshotSource<Object> groups;

    private final ClusterLoadAssignment[] changedEndpoints = new ClusterLoadAssignment[2];

    private int version;

    private static String service(int i) {
        return "svc-" + i;
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

    private static RouteConfiguration routes(Iterable<Integer> called) {
        RouteConfiguration.Builder routes = RouteConfiguration.newBuilder().setName("mesh");
        for (int i : called) {
            routes.addVirtualHosts(VirtualHost.newBuilder().setName(service(i)).addDomains(service(i))
                .addRoutes(Route.newBuilder().setMatch(RouteMatch.newBuilder().setPrefix("/"))
                    .setRoute(RouteAction.newBuilder().setCluster(service(i)))));
        }
        return routes.build();
    }

    /** One replica: works out what it would be sent, as its stream would. */
    private static final class Client implements ClusterManagerEventListener<SnapshotUpdate<Object>> {
        private final Object details;
        private final List<SnapshotConfigBuilder<?, Object>> builders =
            TYPES.stream().<SnapshotConfigBuilder<?, Object>>map(SnapshotConfigBuilder::new).toList();
        int sent;

        Client(Object details) {
            this.details = details;
        }

        @Override
        public void onNetworkChange(SnapshotUpdate<Object> update) {
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
        Cluster[] clusters = new Cluster[services];
        ClusterLoadAssignment[] assignments = new ClusterLoadAssignment[services];
        for (int i = 0; i < services; i++) {
            clusters[i] = cluster(i);
            assignments[i] = i == 0 ? changedEndpoints[0] : endpoints(i, endpoints, 0);
        }

        groups = new SnapshotSource<>(details -> (String) details, Runnable::run);
        if (design.equals("groups")) {
            groups.update(c -> {
                for (int i = 0; i < services; i++) {
                    int s = i;
                    c.group(service(i), e -> e.put(clusters[s]).put(assignments[s]));
                }
            });
        }
        Random random = new Random(42);
        for (int v = 0; v < services; v++) {
            TreeSet<Integer> called = new TreeSet<>();
            if (v < callers) {
                called.add(0);
            }
            while (called.size() < calls) {
                called.add(1 + random.nextInt(services - 1));
            }
            String caller = "caller-" + v;
            SnapshotSource<Object> source;
            if (design.equals("groups")) {
                source = groups;
                List<String> list = new ArrayList<>();
                list.add(caller);
                called.forEach(i -> list.add(service(i)));
                source.update(c -> c.view(caller, list).group(caller, e -> e.put(Listener.newBuilder().setName("mesh").build()).put(routes(called))));
            } else {
                source = new SnapshotSource<>(details -> (String) details, Runnable::run);
                Snapshot.Editor view = Snapshot.empty().edit();
                view.put(Listener.newBuilder().setName("mesh").build());
                view.put(routes(called));
                for (int i : called) {
                    view.put(clusters[i]);
                    view.put(assignments[i]);
                }
                Snapshot whole = view.build();
                source.update(c -> c.view(caller, caller).group(caller, whole));
                if (v < callers) {
                    callersOfChanged.add(source);
                }
            }
            for (int r = 0; r < replicas; r++) {
                Client client = new Client(caller);
                client.onNetworkChange(source.subscribe(client, caller));
            }
            sourceOf.add(source);
        }
    }

    /** One service's endpoints change: perViewSnapshots edits every view that calls it, groups edits its group. */
    @Benchmark
    public void endpointChange() {
        version ^= 1;
        ClusterLoadAssignment next = changedEndpoints[version];
        if (design.equals("groups")) {
            groups.update(c -> c.group(service(0), e -> e.put(next)));
            return;
        }
        for (int v = 0; v < callersOfChanged.size(); v++) {
            String caller = "caller-" + v; // callers of service 0 are the first views
            callersOfChanged.get(v).update(c -> c.group(caller, e -> e.put(next)));
        }
    }

    /** A replica (re)connects and is sent its whole view. */
    @Benchmark
    public int connect() {
        Client client = new Client("caller-1");
        client.onNetworkChange(sourceOf.get(1).subscribe(client, "caller-1"));
        sourceOf.get(1).unsubscribe(client);
        return client.sent;
    }
}
