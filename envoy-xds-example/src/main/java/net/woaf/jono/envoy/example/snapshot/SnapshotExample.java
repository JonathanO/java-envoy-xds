package net.woaf.jono.envoy.example.snapshot;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import com.google.protobuf.Duration;
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
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.woaf.jono.envoy.example.config.ClientConfig;
import net.woaf.jono.envoy.example.config.StaticClientConfigSource;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;
import net.woaf.jono.envoy.xds.snapshot.Snapshot;
import net.woaf.jono.envoy.xds.snapshot.SnapshotConfigBuilder;
import net.woaf.jono.envoy.xds.snapshot.SnapshotSource;
import net.woaf.jono.envoy.xds.snapshot.SnapshotUpdate;

/**
 * The same CLI driven mesh as XdsExample, built on envoy-xds-snapshot instead of hand-written config builders.
 *
 * <p>We just describe the config we want as a snapshot of Envoy resources. The snapshot config builders work out what
 * changed and send it in a make-before-break order: a new cluster and its endpoints reach Envoy before the route that
 * uses it, and a removed cluster goes only after the route has stopped using it.
 */
public class SnapshotExample {

    // The route config name the http connection manager in envoy-delta.yaml asks for.
    private static final String ROUTE_NAME = "all_routes";

    private static final Splitter argSplitter = Splitter.on(' ').trimResults().omitEmptyStrings();

    public static void main(String[] args) throws IOException, InterruptedException {

        // The snapshot holds what every client shares: clusters and their endpoints.
        // The route config depends on the port each Envoy listens on, so an overlay builds it per client. Overlays are
        // built once per distinct ClientConfig, so all Envoys with the same port share one.
        SnapshotSource<ClientConfig> source = new SnapshotSource<>(SnapshotExample::routesFor);

        AggregatedDiscoveryService<SnapshotUpdate<ClientConfig>, ClientConfig> ads = new AggregatedDiscoveryService<>(
            source, // The snapshot source tells every connected Envoy when the snapshot changes
            SnapshotConfigBuilder.forTypes(Cluster.class, ClusterLoadAssignment.class, RouteConfiguration.class), // One config builder per type we serve
            new StaticClientConfigSource(), // ClientConfigProviders provide per-client configuration, which the overlay uses
            ImmutableList.of(), // Our example has no event listeners, but you can use these for things like tracking connected clients
            MergingStateBacklog.factory(), // Strategy for handling a backlog of snapshot updates.
            DiscoveryServiceManagerMetrics.METRICS_DISABLED // Use the NOOP metrics factory.
        );

        Server server = ServerBuilder.forPort(6566).addService(ads).build();
        server.start();

        // Everything after this point is just providing the CLI

        mainLoop(source);
        System.out.println("Initiating graceful shutdown...");
        server.shutdown();
        server.awaitTermination(1, TimeUnit.MINUTES);
        System.out.println("Terminating.");
        server.shutdownNow();
        server.awaitTermination();
    }

    /**
     * The overlay: a route config for one client, with a virtual host per cluster in the snapshot.
     */
    private static Snapshot routesFor(Snapshot base, ClientConfig client) {
        RouteConfiguration.Builder routes = RouteConfiguration.newBuilder().setName(ROUTE_NAME);
        for (String name : base.get(Cluster.class).keySet()) {
            routes.addVirtualHosts(VirtualHost.newBuilder()
                .setName(name)
                .addDomains(name)
                .addDomains(name + ":" + client.getListenPort())
                .addRoutes(Route.newBuilder()
                    .setMatch(RouteMatch.newBuilder().setPrefix("/").build())
                    .setRoute(RouteAction.newBuilder().setCluster(name).build())
                    .build())
                .build());
        }
        return Snapshot.empty().edit().put(routes.build()).build();
    }

    private static Cluster cluster(String name) {
        return Cluster.newBuilder()
            .setName(name)
            .setType(Cluster.DiscoveryType.EDS)
            .setConnectTimeout(Duration.newBuilder().setSeconds(1).build())
            .setEdsClusterConfig(Cluster.EdsClusterConfig.newBuilder()
                .setEdsConfig(ConfigSource.newBuilder()
                    .setResourceApiVersion(ApiVersion.V3)
                    .setAds(AggregatedConfigSource.newBuilder().build())
                    .build())
                .build())
            .build();
    }

    private static LbEndpoint lbEndpoint(HostAndPort addr) {
        return LbEndpoint.newBuilder()
            .setEndpoint(Endpoint.newBuilder()
                .setAddress(Address.newBuilder()
                    .setSocketAddress(SocketAddress.newBuilder()
                        .setProtocol(SocketAddress.Protocol.TCP)
                        .setAddress(addr.getHost())
                        .setPortValue(addr.getPortOrDefault(80))
                        .build())
                    .build())
                .build())
            .build();
    }

    private static List<LbEndpoint> endpoints(Snapshot snapshot, String name) {
        ClusterLoadAssignment cla = snapshot.get(ClusterLoadAssignment.class).get(name);
        return cla == null ? List.of() : cla.getEndpoints(0).getLbEndpointsList();
    }

    private static ClusterLoadAssignment withEndpoints(String name, Iterable<LbEndpoint> endpoints) {
        return ClusterLoadAssignment.newBuilder()
            .setClusterName(name)
            .addEndpoints(LocalityLbEndpoints.newBuilder().addAllLbEndpoints(endpoints).build())
            .build();
    }

    private static HostAndPort parseEndpoint(String endpoint) {
        HostAndPort addr = HostAndPort.fromString(endpoint);
        if (!InetAddresses.isInetAddress(addr.getHost())) {
            throw new IllegalArgumentException("endpoints must be ip:port format");
        }
        return addr;
    }

    private static void cmdList(SnapshotSource<ClientConfig> source) {
        System.out.println("Services:");
        source.current().get(Cluster.class).keySet().forEach(name -> {
            System.out.print('\t');
            System.out.println(name);
        });
    }

    private static void cmdShow(SnapshotSource<ClientConfig> source, String name) {
        System.out.println("Service: " + name);
        Snapshot current = source.current();
        if (!current.get(Cluster.class).containsKey(name)) {
            System.out.println("\tNot Found");
            return;
        }
        endpoints(current, name).forEach(e -> {
            SocketAddress addr = e.getEndpoint().getAddress().getSocketAddress();
            System.out.print('\t');
            System.out.println(HostAndPort.fromParts(addr.getAddress(), addr.getPortValue()));
        });
    }

    private static void cmdAdd(SnapshotSource<ClientConfig> source, String name, String endpoint) {
        LbEndpoint added = lbEndpoint(parseEndpoint(endpoint));
        List<LbEndpoint> endpoints = endpoints(source.current(), name);
        if (endpoints.contains(added)) {
            return;
        }
        // Put the cluster and its endpoints in one edit; Envoy gets the cluster before the route that uses it.
        // Putting a resource equal to the one already there is a no-op, so re-putting the cluster costs nothing.
        source.update(e -> e.put(cluster(name)).put(withEndpoints(name, ImmutableList.<LbEndpoint>builder().addAll(endpoints).add(added).build())));
    }

    private static void cmdRemove(SnapshotSource<ClientConfig> source, String name, String endpoint) {
        LbEndpoint removed = lbEndpoint(parseEndpoint(endpoint));
        List<LbEndpoint> endpoints = endpoints(source.current(), name);
        if (!endpoints.contains(removed)) {
            return;
        }
        source.update(e -> e.put(withEndpoints(name, endpoints.stream().filter(ep -> !ep.equals(removed)).toList())));
    }

    private static void cmdRemove(SnapshotSource<ClientConfig> source, String name) {
        // Removing the cluster drops it from every client's route config too; Envoy gets the new routes first.
        source.update(e -> e.remove(Cluster.class, name).remove(ClusterLoadAssignment.class, name));
    }

    private static void mainLoop(SnapshotSource<ClientConfig> source) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()));
        System.out.println("CLI driven service mesh, snapshot edition!");
        cliIntro();
        while (true) {
            System.out.print("> ");
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            List<String> parts = argSplitter.splitToList(line);
            if (parts.isEmpty()) {
                continue;
            }
            try {
                switch (parts.get(0)) {
                    case "list" -> {
                        if (parts.size() != 1) {
                            throw new IllegalArgumentException("Incorrect argument count.");
                        }
                        cmdList(source);
                    }
                    case "show" -> {
                        if (parts.size() != 2) {
                            throw new IllegalArgumentException("Incorrect argument count.");
                        }
                        cmdShow(source, parts.get(1));
                    }
                    case "add" -> {
                        if (parts.size() != 3) {
                            throw new IllegalArgumentException("Incorrect argument count.");
                        }
                        cmdAdd(source, parts.get(1), parts.get(2));
                    }
                    case "remove" -> {
                        if (parts.size() < 2 || parts.size() > 3) {
                            throw new IllegalArgumentException("Incorrect argument count.");
                        }
                        if (parts.size() == 2) {
                            cmdRemove(source, parts.get(1));
                        } else {
                            cmdRemove(source, parts.get(1), parts.get(2));
                        }
                    }
                    case "quit" -> {
                        return;
                    }
                    case "help" -> cliIntro();
                    default -> {
                        System.out.println("Unknown command");
                        cliIntro();
                    }
                }
            } catch (IllegalArgumentException e) {
                System.out.println("Failed to parse command: " + e.getMessage());
                cliIntro();
            }
        }

    }

    private static void cliIntro() {
        System.out.println("Commands:");
        System.out.println("  list - list all service names");
        System.out.println("  show <name> - show endpoints for service by name");
        System.out.println("  add <name> <ip:port> - add a new endpoint to a service, creating it if needed");
        System.out.println("  remove <name> <ip:port> - remove an endpoint from a service");
        System.out.println("  remove <name> - remove a service");
        System.out.println("  quit - shutdown");
    }

}
