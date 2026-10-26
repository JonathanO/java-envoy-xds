package net.woaf.jono.envoy.testing.assertions;

import static net.woaf.jono.envoy.testing.configdump.EnvoyAdminClient.sneakyUnpack;

import com.google.protobuf.Any;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.WeightedCluster.ClusterWeight;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.woaf.jono.envoy.testing.XdsTypes;

/**
 * What Envoy has ACKed so far, to check each response only relies on config Envoy already has (make before break).
 */
final class AckedState {

    private final Set<String> clusters = new HashSet<>();
    private final Map<String, String> edsClusters = new HashMap<>(); // the name their endpoints are under, by cluster
    private final Set<String> endpoints = new HashSet<>();
    private final Map<String, Set<String>> routes = new HashMap<>();
    private final Map<String, Set<String>> listeners = new HashMap<>(); // route config names used, by listener
    private final List<String> problems = new ArrayList<>();

    Set<String> clusters() {
        return clusters;
    }

    Set<String> listeners() {
        return listeners.keySet();
    }

    List<String> problems() {
        return problems;
    }

    /**
     * Check a response before Envoy sees it.
     * @param typeUrl response type
     * @param resources resources added or updated
     * @param removed resource names removed
     */
    void checkResponse(String typeUrl, Collection<Any> resources, Collection<String> removed) {
        if (typeUrl.equals(XdsTypes.RDS)) {
            for (Any any : resources) {
                RouteConfiguration route = sneakyUnpack(any, RouteConfiguration.class);
                for (String cluster : referencedClusters(route)) {
                    String edsName = edsClusters.get(cluster);
                    if (!clusters.contains(cluster)) {
                        problems.add("Route <" + route.getName() + "> sent using cluster <" + cluster + "> before Envoy ACKed the cluster");
                    } else if (edsName != null && !endpoints.contains(edsName)) {
                        problems.add("Route <" + route.getName() + "> sent using cluster <" + cluster + "> before Envoy ACKed its endpoints");
                    }
                }
            }
            for (String route : removed) {
                listeners.forEach((name, used) -> {
                    if (used.contains(route)) {
                        problems.add("Route config <" + route + "> removed while Envoy's ACKed listener <" + name + "> still uses it");
                    }
                });
            }
        } else if (typeUrl.equals(XdsTypes.CDS)) {
            for (String cluster : removed) {
                routes.forEach((name, used) -> {
                    if (used.contains(cluster)) {
                        problems.add("Cluster <" + cluster + "> removed while Envoy's ACKed route <" + name + "> still uses it");
                    }
                });
            }
        }
    }

    /**
     * Record a response Envoy has ACKed.
     * @param typeUrl response type
     * @param resources resources added or updated
     * @param removed resource names removed
     */
    void acked(String typeUrl, Collection<Any> resources, Collection<String> removed) {
        if (typeUrl.equals(XdsTypes.CDS)) {
            for (Any any : resources) {
                Cluster cluster = sneakyUnpack(any, Cluster.class);
                clusters.add(cluster.getName());
                if (cluster.getType() == Cluster.DiscoveryType.EDS) {
                    String serviceName = cluster.getEdsClusterConfig().getServiceName();
                    edsClusters.put(cluster.getName(), serviceName.isEmpty() ? cluster.getName() : serviceName);
                } else {
                    edsClusters.remove(cluster.getName());
                }
            }
            clusters.removeAll(removed);
            edsClusters.keySet().removeAll(removed);
        } else if (typeUrl.equals(XdsTypes.EDS)) {
            resources.forEach(any -> endpoints.add(sneakyUnpack(any, ClusterLoadAssignment.class).getClusterName()));
            endpoints.removeAll(removed);
        } else if (typeUrl.equals(XdsTypes.RDS)) {
            resources.forEach(any -> {
                RouteConfiguration route = sneakyUnpack(any, RouteConfiguration.class);
                routes.put(route.getName(), referencedClusters(route));
            });
            removed.forEach(routes::remove);
        } else if (typeUrl.equals(XdsTypes.LDS)) {
            resources.forEach(any -> {
                Listener listener = sneakyUnpack(any, Listener.class);
                listeners.put(listener.getName(), routeConfigsUsed(listener));
            });
            for (String gone : removed) {
                // A removed listener drains for a while and keeps its route configs, but serves no new requests: route
                // configs only it used are now unused.
                Set<String> used = listeners.remove(gone);
                if (used != null) {
                    used.stream().filter(r -> listeners.values().stream().noneMatch(l -> l.contains(r))).forEach(routes::remove);
                }
            }
        }
    }

    /** Route configs a listener's HTTP connection managers get by RDS. */
    private static Set<String> routeConfigsUsed(Listener listener) {
        Set<String> used = new HashSet<>();
        listener.getFilterChainsList().forEach(chain -> chain.getFiltersList().forEach(filter -> {
            if (filter.getTypedConfig().is(HttpConnectionManager.class)) {
                String name = sneakyUnpack(filter.getTypedConfig(), HttpConnectionManager.class).getRds().getRouteConfigName();
                if (!name.isEmpty()) {
                    used.add(name);
                }
            }
        }));
        return used;
    }

    private static Set<String> referencedClusters(RouteConfiguration route) {
        Set<String> used = new HashSet<>();
        route.getVirtualHostsList().forEach(vh -> vh.getRoutesList().forEach(r -> {
            RouteAction action = r.getRoute();
            if (!action.getCluster().isEmpty()) {
                used.add(action.getCluster());
            }
            action.getWeightedClusters().getClustersList().stream().map(ClusterWeight::getName).forEach(used::add);
        }));
        return used;
    }
}
