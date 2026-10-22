package net.woaf.jono.envoy.e2e.assertions;

import static net.woaf.jono.envoy.e2e.configdump.EnvoyAdminClient.sneakyUnpack;

import com.google.protobuf.Any;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.WeightedCluster.ClusterWeight;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.woaf.jono.envoy.xds.TypeUrl;

/**
 * What Envoy has ACKed so far, to check each response only relies on config Envoy already has (make before break).
 */
final class AckedState {

    private final Set<String> clusters = new HashSet<>();
    private final Set<String> edsClusters = new HashSet<>();
    private final Set<String> endpoints = new HashSet<>();
    private final Map<String, Set<String>> routes = new HashMap<>();
    private final List<String> problems = new ArrayList<>();

    Set<String> clusters() {
        return clusters;
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
        if (typeUrl.equals(TypeUrl.RDS.getTypeUrl())) {
            for (Any any : resources) {
                RouteConfiguration route = sneakyUnpack(any, RouteConfiguration.class);
                for (String cluster : referencedClusters(route)) {
                    if (!clusters.contains(cluster)) {
                        problems.add("Route <" + route.getName() + "> sent using cluster <" + cluster + "> before Envoy ACKed the cluster");
                    } else if (edsClusters.contains(cluster) && !endpoints.contains(cluster)) {
                        problems.add("Route <" + route.getName() + "> sent using cluster <" + cluster + "> before Envoy ACKed its endpoints");
                    }
                }
            }
        } else if (typeUrl.equals(TypeUrl.CDS.getTypeUrl())) {
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
        if (typeUrl.equals(TypeUrl.CDS.getTypeUrl())) {
            for (Any any : resources) {
                Cluster cluster = sneakyUnpack(any, Cluster.class);
                clusters.add(cluster.getName());
                if (cluster.getType() == Cluster.DiscoveryType.EDS) {
                    edsClusters.add(cluster.getName());
                } else {
                    edsClusters.remove(cluster.getName());
                }
            }
            clusters.removeAll(removed);
            edsClusters.removeAll(removed);
        } else if (typeUrl.equals(TypeUrl.EDS.getTypeUrl())) {
            resources.forEach(any -> endpoints.add(sneakyUnpack(any, ClusterLoadAssignment.class).getClusterName()));
            endpoints.removeAll(removed);
        } else if (typeUrl.equals(TypeUrl.RDS.getTypeUrl())) {
            resources.forEach(any -> {
                RouteConfiguration route = sneakyUnpack(any, RouteConfiguration.class);
                routes.put(route.getName(), referencedClusters(route));
            });
            removed.forEach(routes::remove);
        }
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
