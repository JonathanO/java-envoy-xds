package net.woaf.jono.envoy.testing.assertions;

import com.google.protobuf.Any;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Filter;
import io.envoyproxy.envoy.config.listener.v3.FilterChain;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.Rds;

final class TestResources {

    private TestResources() {
    }

    static Cluster cluster(String name) {
        return Cluster.newBuilder().setName(name).setType(Cluster.DiscoveryType.EDS).build();
    }

    static ClusterLoadAssignment endpoints(String cluster) {
        return ClusterLoadAssignment.newBuilder().setClusterName(cluster).build();
    }

    static RouteConfiguration routeTo(String cluster) {
        return RouteConfiguration.newBuilder().setName("routes").addVirtualHosts(VirtualHost.newBuilder().setName("foo")
            .addRoutes(Route.newBuilder().setRoute(RouteAction.newBuilder().setCluster(cluster)))).build();
    }

    static Listener listenerUsing(String routeConfig) {
        HttpConnectionManager hcm = HttpConnectionManager.newBuilder().setRds(Rds.newBuilder().setRouteConfigName(routeConfig)).build();
        return Listener.newBuilder().setName("listener").addFilterChains(FilterChain.newBuilder()
            .addFilters(Filter.newBuilder().setName("envoy.filters.network.http_connection_manager").setTypedConfig(Any.pack(hcm)))).build();
    }
}
