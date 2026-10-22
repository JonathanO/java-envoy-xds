package net.woaf.jono.envoy.xds.snapshot;

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

final class TestResources {

    private TestResources() {
    }

    static Cluster cluster(String name) {
        return Cluster.newBuilder()
            .setName(name)
            .setType(Cluster.DiscoveryType.EDS)
            .setConnectTimeout(Duration.newBuilder().setSeconds(1).build())
            .setEdsClusterConfig(Cluster.EdsClusterConfig.newBuilder()
                .setEdsConfig(ConfigSource.newBuilder()
                    .setResourceApiVersion(ApiVersion.V3)
                    .setAds(AggregatedConfigSource.getDefaultInstance())))
            .build();
    }

    static ClusterLoadAssignment cla(String cluster, String... ips) {
        LocalityLbEndpoints.Builder endpoints = LocalityLbEndpoints.newBuilder();
        for (String ip : ips) {
            endpoints.addLbEndpoints(LbEndpoint.newBuilder().setEndpoint(Endpoint.newBuilder()
                .setAddress(Address.newBuilder().setSocketAddress(SocketAddress.newBuilder().setAddress(ip).setPortValue(80)))));
        }
        return ClusterLoadAssignment.newBuilder().setClusterName(cluster).addEndpoints(endpoints).build();
    }

    /** A route config with one virtual host routing "/" + cluster to each cluster. */
    static RouteConfiguration route(String name, String... clusters) {
        VirtualHost.Builder vhost = VirtualHost.newBuilder().setName("default").addDomains("*");
        for (String cluster : clusters) {
            vhost.addRoutes(Route.newBuilder()
                .setMatch(RouteMatch.newBuilder().setPrefix("/" + cluster))
                .setRoute(RouteAction.newBuilder().setCluster(cluster)));
        }
        return RouteConfiguration.newBuilder().setName(name).addVirtualHosts(vhost).build();
    }
}
