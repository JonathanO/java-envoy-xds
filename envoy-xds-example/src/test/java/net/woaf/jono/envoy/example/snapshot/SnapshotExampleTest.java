package net.woaf.jono.envoy.example.snapshot;

import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import net.woaf.jono.envoy.example.config.ClientConfig;
import net.woaf.jono.envoy.xds.snapshot.SnapshotSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotExampleTest {

    @Test
    public void commandsKeepTheClustersAndEachPortsRoutesInStep() {
        SnapshotSource<ClientConfig, String, Long> source = new SnapshotSource<>(ClientConfig::listenPort);

        SnapshotExample.addPort(source, 8080);
        SnapshotExample.cmdAdd(source, "a", "10.0.0.1:80");

        assertThat(source.view(8080L)).containsExactly("routes-8080", "clusters");
        assertThat(source.group("clusters").get(Cluster.class)).containsOnlyKeys("a");
        assertThat(source.group("clusters").get(ClusterLoadAssignment.class).get("a").getEndpoints(0).getLbEndpointsCount()).isEqualTo(1);
        RouteConfiguration routes = source.group("routes-8080").get(RouteConfiguration.class).get("all_routes");
        assertThat(routes.getVirtualHostsList()).singleElement().satisfies(host -> assertThat(host.getDomainsList()).containsExactly("a", "a:8080"));

        SnapshotExample.cmdRemove(source, "a");

        assertThat(source.group("clusters").get(Cluster.class)).isEmpty();
        assertThat(source.group("clusters").get(ClusterLoadAssignment.class)).isEmpty();
        assertThat(source.group("routes-8080").get(RouteConfiguration.class).get("all_routes").getVirtualHostsList()).isEmpty();
    }
}
