package net.woaf.jono.envoy.xds.snapshot;

import static net.woaf.jono.envoy.xds.snapshot.TestResources.cla;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cluster;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.route;
import static org.assertj.core.api.Assertions.assertThat;

import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import org.junit.jupiter.api.Test;

public class SnapshotTest {

    private final Snapshot base = Snapshot.empty().edit().put(cluster("foo")).put(route("all_routes", "foo")).build();

    @Test
    public void editSharesUntouchedTypesAndRecordsChangedNames() {
        Snapshot next = base.edit().put(cluster("bar")).build();

        assertThat(next.get(Cluster.class)).containsOnlyKeys("foo", "bar");
        assertThat(next.get(RouteConfiguration.class)).isSameAs(base.get(RouteConfiguration.class));
        assertThat(next.touched()).containsOnlyKeys(Cluster.class);
        assertThat(next.touched().get(Cluster.class)).containsExactly("bar");
        assertThat(base.get(Cluster.class)).containsOnlyKeys("foo");
    }

    @Test
    public void noOpEditsTouchNothing() {
        Snapshot next = base.edit().put(cluster("foo")).remove(Cluster.class, "missing").build();

        assertThat(next.touched()).isEmpty();
    }

    @Test
    public void removeIsRecorded() {
        Snapshot next = base.edit().remove(Cluster.class, "foo").build();

        assertThat(next.get(Cluster.class)).isEmpty();
        assertThat(next.touched().get(Cluster.class)).containsExactly("foo");
    }

    @Test
    public void endpointsAreNamedByClusterName() {
        assertThat(Snapshot.empty().edit().put(cla("foo", "127.0.0.5")).build().get(ClusterLoadAssignment.class)).containsOnlyKeys("foo");
    }
}
