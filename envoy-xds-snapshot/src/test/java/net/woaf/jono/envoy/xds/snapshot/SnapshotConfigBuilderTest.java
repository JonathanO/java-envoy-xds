package net.woaf.jono.envoy.xds.snapshot;

import static net.woaf.jono.envoy.xds.snapshot.TestResources.cla;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cluster;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.route;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.NamedMessage;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.Response;
import org.junit.jupiter.api.Test;

public class SnapshotConfigBuilderTest {

    private final SnapshotSource<String> source = new SnapshotSource<>();

    @Test
    public void addingRouteClusterAndEndpointsTogetherIsAllAddOrder() {
        source.update(e -> e.put(route("all_routes")));

        SnapshotUpdate<String> update = source.update(e -> e.put(cluster("foo")).put(cla("foo", "127.0.0.5")).put(route("all_routes", "foo")));

        assertThat(added(Cluster.class, update)).containsExactly(cluster("foo"));
        assertThat(added(ClusterLoadAssignment.class, update)).containsExactly(cla("foo", "127.0.0.5"));
        assertThat(added(RouteConfiguration.class, update)).containsExactly(route("all_routes", "foo"));
        assertThat(removed(Cluster.class, update)).isEmpty();
        assertThat(removed(RouteConfiguration.class, update)).isEmpty();
    }

    @Test
    public void removingRouteAndClusterUpdatesRouteThenRemovesCluster() {
        source.update(e -> e.put(cluster("foo")).put(cla("foo", "127.0.0.5")).put(route("all_routes", "foo")));

        SnapshotUpdate<String> update = source.update(e -> e.put(route("all_routes")).remove(Cluster.class, "foo").remove(ClusterLoadAssignment.class, "foo"));

        assertThat(added(RouteConfiguration.class, update)).containsExactly(route("all_routes"));
        assertThat(added(Cluster.class, update)).isEmpty();
        assertThat(removed(Cluster.class, update)).containsExactly("foo");
        assertThat(removed(ClusterLoadAssignment.class, update)).containsExactly("foo");
        // Mid-change (add order) the cluster still exists; once removes are applied it's gone.
        assertThat(builder(Cluster.class).getResourcesAddOrder(update, SnapshotConfigBuilderTest::all, "c").resources()).extracting(NamedMessage::name).containsExactly("foo");
        assertThat(builder(Cluster.class).getResourcesRemoveOrder(update, SnapshotConfigBuilderTest::all, "c").resources()).isEmpty();
    }

    @Test
    public void mergedUpdatesCancelOut() {
        source.update(e -> e.put(cluster("foo")));
        SnapshotUpdate<String> add = source.update(e -> e.put(cluster("bar")));
        SnapshotUpdate<String> remove = source.update(e -> e.remove(Cluster.class, "bar"));

        SnapshotUpdate<String> merged = add.merge(remove);

        assertThat(added(Cluster.class, merged)).isEmpty();
        assertThat(removed(Cluster.class, merged)).isEmpty();
    }

    @Test
    public void unrelatedSnapshotIsComparedInFull() {
        source.update(e -> e.put(cluster("foo")).put(cluster("same")));

        SnapshotUpdate<String> update = source.publish(Snapshot.empty().edit().put(cluster("bar")).put(cluster("same")).build());

        assertThat(added(Cluster.class, update)).containsExactly(cluster("bar"));
        assertThat(removed(Cluster.class, update)).containsExactly("foo");
    }

    @Test
    public void unsubscribedNamesAreIgnored() {
        SnapshotUpdate<String> update = source.update(e -> e.put(cluster("foo")).put(cluster("bar")));

        assertThat(builder(Cluster.class).addOrder(update, "foo"::equals, "c").addAndUpdates()).extracting(NamedMessage::name).containsExactly("foo");
    }

    @Test
    public void overlayIsPerClientAndComputedOncePerDetails() {
        AtomicInteger overlayCalls = new AtomicInteger();
        SnapshotSource<String> perClient = new SnapshotSource<>((base, client) -> {
            overlayCalls.incrementAndGet();
            return Snapshot.empty().edit().put(route("all_routes", base.get(Cluster.class).keySet().stream().map(c -> client + "-" + c).toArray(String[]::new))).build();
        });
        perClient.update(e -> e.put(route("all_routes")));

        SnapshotUpdate<String> update = perClient.update(e -> e.put(cluster("foo")));

        for (int i = 0; i < 3; i++) {
            assertThat(added(RouteConfiguration.class, update, "a")).containsExactly(route("all_routes", "a-foo"));
            assertThat(added(RouteConfiguration.class, update, "b")).containsExactly(route("all_routes", "b-foo"));
        }
        assertThat(overlayCalls).hasValue(4); // previous and current snapshot, for each of "a" and "b"
    }

    @Test
    public void unchangedOverlayIsNotResent() {
        SnapshotSource<String> perClient = new SnapshotSource<>((base, client) -> Snapshot.empty().edit().put(route("all_routes", client)).build());
        perClient.update(e -> e.put(cluster("a")));

        SnapshotUpdate<String> update = perClient.update(e -> e.put(cluster("b")));

        assertThat(added(RouteConfiguration.class, update, "a")).isEmpty();
        assertThat(added(Cluster.class, update, "a")).containsExactly(cluster("b"));
    }

    private static boolean all(String unused) {
        return true;
    }

    private static <R extends Message> SnapshotConfigBuilder<R, String> builder(Class<R> type) {
        return new SnapshotConfigBuilder<>(type);
    }

    private static <R extends Message> List<R> added(Class<R> type, SnapshotUpdate<String> update) {
        return added(type, update, "c");
    }

    private static <R extends Message> List<R> added(Class<R> type, SnapshotUpdate<String> update, String client) {
        return builder(type).addOrder(update, SnapshotConfigBuilderTest::all, client).addAndUpdates().stream().map(NamedMessage::message).collect(Collectors.toList());
    }

    private static <R extends Message> List<String> removed(Class<R> type, SnapshotUpdate<String> update) {
        Response<R> response = builder(type).removeOrder(update, SnapshotConfigBuilderTest::all, "c");
        return response.removes();
    }
}
