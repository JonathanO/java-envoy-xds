package net.woaf.jono.envoy.xds.snapshot;

import static net.woaf.jono.envoy.xds.snapshot.TestResources.all;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cla;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cluster;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.route;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.NamedMessage;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.Response;
import org.junit.jupiter.api.Test;

public class SnapshotConfigBuilderTest {

    private final SnapshotSource<String> source = TestResources.oneGroup();

    @Test
    public void addingRouteClusterAndEndpointsTogetherIsAllAddOrder() {
        source.update(all(e -> e.put(route("all_routes"))));

        SnapshotUpdate<String> update = source.update(all(e -> e.put(cluster("foo")).put(cla("foo", "127.0.0.5")).put(route("all_routes", "foo"))));

        assertThat(added(Cluster.class, update)).containsExactly(cluster("foo"));
        assertThat(added(ClusterLoadAssignment.class, update)).containsExactly(cla("foo", "127.0.0.5"));
        assertThat(added(RouteConfiguration.class, update)).containsExactly(route("all_routes", "foo"));
        assertThat(removed(Cluster.class, update)).isEmpty();
        assertThat(removed(RouteConfiguration.class, update)).isEmpty();
    }

    @Test
    public void removingRouteAndClusterUpdatesRouteThenRemovesCluster() {
        source.update(all(e -> e.put(cluster("foo")).put(cla("foo", "127.0.0.5")).put(route("all_routes", "foo"))));

        SnapshotUpdate<String> update = source.update(all(e -> e.put(route("all_routes")).remove(Cluster.class, "foo").remove(ClusterLoadAssignment.class, "foo")));

        assertThat(added(RouteConfiguration.class, update)).containsExactly(route("all_routes"));
        assertThat(added(Cluster.class, update)).isEmpty();
        assertThat(removed(Cluster.class, update)).containsExactly("foo");
        assertThat(removed(ClusterLoadAssignment.class, update)).containsExactly("foo");
        // Mid-change (add order) the cluster still exists; once removes are applied it's gone.
        assertThat(builder(Cluster.class).getResourcesAddOrder(update, SnapshotConfigBuilderTest::everyName, "c").resources()).extracting(NamedMessage::name).containsExactly("foo");
        assertThat(builder(Cluster.class).getResourcesRemoveOrder(update, SnapshotConfigBuilderTest::everyName, "c").resources()).isEmpty();
    }

    @Test
    public void mergedUpdatesCancelOut() {
        source.update(all(e -> e.put(cluster("foo"))));
        SnapshotUpdate<String> add = source.update(all(e -> e.put(cluster("bar"))));
        SnapshotUpdate<String> remove = source.update(all(e -> e.remove(Cluster.class, "bar")));

        SnapshotUpdate<String> merged = add.merge(remove);

        assertThat(added(Cluster.class, merged)).isEmpty();
        assertThat(removed(Cluster.class, merged)).isEmpty();
    }

    @Test
    public void replacedGroupIsComparedInFull() {
        source.update(all(e -> e.put(cluster("foo")).put(cluster("same"))));

        SnapshotUpdate<String> update = source.update(c -> c.group("all", Snapshot.empty().edit().put(cluster("bar")).put(cluster("same")).build()));

        assertThat(added(Cluster.class, update)).containsExactly(cluster("bar"));
        assertThat(removed(Cluster.class, update)).containsExactly("foo");
    }

    @Test
    public void unsubscribedNamesAreIgnored() {
        SnapshotUpdate<String> update = source.update(all(e -> e.put(cluster("foo")).put(cluster("bar"))));

        assertThat(builder(Cluster.class).addOrder(update, "foo"::equals, "c").addAndUpdates()).extracting(NamedMessage::name).containsExactly("foo");
    }

    @Test
    public void firstGroupInTheViewWins() {
        SnapshotSource<String> views = viewPerClient();
        views.update(c -> c.view("c", "own", "shared")
            .group("shared", e -> e.put(cluster("foo")))
            .group("own", e -> e.put(slow("foo"))));

        SnapshotUpdate<String> sharedChange = views.update(c -> c.group("shared", e -> e.put(cluster("foo").toBuilder().setAltStatName("new").build())));
        assertThat(added(Cluster.class, sharedChange)).isEmpty();

        SnapshotUpdate<String> overrideRemoved = views.update(c -> c.group("own", e -> e.remove(Cluster.class, "foo")));
        assertThat(added(Cluster.class, overrideRemoved)).containsExactly(cluster("foo").toBuilder().setAltStatName("new").build());
        assertThat(removed(Cluster.class, overrideRemoved)).isEmpty();
    }

    @Test
    public void changingAViewsGroupsComparesEverythingInIt() {
        SnapshotSource<String> views = viewPerClient();
        views.update(c -> c.view("c", "a").group("a", e -> e.put(cluster("foo"))).group("b", e -> e.put(cluster("bar")).put(cluster("foo"))));

        SnapshotUpdate<String> update = views.update(c -> c.view("c", "b"));

        assertThat(added(Cluster.class, update)).containsExactly(cluster("bar"));
        assertThat(removed(Cluster.class, update)).isEmpty();
        assertThat(removed(Cluster.class, views.update(c -> c.removeView("c")))).containsExactlyInAnyOrder("foo", "bar");
    }

    @Test
    public void onlyClientsWhoseViewHasTheGroupAreWoken() {
        SnapshotSource<String> views = new SnapshotSource<>(Function.identity(), Runnable::run);
        views.update(c -> c.view("a", "own-a", "shared").view("b", "own-b"));
        List<String> woken = new ArrayList<>();
        assertThat(views.subscribe(update -> woken.add("a"), "a")).isNotNull();
        assertThat(views.subscribe(update -> woken.add("b"), "b")).isNotNull();

        views.update(c -> c.group("shared", e -> e.put(cluster("foo"))));
        views.update(c -> c.group("own-b", e -> e.put(cluster("foo"))));
        views.update(c -> c.group("nobody", e -> e.put(cluster("foo"))));
        views.update(c -> c.group("shared", e -> e.put(cluster("foo")))); // no change

        assertThat(woken).containsExactly("a", "b");
    }

    @Test
    public void updatesMergeAcrossVersionsTheClientSkipped() {
        SnapshotSource<String> views = viewPerClient();
        views.update(c -> c.view("c", "mine").view("other", "theirs"));
        SnapshotUpdate<String> first = views.update(c -> c.group("mine", e -> e.put(cluster("foo"))));
        views.update(c -> c.group("theirs", e -> e.put(cluster("bar"))));
        SnapshotUpdate<String> second = views.update(c -> c.group("mine", e -> e.put(cluster("baz"))));

        SnapshotUpdate<String> merged = first.merge(second);

        assertThat(added(Cluster.class, merged)).containsExactlyInAnyOrder(cluster("foo"), cluster("baz"));
        assertThatThrownBy(() -> second.merge(first)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientWithNoViewSeesNothingUntilItIsAdded() {
        SnapshotSource<String> views = viewPerClient();
        views.update(c -> c.group("g", e -> e.put(cluster("foo"))));

        SnapshotUpdate<String> connected = views.subscribe(update -> { }, "c");
        assertThat(added(Cluster.class, connected)).isEmpty();
        assertThat(added(Cluster.class, views.update(c -> c.view("c", "g")))).containsExactly(cluster("foo"));
    }

    @Test
    public void subscribingNeedsTheClientsDetails() {
        assertThatThrownBy(() -> source.subscribe(update -> { })).isInstanceOf(UnsupportedOperationException.class);
    }

    private static SnapshotSource<String> viewPerClient() {
        return new SnapshotSource<>(Function.identity());
    }

    private static Cluster slow(String name) {
        return cluster(name).toBuilder().setConnectTimeout(Duration.newBuilder().setSeconds(30)).build();
    }

    private static boolean everyName(String unused) {
        return true;
    }

    private static <R extends Message> SnapshotConfigBuilder<R, String> builder(Class<R> type) {
        return new SnapshotConfigBuilder<>(type);
    }

    private static <R extends Message> List<R> added(Class<R> type, SnapshotUpdate<String> update) {
        return builder(type).addOrder(update, SnapshotConfigBuilderTest::everyName, "c").addAndUpdates().stream().map(NamedMessage::message).collect(Collectors.toList());
    }

    private static <R extends Message> List<String> removed(Class<R> type, SnapshotUpdate<String> update) {
        Response<R> response = builder(type).removeOrder(update, SnapshotConfigBuilderTest::everyName, "c");
        return response.removes();
    }
}
