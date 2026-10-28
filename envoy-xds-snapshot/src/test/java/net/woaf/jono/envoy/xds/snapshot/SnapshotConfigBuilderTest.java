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
import java.util.Map;
import java.util.Objects;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.NamedMessage;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.Response;
import org.junit.jupiter.api.Test;

public class SnapshotConfigBuilderTest {

    private final SnapshotSource<String, String, String> source = TestResources.oneGroup();

    @Test
    public void addingRouteClusterAndEndpointsTogetherIsAllAddOrder() {
        source.update(all(e -> e.put(route("all_routes"))));

        SnapshotUpdate<String> update = sent(source.update(all(e -> e.put(cluster("foo")).put(cla("foo", "127.0.0.5")).put(route("all_routes", "foo")))), "all");

        assertThat(added(Cluster.class, update)).containsExactly(cluster("foo"));
        assertThat(added(ClusterLoadAssignment.class, update)).containsExactly(cla("foo", "127.0.0.5"));
        assertThat(added(RouteConfiguration.class, update)).containsExactly(route("all_routes", "foo"));
        assertThat(removed(Cluster.class, update)).isEmpty();
        assertThat(removed(RouteConfiguration.class, update)).isEmpty();
    }

    @Test
    public void removingRouteAndClusterUpdatesRouteThenRemovesCluster() {
        source.update(all(e -> e.put(cluster("foo")).put(cla("foo", "127.0.0.5")).put(route("all_routes", "foo"))));

        SnapshotUpdate<String> update = sent(source.update(all(e -> e.put(route("all_routes")).remove(Cluster.class, "foo").remove(ClusterLoadAssignment.class, "foo"))), "all");

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
        SnapshotUpdate<String> add = sent(source.update(all(e -> e.put(cluster("bar")))), "all");
        SnapshotUpdate<String> remove = sent(source.update(all(e -> e.remove(Cluster.class, "bar"))), "all");

        SnapshotUpdate<String> merged = add.merge(remove);

        assertThat(added(Cluster.class, merged)).isEmpty();
        assertThat(removed(Cluster.class, merged)).isEmpty();
    }

    @Test
    public void replacedGroupIsComparedInFull() {
        source.update(all(e -> e.put(cluster("foo")).put(cluster("same"))));

        SnapshotUpdate<String> update = sent(source.update(c -> c.group("all", Snapshot.empty().edit().put(cluster("bar")).put(cluster("same")).build())), "all");

        assertThat(added(Cluster.class, update)).containsExactly(cluster("bar"));
        assertThat(removed(Cluster.class, update)).containsExactly("foo");
    }

    @Test
    public void unsubscribedNamesAreIgnored() {
        SnapshotUpdate<String> update = sent(source.update(all(e -> e.put(cluster("foo")).put(cluster("bar")))), "all");

        assertThat(builder(Cluster.class).addOrder(update, "foo"::equals, "c").addAndUpdates()).extracting(NamedMessage::name).containsExactly("foo");
    }

    @Test
    public void firstGroupInTheViewWins() {
        SnapshotSource<String, String, String> views = viewPerClient();
        views.update(c -> c.view("c", "own", "shared")
            .group("shared", e -> e.put(cluster("foo")))
            .group("own", e -> e.put(slow("foo"))));

        SnapshotUpdate<String> sharedChange = sent(views.update(c -> c.group("shared", e -> e.put(cluster("foo").toBuilder().setAltStatName("new").build()))), "c");
        assertThat(added(Cluster.class, sharedChange)).isEmpty();

        SnapshotUpdate<String> overrideRemoved = sent(views.update(c -> c.group("own", e -> e.remove(Cluster.class, "foo"))), "c");
        assertThat(added(Cluster.class, overrideRemoved)).containsExactly(cluster("foo").toBuilder().setAltStatName("new").build());
        assertThat(removed(Cluster.class, overrideRemoved)).isEmpty();
    }

    @Test
    public void changingAViewsGroupsComparesEverythingInIt() {
        SnapshotSource<String, String, String> views = viewPerClient();
        views.update(c -> c.view("c", "a").group("a", e -> e.put(cluster("foo"))).group("b", e -> e.put(cluster("bar")).put(cluster("foo"))));

        SnapshotUpdate<String> update = sent(views.update(c -> c.view("c", "b")), "c");

        assertThat(added(Cluster.class, update)).containsExactly(cluster("bar"));
        assertThat(removed(Cluster.class, update)).isEmpty();
        assertThat(removed(Cluster.class, sent(views.update(c -> c.removeView("c")), "c"))).containsExactlyInAnyOrder("foo", "bar");
    }

    @Test
    public void onlyClientsWhoseViewHasTheGroupAreWoken() {
        SnapshotSource<String, String, String> views = new SnapshotSource<>(Function.identity(), Runnable::run);
        views.update(c -> c.view("a", "own-a", "shared").view("b", "own-b"));
        List<String> woken = new ArrayList<>();
        assertThat(views.subscribe(update -> woken.add("a"), "a")).isNotNull();
        assertThat(views.subscribe(update -> woken.add("b"), "b")).isNotNull();

        views.update(c -> c.group("shared", e -> e.put(cluster("foo"))));
        views.update(c -> c.group("own-b", e -> e.put(cluster("foo"))));
        views.update(c -> c.group("nobody", e -> e.put(cluster("foo"))));
        views.update(c -> c.group("shared", e -> e.put(cluster("foo")))); // no change
        views.update(c -> c.view("a", "own-a"));
        views.update(c -> c.group("shared", e -> e.put(cluster("bar"))));

        assertThat(woken).containsExactly("a", "b", "a");
    }

    @Test
    public void viewLeavingASharedGroupLeavesTheOthersInIt() {
        SnapshotSource<String, String, String> views = viewPerClient();
        views.update(c -> c.view("a", "shared").view("b", "shared"));
        views.update(c -> c.view("a", "own"));

        assertThat(views.update(c -> c.group("shared", e -> e.put(cluster("foo"))))).containsOnlyKeys("b");
    }

    @Test
    public void replacingAGroupWhereNeitherHasAnyNamesWakesNobody() {
        SnapshotSource<String, String, String> views = viewPerClient();
        views.update(c -> c.view("c", "g"));
        // Has a type, but nothing of it.
        Snapshot emptied = Snapshot.empty().edit().put(cluster("foo")).remove(Cluster.class, "foo").build();

        assertThat(views.update(c -> c.group("g", emptied))).isEmpty();
    }

    @Test
    public void editingAGroupTwiceInAChangeEditsOnTopOfTheFirst() {
        SnapshotSource<String, String, String> views = viewPerClient();

        views.update(c -> c.group("g", e -> e.put(cluster("a"))).group("g", e -> e.put(cluster("b"))));

        assertThat(views.group("g").get(Cluster.class)).containsOnlyKeys("a", "b");
    }

    @Test
    public void viewsAreAsLastPublished() {
        SnapshotSource<String, String, String> views = viewPerClient();
        views.update(c -> c.view("a", "g", "h").view("b", "g"));
        assertThat(views.view("a")).containsExactly("g", "h");

        views.update(c -> c.removeView("a").removeGroup("g").group("h", Snapshot.empty()).view("b", "h"));

        assertThat(views.view("a")).isEmpty();
        assertThat(views.view("b")).containsExactly("h");
    }

    @Test
    public void eachUpdateFollowsOnFromWhatTheClientHas() {
        SnapshotSource<String, String, String> views = new SnapshotSource<>(Function.identity(), Runnable::run);
        views.update(c -> c.view("c", "g"));
        List<SnapshotUpdate<String>> got = new ArrayList<>();
        final SnapshotUpdate<String> connected = views.subscribe(got::add, "c");

        views.update(c -> c.group("g", e -> e.put(cluster("a"))));
        views.update(c -> c.group("g", e -> e.put(cluster("b"))));

        assertThat(got).hasSize(2);
        assertThat(got.get(0).previous()).isSameAs(connected.current());
        assertThat(got.get(1).previous()).isSameAs(got.get(0).current());
        assertThat(got.get(1).current().group("g")).isSameAs(views.group("g"));
        assertThat(got.get(1).current().group("missing")).isSameAs(Snapshot.empty());
    }

    @Test
    public void unsubscribingClosesTheListenerAndStopsItsUpdates() {
        SnapshotSource<String, String, String> views = new SnapshotSource<>(Function.identity(), Runnable::run);
        views.update(c -> c.view("c", "g"));
        List<String> events = new ArrayList<>();
        ClusterManagerEventListener<SnapshotUpdate<String>> listener = new ClusterManagerEventListener<>() {
            @Override
            public void onNetworkChange(SnapshotUpdate<String> update) {
                events.add("update");
            }

            @Override
            public void close() {
                events.add("closed");
            }
        };
        views.unsubscribe(listener); // never subscribed, so nothing to do
        assertThat(views.subscribe(listener, "c")).isNotNull();
        List<SnapshotUpdate<String>> other = new ArrayList<>();
        assertThat(views.subscribe(other::add, "c")).isNotNull();

        views.update(c -> c.group("g", e -> e.put(cluster("a"))));
        views.unsubscribe(listener);
        views.update(c -> c.group("g", e -> e.put(cluster("b"))));

        assertThat(events).containsExactly("update", "closed");
        assertThat(other).as("another client of the view still gets them").hasSize(2);
    }

    @Test
    public void removingAGroupUncoversWhatItOverrode() {
        SnapshotSource<String, String, String> views = new SnapshotSource<>(Function.identity(), Runnable::run);
        views.update(c -> c.view("c", "own", "shared").view("other", "theirs")
            .group("own", e -> e.put(slow("foo")).put(cluster("mine")))
            .group("shared", e -> e.put(cluster("foo")))
            .group("theirs", e -> e.put(cluster("bar"))));
        List<String> woken = new ArrayList<>();
        assertThat(views.subscribe(update -> woken.add("c"), "c")).isNotNull();
        assertThat(views.subscribe(update -> woken.add("other"), "other")).isNotNull();

        Map<String, SnapshotUpdate<String>> updates = views.update(c -> c.removeGroup("own"));

        SnapshotUpdate<String> update = sent(updates, "c");
        assertThat(added(Cluster.class, update)).containsExactly(cluster("foo"));
        assertThat(removed(Cluster.class, update)).containsExactly("mine");
        assertThat(updates).doesNotContainKey("other");
        assertThat(woken).containsExactly("c");
    }

    @Test
    public void updatesMergeAcrossVersionsTheClientSkipped() {
        SnapshotSource<String, String, String> views = viewPerClient();
        views.update(c -> c.view("c", "mine").view("other", "theirs"));
        SnapshotUpdate<String> first = sent(views.update(c -> c.group("mine", e -> e.put(cluster("foo")))), "c");
        views.update(c -> c.group("theirs", e -> e.put(cluster("bar"))));
        SnapshotUpdate<String> second = sent(views.update(c -> c.group("mine", e -> e.put(cluster("baz")))), "c");

        SnapshotUpdate<String> merged = first.merge(second);

        assertThat(added(Cluster.class, merged)).containsExactlyInAnyOrder(cluster("foo"), cluster("baz"));
        assertThatThrownBy(() -> second.merge(first)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void clientWithNoViewSeesNothingUntilItIsAdded() {
        SnapshotSource<String, String, String> views = viewPerClient();
        views.update(c -> c.group("g", e -> e.put(cluster("foo"))));

        SnapshotUpdate<String> connected = views.subscribe(update -> { }, "c");
        assertThat(added(Cluster.class, connected)).isEmpty();
        assertThat(added(Cluster.class, sent(views.update(c -> c.view("c", "g")), "c"))).containsExactly(cluster("foo"));
    }

    @Test
    public void viewOfReturningNullFailsClearly() {
        SnapshotSource<String, String, String> views = new SnapshotSource<>(details -> null);

        assertThatThrownBy(() -> views.subscribe(update -> { }, "c")).isInstanceOf(NullPointerException.class).hasMessageContaining("viewOf returned null for c");
    }

    @Test
    public void subscribingNeedsTheClientsDetails() {
        assertThatThrownBy(() -> source.subscribe(update -> { })).isInstanceOf(UnsupportedOperationException.class);
    }

    private static SnapshotUpdate<String> sent(Map<String, SnapshotUpdate<String>> updates, String view) {
        return Objects.requireNonNull(updates.get(view), view + " wasn't sent an update");
    }

    private static SnapshotSource<String, String, String> viewPerClient() {
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
