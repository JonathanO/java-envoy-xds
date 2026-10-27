package net.woaf.jono.envoy.xds.snapshot;

import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import net.woaf.jono.envoy.xds.TypeUrl;
import org.junit.jupiter.api.Test;

import static net.woaf.jono.envoy.xds.snapshot.TestResources.all;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cla;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cluster;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.route;
import static org.assertj.core.api.Assertions.assertThat;

public class PhasesTest {

    private final SotwStream stream = new SotwStream(Cluster.class, ClusterLoadAssignment.class, RouteConfiguration.class);

    @Test
    public void laterPhaseWaitsForTheEarlierOneToBeAckedEvenWhenBothQueue() {
        stream.source.update(all(e -> e.put(cluster("a")).put(cla("a", "10.0.0.1")).put(route("r", "a"))));
        stream.ack(stream.request(TypeUrl.CDS, null));
        stream.ack(stream.request(TypeUrl.EDS, null, "a"), "a");
        stream.ack(stream.request(TypeUrl.RDS, null, "r"), "r");

        // Envoy is busy with new endpoints when the phased change arrives, so both phases wait in its backlog.
        stream.source.update(all(e -> e.put(cla("a", "10.0.0.2"))));
        stream.runTasks();
        DiscoveryResponse busy = stream.last();
        stream.source.update(c -> c
            .group("all", e -> e.put(route("r"))) // stop routing to a first
            .then()
            .group("all", e -> e.put(cluster("a").toBuilder().setDnsLookupFamily(Cluster.DnsLookupFamily.V4_ONLY).build())));
        stream.runTasks();
        stream.ack(busy, "a");

        // Without phases the cluster would go first, as make before break sends clusters before routes.
        DiscoveryResponse routes = stream.last();
        assertThat(routes.getTypeUrl()).isEqualTo(TypeUrl.RDS.getTypeUrl());
        int before = stream.responses.size();
        stream.ack(routes, "r");
        assertThat(stream.responses).hasSize(before + 1);
        assertThat(stream.last().getTypeUrl()).as("the second phase, once the first is ACKed").isEqualTo(TypeUrl.CDS.getTypeUrl());
    }

    @Test
    public void eachClientGetsEachPhaseOnItsOwnAndTheCallerGetsThemMerged() {
        SnapshotSource<String, String, String> views = new SnapshotSource<>(Function.identity(), Runnable::run);
        views.update(c -> c.view("c", "routes", "clusters").view("other", "routes"));
        List<SnapshotUpdate<String>> toC = new ArrayList<>();
        List<SnapshotUpdate<String>> toOther = new ArrayList<>();
        assertThat(views.subscribe(toC::add, "c")).isNotNull();
        assertThat(views.subscribe(toOther::add, "other")).isNotNull();

        var updates = views.update(c -> c.group("routes", e -> e.put(route("r")))
            .then().then() // an empty phase is no phase
            .group("clusters", e -> e.put(cluster("a")))
            .then()
            .group("clusters", e -> e.put(cluster("b"))));

        assertThat(toC).hasSize(3);
        assertThat(toC).extracting(SnapshotUpdate::mergesWithLater).as("each phase but the last ends one").containsExactly(false, false, true);
        assertThat(toOther).as("not in a later phase, so it merges as usual").singleElement().returns(true, SnapshotUpdate::mergesWithLater);
        assertThat(views.group("clusters").get(Cluster.class)).as("a later phase edits on top of an earlier one").containsOnlyKeys("a", "b");
        SnapshotUpdate<String> whole = updates.get("c");
        assertThat(whole).isNotNull();
        assertThat(whole.mergesWithLater()).isTrue();
        assertThat(new SnapshotConfigBuilder<Cluster, String>(Cluster.class).addOrder(whole, name -> true, "c").addAndUpdates())
            .as("the caller gets every phase merged").hasSize(2);
    }
}
