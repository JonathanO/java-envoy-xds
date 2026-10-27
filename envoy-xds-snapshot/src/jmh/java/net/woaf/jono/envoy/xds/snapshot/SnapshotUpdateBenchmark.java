package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import java.util.concurrent.TimeUnit;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.Resources;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.Response;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * What one snapshot change costs per connected client: each client's stream works out its own changes. Multiply by
 * the number of clients for the cost of a publish.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SnapshotUpdateBenchmark {

    @Param({"1000", "10000"})
    int resources;

    @Param({"1", "100"})
    int changed;

    private final SnapshotConfigBuilder<Cluster, Object> clusters = new SnapshotConfigBuilder<>(Cluster.class);
    private final Object client = new Object();

    private SnapshotUpdate<Object> edited;
    private SnapshotUpdate<Object> rebuilt;

    private static Cluster cluster(int i, int version) {
        return Cluster.newBuilder().setName("cluster-" + i).setAltStatName("v" + version).build();
    }

    @Setup
    public void setUp() {
        Snapshot.Editor base = Snapshot.empty().edit();
        for (int i = 0; i < resources; i++) {
            base.put(cluster(i, 1));
        }
        Snapshot first = base.build();

        // The same config built from scratch, as a control plane that rebuilds its whole snapshot would publish it.
        Snapshot.Editor fresh = Snapshot.empty().edit();
        for (int i = 0; i < resources; i++) {
            fresh.put(cluster(i, i < changed ? 2 : 1));
        }


        SnapshotSource<Object, String, String> source = new SnapshotSource<>(details -> "all", Runnable::run);
        source.update(c -> c.view("all", "all").group("all", first));
        edited = source.update(c -> c.group("all", e -> {
            for (int i = 0; i < changed; i++) {
                e.put(cluster(i, 2));
            }
            return e;
        })).get("all");
        SnapshotSource<Object, String, String> other = new SnapshotSource<>(details -> "all", Runnable::run);
        other.update(c -> c.view("all", "all").group("all", first));
        rebuilt = other.update(c -> c.group("all", fresh.build())).get("all");
    }

    /** A change made by editing the group: only the names it touched are compared. */
    @Benchmark
    public Response<Cluster> editedSnapshot() {
        return clusters.addOrder(edited, name -> true, client);
    }

    /** The same change as a rebuilt group: every resource is compared, field by field. */
    @Benchmark
    public Response<Cluster> rebuiltSnapshot() {
        return clusters.addOrder(rebuilt, name -> true, client);
    }

    /** Everything a client gets when it connects or reconnects. */
    @Benchmark
    public Resources<Cluster> connect() {
        return clusters.getResourcesRemoveOrder(new SnapshotUpdate<>(ViewState.EMPTY, edited.current(), true, ImmutableList.of()), name -> true, client);
    }
}
