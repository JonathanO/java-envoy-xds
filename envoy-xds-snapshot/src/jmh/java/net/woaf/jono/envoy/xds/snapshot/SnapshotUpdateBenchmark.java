package net.woaf.jono.envoy.xds.snapshot;

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

    private Published<Object> before = Published.none();
    private Published<Object> edited = Published.none();
    private Published<Object> rebuilt = Published.none();

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

        Snapshot.Editor change = first.edit();
        for (int i = 0; i < changed; i++) {
            change.put(cluster(i, 2));
        }
        Snapshot next = change.build();

        // The same config built from scratch, as a control plane that rebuilds its whole snapshot would publish it.
        Snapshot.Editor fresh = Snapshot.empty().edit();
        for (int i = 0; i < resources; i++) {
            fresh.put(cluster(i, i < changed ? 2 : 1));
        }

        before = new Published<>(first, null);
        edited = new Published<>(next, null);
        rebuilt = new Published<>(fresh.build(), null);
    }

    /** A change made with Snapshot.edit(): only the names it touched are compared. */
    @Benchmark
    public Response<Cluster> editedSnapshot() {
        return clusters.addOrder(new SnapshotUpdate<>(before, edited, edited.base().touched()), name -> true, client);
    }

    /** The same change in a rebuilt snapshot: every resource is compared, field by field. */
    @Benchmark
    public Response<Cluster> rebuiltSnapshot() {
        return clusters.addOrder(new SnapshotUpdate<>(before, rebuilt, null), name -> true, client);
    }

    /** Everything a client gets when it connects or reconnects. */
    @Benchmark
    public Resources<Cluster> connect() {
        return clusters.getResourcesRemoveOrder(new SnapshotUpdate<>(Published.none(), edited, null), name -> true, client);
    }
}
