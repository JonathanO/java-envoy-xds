package net.woaf.jono.envoy.xds.snapshot;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import net.woaf.jono.envoy.xds.api.utils.QueueingMergingEventListener;

/**
 * Holds the current snapshot and tells every connected client when it changes.
 *
 * <p>Optionally takes an overlay function that adds or replaces resources per client, given the base snapshot and the
 * client details from your ClientConfigProvider. Overlays are cached per snapshot and per distinct details, so give
 * your details type a value-based equals/hashCode to share them between clients.
 *
 * @param <D> client details type
 */
public class SnapshotSource<D> implements ClusterEventSource<SnapshotUpdate<D>> {

    private final ConcurrentHashMap<ClusterManagerEventListener<SnapshotUpdate<D>>, QueueingMergingEventListener<SnapshotUpdate<D>>> targets =
        new ConcurrentHashMap<>();

    @Nullable
    private final BiFunction<Snapshot, ? super D, Snapshot> overlay;

    @GuardedBy("this")
    private Published<D> current;

    public SnapshotSource() {
        this(null);
    }

    public SnapshotSource(@Nullable BiFunction<Snapshot, ? super D, Snapshot> overlay) {
        this.overlay = overlay;
        this.current = new Published<>(Snapshot.empty(), overlay);
    }

    public synchronized Snapshot current() {
        return current.base();
    }

    /**
     * Edit the current snapshot and publish the result.
     * @param edit applies your changes to an editor of the current snapshot
     * @return the update sent to clients
     */
    public synchronized SnapshotUpdate<D> update(UnaryOperator<Snapshot.Editor> edit) {
        return publish(edit.apply(current.base().edit()).build());
    }

    /**
     * Publish a snapshot. If it was built by editing the current snapshot, clients only compare the names you touched.
     * Otherwise every resource is compared.
     * @param next the new desired config
     * @return the update sent to clients
     */
    public synchronized SnapshotUpdate<D> publish(Snapshot next) {
        Published<D> published = new Published<>(next, overlay);
        SnapshotUpdate<D> update = new SnapshotUpdate<>(current, published, next.parentId() == current.base().id() ? next.touched() : null);
        current = published;
        // Enqueue only: each listener runs on its own thread, so we never call into a stream while holding our lock.
        targets.values().forEach(t -> t.onNetworkChange(update));
        return update;
    }

    @Override
    public synchronized SnapshotUpdate<D> subscribe(ClusterManagerEventListener<SnapshotUpdate<D>> listener) {
        targets.computeIfAbsent(listener, QueueingMergingEventListener::createAndStart);
        return new SnapshotUpdate<>(Published.none(), current, null);
    }

    @Override
    public void unsubscribe(ClusterManagerEventListener<SnapshotUpdate<D>> listener) {
        QueueingMergingEventListener<SnapshotUpdate<D>> old = targets.remove(listener);
        if (old != null) {
            old.close();
        }
    }
}
