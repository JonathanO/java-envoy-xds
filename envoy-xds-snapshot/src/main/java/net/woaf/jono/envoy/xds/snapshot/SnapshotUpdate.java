package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import net.woaf.jono.envoy.xds.api.Mergeable;

/**
 * A move from one published snapshot to the next. One instance is shared by every client.
 *
 * <p>Merging keeps the oldest previous and the newest current, so any number of snapshots published while a client is
 * busy collapse into one change.
 *
 * @param <D> client details type
 */
public final class SnapshotUpdate<D> implements Mergeable<SnapshotUpdate<D>> {

    private final Published<D> previous;

    private final Published<D> current;

    /** Base names that may differ between previous and current, by type. Null means compare everything. */
    @Nullable
    private final ImmutableMap<Class<? extends Message>, ImmutableSet<String>> touched;

    SnapshotUpdate(Published<D> previous, Published<D> current, @Nullable ImmutableMap<Class<? extends Message>, ImmutableSet<String>> touched) {
        this.previous = previous;
        this.current = current;
        this.touched = touched;
    }

    public Snapshot getPrevious() {
        return previous.base();
    }

    public Snapshot getCurrent() {
        return current.base();
    }

    @Override
    public SnapshotUpdate<D> merge(SnapshotUpdate<D> next) {
        Preconditions.checkArgument(next.previous == current, "Updates must be merged in the order they were published");
        if (touched == null || next.touched == null) {
            return new SnapshotUpdate<>(previous, next.current, null);
        }
        Map<Class<? extends Message>, ImmutableSet<String>> union = new HashMap<>(touched);
        next.touched.forEach((type, names) -> union.merge(type, names, (a, b) -> Sets.union(a, b).immutableCopy()));
        return new SnapshotUpdate<>(previous, next.current, ImmutableMap.copyOf(union));
    }

    @Nullable
    <R extends Message> R before(Class<R> type, String name, @Nullable D details) {
        return previous.lookup(type, name, details);
    }

    @Nullable
    <R extends Message> R after(Class<R> type, String name, @Nullable D details) {
        return current.lookup(type, name, details);
    }

    /** Names this client may see change: touched base names plus everything in either overlay. */
    Set<String> candidates(Class<? extends Message> type, @Nullable D details) {
        Set<String> names = new HashSet<>();
        if (touched == null) {
            names.addAll(previous.base().get(type).keySet());
            names.addAll(current.base().get(type).keySet());
        } else {
            ImmutableSet<String> touchedNames = touched.get(type);
            if (touchedNames != null) {
                names.addAll(touchedNames);
            }
        }
        names.addAll(previous.overlay(details).get(type).keySet());
        names.addAll(current.overlay(details).get(type).keySet());
        return names;
    }

    Set<String> namesBefore(Class<? extends Message> type, @Nullable D details) {
        return previous.names(type, details);
    }

    Set<String> namesAfter(Class<? extends Message> type, @Nullable D details) {
        return current.names(type, details);
    }
}
