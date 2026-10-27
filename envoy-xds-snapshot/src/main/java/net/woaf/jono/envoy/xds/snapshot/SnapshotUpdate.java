package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.util.HashSet;
import java.util.Set;
import net.woaf.jono.envoy.xds.api.Mergeable;

/**
 * A move of one view from one state to the next. One instance is shared by every client of that view.
 *
 * <p>Merging keeps the oldest previous and the newest current, so any number of updates arriving while a client is
 * busy collapse into one change.
 *
 * @param <D> client details type
 */
public final class SnapshotUpdate<D> implements Mergeable<SnapshotUpdate<D>> {

    private final ViewState previous;

    private final ViewState current;

    /** The view's groups changed: every name in it is compared. */
    private final boolean full;

    /** Otherwise, the names touched in each changed group of the view, by type. */
    private final ImmutableList<ImmutableMap<Class<? extends Message>, ImmutableSet<String>>> touched;

    SnapshotUpdate(ViewState previous, ViewState current, boolean full,
        ImmutableList<ImmutableMap<Class<? extends Message>, ImmutableSet<String>>> touched) {
        this.previous = previous;
        this.current = current;
        this.full = full;
        this.touched = touched;
    }

    ViewState previous() {
        return previous;
    }

    ViewState current() {
        return current;
    }

    @Override
    public SnapshotUpdate<D> merge(SnapshotUpdate<D> next) {
        Preconditions.checkArgument(next.previous == current, "Updates must be merged in the order they were published");
        return new SnapshotUpdate<>(previous, next.current, full || next.full,
            full || next.full ? ImmutableList.of() : ImmutableList.<ImmutableMap<Class<? extends Message>, ImmutableSet<String>>>builder()
                .addAll(touched).addAll(next.touched).build());
    }

    @Nullable
    <R extends Message> R before(Class<R> type, String name) {
        return previous.lookup(type, name);
    }

    @Nullable
    <R extends Message> R after(Class<R> type, String name) {
        return current.lookup(type, name);
    }

    /** Names that may differ between previous and current. */
    Set<String> candidates(Class<? extends Message> type) {
        Set<String> names = new HashSet<>();
        if (full) {
            names.addAll(previous.names(type));
            names.addAll(current.names(type));
            return names;
        }
        touched.forEach(byType -> names.addAll(byType.getOrDefault(type, ImmutableSet.of())));
        return names;
    }

    Set<String> namesBefore(Class<? extends Message> type) {
        return previous.names(type);
    }

    Set<String> namesAfter(Class<? extends Message> type) {
        return current.names(type);
    }
}
