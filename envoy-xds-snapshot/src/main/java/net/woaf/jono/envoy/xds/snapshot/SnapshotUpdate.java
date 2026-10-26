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
import java.util.function.Function;
import net.woaf.jono.envoy.xds.api.Mergeable;

/**
 * A move from one published version to the next. One instance is shared by every client it concerns.
 *
 * <p>Clients are only sent the updates that touch their view, so a client may skip versions. Merging keeps the oldest
 * previous and the newest current, so any number of updates arriving while a client is busy collapse into one change.
 *
 * @param <D> client details type
 */
public final class SnapshotUpdate<D> implements Mergeable<SnapshotUpdate<D>> {

    private final Function<? super D, String> viewOf;

    private final Published previous;

    private final Published current;

    /** Names that may differ between previous and current, by group, then by type. */
    private final ImmutableMap<String, ImmutableMap<Class<? extends Message>, ImmutableSet<String>>> touched;

    /** Views whose list of groups changed: every name in them is compared. */
    private final ImmutableSet<String> changedViews;

    SnapshotUpdate(Function<? super D, String> viewOf, Published previous, Published current,
        ImmutableMap<String, ImmutableMap<Class<? extends Message>, ImmutableSet<String>>> touched, ImmutableSet<String> changedViews) {
        this.viewOf = viewOf;
        this.previous = previous;
        this.current = current;
        this.touched = touched;
        this.changedViews = changedViews;
    }

    Published previous() {
        return previous;
    }

    Published current() {
        return current;
    }

    @Override
    public SnapshotUpdate<D> merge(SnapshotUpdate<D> next) {
        Preconditions.checkArgument(next.previous.version() >= current.version(), "Updates must be merged in the order they were published");
        Map<String, ImmutableMap<Class<? extends Message>, ImmutableSet<String>>> groups = new HashMap<>(touched);
        next.touched.forEach((group, names) -> groups.merge(group, names, SnapshotUpdate::union));
        return new SnapshotUpdate<>(viewOf, previous, next.current, ImmutableMap.copyOf(groups),
            Sets.union(changedViews, next.changedViews).immutableCopy());
    }

    private static ImmutableMap<Class<? extends Message>, ImmutableSet<String>> union(
        ImmutableMap<Class<? extends Message>, ImmutableSet<String>> a, ImmutableMap<Class<? extends Message>, ImmutableSet<String>> b) {
        Map<Class<? extends Message>, ImmutableSet<String>> union = new HashMap<>(a);
        b.forEach((type, names) -> union.merge(type, names, (x, y) -> Sets.union(x, y).immutableCopy()));
        return ImmutableMap.copyOf(union);
    }

    @Nullable
    <R extends Message> R before(Class<R> type, String name, D details) {
        return previous.lookup(type, name, viewOf.apply(details));
    }

    @Nullable
    <R extends Message> R after(Class<R> type, String name, D details) {
        return current.lookup(type, name, viewOf.apply(details));
    }

    /** Names this client may see change: everything if its view's groups changed, else the names touched in them. */
    Set<String> candidates(Class<? extends Message> type, D details) {
        String view = viewOf.apply(details);
        Set<String> names = new HashSet<>();
        if (changedViews.contains(view)) {
            names.addAll(previous.names(type, view));
            names.addAll(current.names(type, view));
            return names;
        }
        // The view's groups are the same before and after, so current says which touched groups are in it.
        touched.forEach((group, byType) -> {
            if (current.viewsWith(group).contains(view)) {
                names.addAll(byType.getOrDefault(type, ImmutableSet.of()));
            }
        });
        return names;
    }

    Set<String> namesBefore(Class<? extends Message> type, D details) {
        return previous.names(type, viewOf.apply(details));
    }

    Set<String> namesAfter(Class<? extends Message> type, D details) {
        return current.names(type, viewOf.apply(details));
    }
}
