package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Everything a SnapshotSource has published at one point: its groups, and each view's list of groups.
 */
final class Published {

    static final Published NONE = new Published(0, ImmutableMap.of(), ImmutableMap.of(), ImmutableSetMultimap.of());

    private final long version;

    private final ImmutableMap<String, Snapshot> groups;

    private final ImmutableMap<String, ImmutableList<String>> views;

    /** The views each group is in. */
    private final ImmutableSetMultimap<String, String> viewsByGroup;

    private Published(long version, ImmutableMap<String, Snapshot> groups, ImmutableMap<String, ImmutableList<String>> views,
        ImmutableSetMultimap<String, String> viewsByGroup) {
        this.version = version;
        this.groups = groups;
        this.views = views;
        this.viewsByGroup = viewsByGroup;
    }

    long version() {
        return version;
    }

    Snapshot group(String name) {
        return groups.getOrDefault(name, Snapshot.empty());
    }

    ImmutableList<String> view(String name) {
        return views.getOrDefault(name, ImmutableList.of());
    }

    ImmutableSet<String> viewsWith(String group) {
        return viewsByGroup.get(group);
    }

    /**
     * The next version, with some groups and views replaced. A null value removes that group or view.
     */
    // ponytail: copies the group map on every publish and rebuilds viewsByGroup on every view change, O(groups) and
    // O(total view length). Fine for thousands; use persistent maps if it shows in MeshBenchmark.
    Published with(Map<String, @Nullable Snapshot> groupChanges, Map<String, @Nullable ImmutableList<String>> viewChanges) {
        ImmutableMap<String, Snapshot> nextGroups = groupChanges.isEmpty() ? groups : replace(groups, groupChanges);
        if (viewChanges.isEmpty()) {
            return new Published(version + 1, nextGroups, views, viewsByGroup);
        }
        ImmutableMap<String, ImmutableList<String>> nextViews = replace(views, viewChanges);
        ImmutableSetMultimap.Builder<String, String> index = ImmutableSetMultimap.builder();
        nextViews.forEach((view, list) -> list.forEach(group -> index.put(group, view)));
        return new Published(version + 1, nextGroups, nextViews, index.build());
    }

    private static <V> ImmutableMap<String, V> replace(ImmutableMap<String, V> old, Map<String, @Nullable V> changes) {
        ImmutableMap.Builder<String, V> next = ImmutableMap.builderWithExpectedSize(old.size() + changes.size());
        old.forEach((name, value) -> {
            if (!changes.containsKey(name)) {
                next.put(name, value);
            }
        });
        changes.forEach((name, value) -> {
            if (value != null) {
                next.put(name, value);
            }
        });
        return next.build();
    }

    /** The resource a view sees: the first of its groups to have one by that name. */
    @Nullable
    <R extends Message> R lookup(Class<R> type, String name, String view) {
        // ponytail: scans the view's groups, O(groups in the view) per name; keep a merged index per view if that shows.
        for (String group : view(view)) {
            R value = group(group).get(type).get(name);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    Set<String> names(Class<? extends Message> type, String view) {
        Set<String> names = new HashSet<>();
        for (String group : view(view)) {
            names.addAll(group(group).get(type).keySet());
        }
        return names;
    }
}
