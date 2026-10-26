package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.util.HashSet;
import java.util.Set;

/**
 * One view as published at one point: its groups' names and contents, in order. Groups are immutable and shared, so
 * this costs a reference per group, and a client holding it keeps nothing else alive.
 */
final class ViewState {

    static final ViewState EMPTY = new ViewState(ImmutableList.of(), ImmutableList.of());

    private final ImmutableList<String> names;

    private final ImmutableList<Snapshot> groups;

    ViewState(ImmutableList<String> names, ImmutableList<Snapshot> groups) {
        this.names = names;
        this.groups = groups;
    }

    /**
     * {@return a group's contents in this view, empty if it hasn't that group}
     */
    Snapshot group(String name) {
        int i = names.indexOf(name);
        return i < 0 ? Snapshot.empty() : groups.get(i);
    }

    /** The resource this view sees: the first of its groups to have one by that name. */
    @Nullable
    <R extends Message> R lookup(Class<R> type, String name) {
        // ponytail: scans the view's groups, O(groups in the view) per name; memoise a merged index here if that shows.
        for (Snapshot group : groups) {
            R value = group.get(type).get(name);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    Set<String> names(Class<? extends Message> type) {
        Set<String> all = new HashSet<>();
        for (Snapshot group : groups) {
            all.addAll(group.get(type).keySet());
        }
        return all;
    }
}
