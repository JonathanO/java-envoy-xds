package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One view as published at one point: its groups' names and contents, in order. Groups are immutable and shared, so
 * this costs a reference per group, and a client holding it keeps nothing else alive.
 */
final class ViewState {

    static final ViewState EMPTY = new ViewState(ImmutableList.of(), ImmutableList.of());

    private final ImmutableList<String> names;

    private final ImmutableList<Snapshot> groups;

    /**
     * What the view sees of each type, merged, once something compares the whole view: a client connecting, or a SotW
     * response. Changes to a few names scan the groups instead, so they never pay to build it.
     */
    private final Map<Class<? extends Message>, ImmutableMap<String, Message>> merged = new ConcurrentHashMap<>();

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
    @SuppressWarnings("unchecked") // merged files each resource under its own type
    <R extends Message> R lookup(Class<R> type, String name) {
        ImmutableMap<String, Message> all = merged.get(type);
        if (all != null) {
            return (R) all.get(name);
        }
        for (Snapshot group : groups) {
            R value = group.get(type).get(name);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    Set<String> names(Class<? extends Message> type) {
        return merged.computeIfAbsent(type, t -> {
            Map<String, Message> all = new HashMap<>();
            for (Snapshot group : groups) {
                group.get(t).forEach(all::putIfAbsent);
            }
            return ImmutableMap.copyOf(all);
        }).keySet();
    }
}
