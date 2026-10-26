package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import net.woaf.jono.envoy.xds.api.utils.SerialEventListener;

/**
 * Holds named groups of resources and named views made of them, and tells each connected client when its view changes.
 *
 * <p>A group is a {@link Snapshot}: any resources, of any types. A view is an ordered list of group names, and a client
 * sees every resource in its view's groups; where two groups have a resource of the same type and name, the first
 * group's wins. Your {@code viewOf} function picks each client's view from the client details your ClientConfigProvider
 * returned, so clients with the same view share it.
 *
 * <p>Changing a group only wakes the clients whose view has it, and each client compares only the names you touched.
 * A client whose view doesn't exist sees nothing until you add it.
 *
 * <p>Everyone seeing the same config is one group in one view:
 * <pre>{@code
 * SnapshotSource<Object> source = new SnapshotSource<>(details -> "all");
 * source.update(c -> c.view("all", "all").group("all", g -> g.put(cluster)));
 * }</pre>
 *
 * @param <D> client details type
 */
public class SnapshotSource<D> implements ClusterEventSource<SnapshotUpdate<D>> {

    private final ConcurrentHashMap<ClusterManagerEventListener<SnapshotUpdate<D>>, Target<D>> targets = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Set<SerialEventListener<SnapshotUpdate<D>>>> byView = new ConcurrentHashMap<>();

    private final Function<? super D, String> viewOf;

    private final Executor executor;

    @GuardedBy("this")
    private final Map<String, Snapshot> groups = new HashMap<>();

    @GuardedBy("this")
    private final Map<String, ImmutableList<String>> views = new HashMap<>();

    /** The views each group is in. */
    @GuardedBy("this")
    private final Map<String, Set<String>> viewsByGroup = new HashMap<>();

    /** Each view's state as its clients were last sent it. */
    @GuardedBy("this")
    private final Map<String, ViewState> states = new HashMap<>();

    private record Target<D>(SerialEventListener<SnapshotUpdate<D>> listener, String view) {
    }

    /**
     * A source delivering on {@link SerialEventListener#sharedExecutor()}.
     * @param viewOf the name of the view a client sees, given its details
     */
    public SnapshotSource(Function<? super D, String> viewOf) {
        this(viewOf, SerialEventListener.sharedExecutor());
    }

    /**
     * Use your own executor to deliver updates to clients, instead of {@link SerialEventListener#sharedExecutor()}.
     *
     * <p>It must run tasks on threads of its own, not the caller's: with {@code MoreExecutors.directExecutor()},
     * publishing delivers to a stream while holding this source's lock, and a stream subscribing at the same time holds
     * its own lock while it waits for this one, so they deadlock.
     * @param viewOf the name of the view a client sees, given its details
     * @param executor runs each client's updates, one at a time per client, on threads other than the caller's
     */
    public SnapshotSource(Function<? super D, String> viewOf, Executor executor) {
        this.viewOf = viewOf;
        this.executor = executor;
    }

    /**
     * {@return a group as last published, empty if there's none}
     * @param name the group's name
     */
    public synchronized Snapshot group(String name) {
        return groups.getOrDefault(name, Snapshot.empty());
    }

    /**
     * {@return a view's groups as last published, empty if there's no such view}
     * @param name the view's name
     */
    public synchronized ImmutableList<String> view(String name) {
        return views.getOrDefault(name, ImmutableList.of());
    }

    /**
     * Change any number of groups and views, and publish them together. Clients get the whole change as one, so it's
     * make before break: anything added is in place before anything referring to it, whatever groups they're in.
     * @param change makes your changes
     * @return the update sent to each view that changed, by view name
     */
    @CanIgnoreReturnValue
    public synchronized ImmutableMap<String, SnapshotUpdate<D>> update(Consumer<Change> change) {
        Change edits = new Change(groups);
        change.accept(edits);

        Set<String> changedViews = new HashSet<>();
        edits.views.forEach((name, list) -> {
            ImmutableList<String> old = views.getOrDefault(name, ImmutableList.of());
            ImmutableList<String> next = list == null ? ImmutableList.of() : list;
            if (old.equals(next)) {
                return;
            }
            old.forEach(group -> viewsByGroup.computeIfPresent(group, (g, in) -> in.remove(name) && in.isEmpty() ? null : in));
            next.forEach(group -> viewsByGroup.computeIfAbsent(group, g -> new HashSet<>()).add(name));
            if (list == null) {
                views.remove(name);
            } else {
                views.put(name, next);
            }
            changedViews.add(name);
        });

        Map<String, List<ImmutableMap<Class<? extends Message>, ImmutableSet<String>>>> touchedByView = new HashMap<>();
        edits.touched.forEach((name, byType) -> {
            Snapshot group = edits.groups.get(name);
            boolean touchedAny = byType.values().stream().anyMatch(names -> !names.isEmpty());
            if (group == null) {
                groups.remove(name);
            } else if (touchedAny) {
                groups.put(name, group);
            }
            if (touchedAny) {
                ImmutableMap.Builder<Class<? extends Message>, ImmutableSet<String>> names = ImmutableMap.builder();
                byType.forEach((type, typeNames) -> names.put(type, ImmutableSet.copyOf(typeNames)));
                ImmutableMap<Class<? extends Message>, ImmutableSet<String>> touched = names.build();
                viewsByGroup.getOrDefault(name, Set.of()).forEach(view -> touchedByView.computeIfAbsent(view, v -> new ArrayList<>()).add(touched));
            }
        });
        changedViews.forEach(view -> touchedByView.putIfAbsent(view, List.of()));

        ImmutableMap.Builder<String, SnapshotUpdate<D>> updates = ImmutableMap.builder();
        touchedByView.forEach((view, touched) -> {
            ViewState before = states.getOrDefault(view, ViewState.EMPTY);
            ViewState after = stateOf(view);
            if (after == ViewState.EMPTY) {
                states.remove(view);
            } else {
                states.put(view, after);
            }
            boolean full = changedViews.contains(view);
            SnapshotUpdate<D> update = new SnapshotUpdate<>(before, after, full, full ? ImmutableList.of() : ImmutableList.copyOf(touched));
            updates.put(view, update);
            // Hand over only: listeners run on the executor, so we never call into a stream while holding our lock.
            byView.getOrDefault(view, Set.of()).forEach(t -> t.onNetworkChange(update));
        });
        return updates.build();
    }

    @GuardedBy("this")
    private ViewState stateOf(String view) {
        ImmutableList<String> names = views.get(view);
        if (names == null) {
            return ViewState.EMPTY;
        }
        ImmutableList.Builder<Snapshot> contents = ImmutableList.builderWithExpectedSize(names.size());
        for (String name : names) {
            contents.add(groups.getOrDefault(name, Snapshot.empty()));
        }
        return new ViewState(names, contents.build());
    }

    /**
     * Always fails: a SnapshotSource needs to know which client is subscribing. The core calls
     * {@link #subscribe(ClusterManagerEventListener, Object)} instead.
     */
    @Override
    public SnapshotUpdate<D> subscribe(ClusterManagerEventListener<SnapshotUpdate<D>> listener) {
        throw new UnsupportedOperationException("SnapshotSource needs the client's details: use subscribe(listener, clientDetails)");
    }

    @Override
    @SuppressWarnings("unchecked") // the details our SnapshotConfigBuilders are typed for, from the same ClientConfigProvider
    public synchronized SnapshotUpdate<D> subscribe(ClusterManagerEventListener<SnapshotUpdate<D>> listener, Object clientDetails) {
        String view = viewOf.apply((D) clientDetails);
        Target<D> target = targets.computeIfAbsent(listener, l -> new Target<>(new SerialEventListener<>(executor, l), view));
        // Inside compute, so it can't race an unsubscribe dropping the view's empty set.
        byView.compute(target.view(), (v, listeners) -> {
            Set<SerialEventListener<SnapshotUpdate<D>>> set = listeners != null ? listeners : ConcurrentHashMap.newKeySet();
            set.add(target.listener());
            return set;
        });
        return new SnapshotUpdate<>(ViewState.EMPTY, states.getOrDefault(target.view(), ViewState.EMPTY), true, ImmutableList.of());
    }

    @Override
    public void unsubscribe(ClusterManagerEventListener<SnapshotUpdate<D>> listener) {
        Target<D> old = targets.remove(listener);
        if (old != null) {
            byView.computeIfPresent(old.view(), (v, listeners) -> {
                listeners.remove(old.listener());
                return listeners.isEmpty() ? null : listeners;
            });
            old.listener().close();
        }
    }

    /**
     * Changes to groups and views, published together by {@link #update}. Not thread safe.
     */
    public static final class Change {

        private final Map<String, Snapshot> base;

        /** Groups as changed so far; a null value means removed. */
        private final Map<String, @Nullable Snapshot> groups = new HashMap<>();

        /** Names touched in each changed group, by type. */
        private final Map<String, Map<Class<? extends Message>, Set<String>>> touched = new HashMap<>();

        /** Views as changed so far; a null value means removed. */
        private final Map<String, @Nullable ImmutableList<String>> views = new LinkedHashMap<>();

        private Change(Map<String, Snapshot> base) {
            this.base = base;
        }

        /**
         * Edit a group, creating it if there's none. Only the names you touch are compared.
         * @param name the group's name
         * @param edit applies your changes to an editor of the group
         * @return this change
         */
        @CanIgnoreReturnValue // returns the change for chaining
        public Change group(String name, UnaryOperator<Snapshot.Editor> edit) {
            Snapshot edited = edit.apply(group(name).edit()).build();
            Map<Class<? extends Message>, Set<String>> names = touched.computeIfAbsent(name, n -> new HashMap<>());
            Map<Class<? extends Message>, ImmutableSet<String>> editTouched = edited.touched();
            if (editTouched != null) { // never null: the edit has a parent
                editTouched.forEach((type, typeNames) -> names.computeIfAbsent(type, t -> new HashSet<>()).addAll(typeNames));
            }
            groups.put(name, edited);
            return this;
        }

        /**
         * Replace a group with a snapshot built however you like. Every name in the old and new group is compared.
         * @param name the group's name
         * @param snapshot its new contents
         * @return this change
         */
        @CanIgnoreReturnValue // returns the change for chaining
        public Change group(String name, Snapshot snapshot) {
            touchAll(name, group(name));
            touchAll(name, snapshot);
            groups.put(name, snapshot);
            return this;
        }

        /**
         * {@return a group as this change has it so far, empty if there's none}
         * @param name the group's name
         */
        public Snapshot group(String name) {
            if (groups.containsKey(name)) {
                Snapshot changed = groups.get(name);
                return changed == null ? Snapshot.empty() : changed;
            }
            return base.getOrDefault(name, Snapshot.empty());
        }

        /**
         * Remove a group. Views that list it see nothing from it until it's added again.
         * @param name the group's name
         * @return this change
         */
        @CanIgnoreReturnValue // returns the change for chaining
        public Change removeGroup(String name) {
            touchAll(name, group(name));
            groups.put(name, null);
            return this;
        }

        /**
         * Set which groups a view is made of, creating it if there's none.
         * @param name the view's name
         * @param groups group names, first first: where groups have a resource of the same type and name, the first wins
         * @return this change
         */
        @CanIgnoreReturnValue // returns the change for chaining
        public Change view(String name, List<String> groups) {
            views.put(name, ImmutableList.copyOf(groups));
            return this;
        }

        /**
         * Set which groups a view is made of, creating it if there's none.
         * @param name the view's name
         * @param groups group names, first first: where groups have a resource of the same type and name, the first wins
         * @return this change
         */
        @CanIgnoreReturnValue // returns the change for chaining
        public Change view(String name, String... groups) {
            return view(name, List.of(groups));
        }

        /**
         * Remove a view. Its clients see nothing until it's added again.
         * @param name the view's name
         * @return this change
         */
        @CanIgnoreReturnValue // returns the change for chaining
        public Change removeView(String name) {
            views.put(name, null);
            return this;
        }

        private void touchAll(String name, Snapshot snapshot) {
            Map<Class<? extends Message>, Set<String>> names = touched.computeIfAbsent(name, n -> new HashMap<>());
            for (Class<? extends Message> type : snapshot.types()) {
                names.computeIfAbsent(type, t -> new HashSet<>()).addAll(snapshot.get(type).keySet());
            }
        }
    }
}
