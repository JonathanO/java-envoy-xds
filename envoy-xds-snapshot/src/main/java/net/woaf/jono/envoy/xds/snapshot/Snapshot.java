package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Immutable desired xDS config: resources keyed by type, then by name.
 *
 * <p>Build new snapshots from old ones with {@link #edit()}. Types and resources you don't touch are shared with the
 * parent, and the names you do touch are recorded, so working out what changed costs as much as the change rather than
 * the whole config.
 */
@Immutable
public final class Snapshot {

    private static final AtomicLong IDS = new AtomicLong();

    private static final Snapshot EMPTY = new Snapshot(-1, ImmutableMap.of(), null);

    private final long id = IDS.incrementAndGet();

    private final long parentId;

    private final ImmutableMap<Class<? extends Message>, ImmutableMap<String, Message>> resources;

    /** Names changed relative to the parent, by type. Null if there is no parent. */
    @Nullable
    private final ImmutableMap<Class<? extends Message>, ImmutableSet<String>> touched;

    private Snapshot(long parentId, ImmutableMap<Class<? extends Message>, ImmutableMap<String, Message>> resources,
        @Nullable ImmutableMap<Class<? extends Message>, ImmutableSet<String>> touched) {
        this.parentId = parentId;
        this.resources = resources;
        this.touched = touched;
    }

    public static Snapshot empty() {
        return EMPTY;
    }

    /**
     * Resources of one type, by name.
     * @param type resource class, e.g. Cluster.class
     * @param <R> resource type
     * @return resources of that type, empty if none
     */
    @SuppressWarnings("unchecked") // Editor.put() only ever files a resource under its own class.
    public <R extends Message> ImmutableMap<String, R> get(Class<R> type) {
        return (ImmutableMap<String, R>) resources.getOrDefault(type, ImmutableMap.of());
    }

    public ImmutableSet<Class<? extends Message>> types() {
        return resources.keySet();
    }

    public Editor edit() {
        return new Editor();
    }

    long id() {
        return id;
    }

    long parentId() {
        return parentId;
    }

    @Nullable
    ImmutableMap<Class<? extends Message>, ImmutableSet<String>> touched() {
        return touched;
    }

    /**
     * The name Envoy knows a resource by: its {@code name} field, or {@code cluster_name} for ClusterLoadAssignment.
     */
    static String nameOf(Message resource) {
        FieldDescriptor field = resource.getDescriptorForType().findFieldByName("name");
        if (field == null) {
            field = resource.getDescriptorForType().findFieldByName("cluster_name");
        }
        Preconditions.checkArgument(field != null && field.getJavaType() == FieldDescriptor.JavaType.STRING,
            "Can't work out the name of a %s, use put(name, resource)", resource.getDescriptorForType().getFullName());
        return (String) resource.getField(field);
    }

    /**
     * Builds a child snapshot. Not thread safe.
     */
    public final class Editor {

        private final Map<Class<? extends Message>, Map<String, Message>> edited = new HashMap<>();

        private final Map<Class<? extends Message>, Set<String>> touchedNames = new HashMap<>();

        private Editor() {
        }

        @CanIgnoreReturnValue // returns the editor for chaining
        public Editor put(Message resource) {
            return put(nameOf(resource), resource);
        }

        @CanIgnoreReturnValue // returns the editor for chaining
        public Editor put(String name, Message resource) {
            Class<? extends Message> type = resource.getClass();
            if (resource.equals(current(type).get(name))) {
                return this;
            }
            mutable(type).put(name, resource);
            touchedNames.computeIfAbsent(type, t -> new HashSet<>()).add(name);
            return this;
        }

        @CanIgnoreReturnValue // returns the editor for chaining
        public Editor remove(Class<? extends Message> type, String name) {
            if (!current(type).containsKey(name)) {
                return this;
            }
            mutable(type).remove(name);
            touchedNames.computeIfAbsent(type, t -> new HashSet<>()).add(name);
            return this;
        }

        public Snapshot build() {
            Map<Class<? extends Message>, ImmutableMap<String, Message>> merged = new HashMap<>(resources);
            edited.forEach((type, values) -> merged.put(type, ImmutableMap.copyOf(values)));
            ImmutableMap.Builder<Class<? extends Message>, ImmutableSet<String>> touchedBuilder = ImmutableMap.builder();
            touchedNames.forEach((type, names) -> touchedBuilder.put(type, ImmutableSet.copyOf(names)));
            return new Snapshot(id, ImmutableMap.copyOf(merged), touchedBuilder.build());
        }

        private Map<String, ? extends Message> current(Class<? extends Message> type) {
            Map<String, Message> values = edited.get(type);
            return values != null ? values : resources.getOrDefault(type, ImmutableMap.of());
        }

        // ponytail: copies the whole type's map on first edit, O(resources of that type) per edited type; use a persistent map if that shows in profiles.
        private Map<String, Message> mutable(Class<? extends Message> type) {
            return edited.computeIfAbsent(type, t -> {
                ImmutableMap<String, Message> existing = resources.get(t);
                return existing != null ? new HashMap<>(existing) : new HashMap<>();
            });
        }
    }
}
