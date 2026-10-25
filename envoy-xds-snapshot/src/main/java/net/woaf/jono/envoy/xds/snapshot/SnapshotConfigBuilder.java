package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;

/**
 * Serves one resource type straight from snapshots.
 *
 * <p>Added and changed resources go out in add order, deleted ones in remove order. Because the core's add order puts
 * referenced types before the types that reference them, a single snapshot change is applied make-before-break without
 * any hand-written ordering.
 *
 * @param <R> resource type
 * @param <D> client details type
 */
public final class SnapshotConfigBuilder<R extends Message, D> implements IncrementalConfigBuilder<R, SnapshotUpdate<D>, D> {

    private final Class<R> type;

    public SnapshotConfigBuilder(Class<R> type) {
        this.type = type;
    }

    /**
     * One builder per type, ready to pass to AggregatedDiscoveryService.
     * @param types resource classes to serve, e.g. Cluster.class
     * @param <D> client details type
     * @return config builders
     */
    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    public static <D> ImmutableList<IncrementalConfigBuilder<?, SnapshotUpdate<D>, D>> forTypes(Class<? extends Message>... types) {
        return Arrays.stream(types).map(SnapshotConfigBuilder::<D>builderFor).collect(ImmutableList.toImmutableList());
    }

    private static <D> IncrementalConfigBuilder<?, SnapshotUpdate<D>, D> builderFor(Class<? extends Message> type) {
        return new SnapshotConfigBuilder<>(type);
    }

    @Override
    public Response<R> addOrder(SnapshotUpdate<D> diff, Predicate<String> resourceInSubListChange, D clientDetails) {
        List<NamedMessage<R>> addAndUpdates = new ArrayList<>();
        for (String name : diff.candidates(type, clientDetails)) {
            if (!resourceInSubListChange.test(name)) {
                continue;
            }
            R after = diff.after(type, name, clientDetails);
            R before = diff.before(type, name, clientDetails);
            if (after != null && !after.equals(before)) { // protobuf equals() checks identity first, so shared resources are cheap
                addAndUpdates.add(new NamedMessage<>(name, after));
            }
        }
        return new Response<>(addAndUpdates, List.of());
    }

    @Override
    public Response<R> removeOrder(SnapshotUpdate<D> diff, Predicate<String> resourceInSubListChange, D clientDetails) {
        List<String> removes = new ArrayList<>();
        for (String name : diff.candidates(type, clientDetails)) {
            if (resourceInSubListChange.test(name)
                && diff.after(type, name, clientDetails) == null
                && diff.before(type, name, clientDetails) != null) {
                removes.add(name);
            }
        }
        return new Response<>(List.of(), removes);
    }

    /** Mid-change state: everything new is in, nothing old has been removed yet. */
    @Override
    public Resources<R> getResourcesAddOrder(SnapshotUpdate<D> services, Predicate<String> resourceInSubListChange, D clientDetails) {
        Set<String> names = new HashSet<>(services.namesBefore(type, clientDetails));
        names.addAll(services.namesAfter(type, clientDetails));
        List<NamedMessage<R>> resources = new ArrayList<>();
        for (String name : names) {
            if (!resourceInSubListChange.test(name)) {
                continue;
            }
            R value = services.after(type, name, clientDetails);
            // Named before or after, so one of them has it.
            resources.add(new NamedMessage<>(name, value != null ? value : Preconditions.checkNotNull(services.before(type, name, clientDetails))));
        }
        return new Resources<>(resources);
    }

    @Override
    public Resources<R> getResourcesRemoveOrder(SnapshotUpdate<D> services, Predicate<String> resourceInSubListChange, D clientDetails) {
        List<NamedMessage<R>> resources = new ArrayList<>();
        for (String name : services.namesAfter(type, clientDetails)) {
            if (resourceInSubListChange.test(name)) {
                resources.add(new NamedMessage<>(name, Preconditions.checkNotNull(services.after(type, name, clientDetails))));
            }
        }
        return new Resources<>(resources);
    }

    @Override
    public Class<R> handlesType() {
        return type;
    }
}
