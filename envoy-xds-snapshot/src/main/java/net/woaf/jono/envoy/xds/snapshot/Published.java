package net.woaf.jono.envoy.xds.snapshot;

import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * A snapshot as published by a SnapshotSource, plus each client's overlay on it.
 * Overlays are computed once per distinct client details and shared by every client with equal details.
 */
final class Published<D> {

    private static final Object NULL_DETAILS = new Object();

    private static final Published<?> NONE = new Published<>(Snapshot.empty(), null);

    private final Snapshot base;

    @Nullable
    private final BiFunction<Snapshot, ? super D, Snapshot> overlayFunction;

    private final Map<Object, Snapshot> overlays = new ConcurrentHashMap<>();

    Published(Snapshot base, @Nullable BiFunction<Snapshot, ? super D, Snapshot> overlayFunction) {
        this.base = base;
        this.overlayFunction = overlayFunction;
    }

    /** What a client has before it has been sent anything. */
    @SuppressWarnings("unchecked")
    static <D> Published<D> none() {
        return (Published<D>) NONE;
    }

    Snapshot base() {
        return base;
    }

    Snapshot overlay(@Nullable D details) {
        if (overlayFunction == null) {
            return Snapshot.empty();
        }
        return overlays.computeIfAbsent(details == null ? NULL_DETAILS : details, k -> overlayFunction.apply(base, details));
    }

    /** The resource this client sees: its overlay's, else the base's. */
    @Nullable
    <R extends Message> R lookup(Class<R> type, String name, @Nullable D details) {
        R value = overlay(details).get(type).get(name);
        return value != null ? value : base.get(type).get(name);
    }

    Set<String> names(Class<? extends Message> type, @Nullable D details) {
        Set<String> names = new HashSet<>(base.get(type).keySet());
        names.addAll(overlay(details).get(type).keySet());
        return names;
    }
}
