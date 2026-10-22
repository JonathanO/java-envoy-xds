package net.woaf.jono.envoy.xds.api.utils;

import com.google.rpc.Status;
import io.envoyproxy.envoy.config.core.v3.Node;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.ClientHandle;
import net.woaf.jono.envoy.xds.api.XdsEventListener;

/**
 * Resyncs an Envoy a while after it NACKs, so a fixed config gets through without a reconnect loop. If it NACKs again
 * after the resync, the next wait doubles, up to maxDelay. Once a node goes 2 * maxDelay without a NACK, it starts
 * again from initialDelay. Add it to your XdsEventListeners.
 *
 * <p>While it waits, that Envoy gets no changes, so maxDelay is the longest you're willing to have it serve stale
 * config, e.g. endpoints for pods that have gone.
 * @param <DetailsT> details type
 */
public class ResyncAfterNack<DetailsT> implements XdsEventListener<DetailsT> {

    private final ScheduledExecutorService scheduler;
    private final Duration initialDelay;
    private final Duration maxDelay;

    private final Map<ClientHandle, ScheduledFuture<?>> pending = new ConcurrentHashMap<>();

    /** By node id rather than handle: a resync is a new stream, so a new handle. */
    private final Map<String, Backoff> backoffs = new ConcurrentHashMap<>();

    private record Backoff(Duration delay, Instant lastNack) {
    }

    /**
     * See class description.
     * @param scheduler runs the resyncs
     * @param initialDelay wait before the first resync
     * @param maxDelay longest wait
     */
    public ResyncAfterNack(ScheduledExecutorService scheduler, Duration initialDelay, Duration maxDelay) {
        this.scheduler = scheduler;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
    }

    @Override
    public void onNewClient(ClientHandle clientHandle, Node node, XdsConfig<DetailsT> config) {
    }

    @Override
    public void onClientDisconnected(ClientHandle clientHandle, Node node) {
        ScheduledFuture<?> resync = pending.remove(clientHandle);
        if (resync != null) {
            resync.cancel(false);
        }
    }

    @Override
    public void onClientNack(ClientHandle clientHandle, Node node, TypeUrl typeUrl, Status errorDetail) {
        // Only the first NACK on a stream schedules a resync, and moves the backoff on.
        pending.computeIfAbsent(clientHandle, handle -> {
            Duration delay = nextDelay(node.getId());
            return scheduler.schedule(() -> {
                pending.remove(handle);
                handle.resync();
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    private Duration nextDelay(String nodeId) {
        Instant now = Instant.now();
        return backoffs.compute(nodeId, (id, last) -> {
            if (last == null || last.lastNack().plus(maxDelay.multipliedBy(2)).isBefore(now)) {
                return new Backoff(initialDelay, now);
            }
            Duration doubled = last.delay().multipliedBy(2);
            return new Backoff(doubled.compareTo(maxDelay) > 0 ? maxDelay : doubled, now);
        }).delay();
    }
}
