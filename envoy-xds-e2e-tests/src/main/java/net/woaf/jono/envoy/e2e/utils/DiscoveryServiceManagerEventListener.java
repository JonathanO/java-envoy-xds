package net.woaf.jono.envoy.e2e.utils;

import net.woaf.jono.envoy.testing.Settled;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DiscoveryServiceManagerEventListener implements DiscoveryServiceManagerMetrics, Settled.Hook {

    private final AtomicBoolean pushInProgress = new AtomicBoolean(false);
    private final AtomicLong pushed = new AtomicLong(0);
    private final AtomicLong messagesOutstanding = new AtomicLong(0);

    // Between one push completing and the next beginning, both of the above read idle for an instant.
    private static final long QUIET_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private final AtomicLong lastEventNanos = new AtomicLong(System.nanoTime());

    @Override
    public void close() {
        onPushComplete();
    }

    @Override
    public void onPushBegin() {
        lastEventNanos.set(System.nanoTime());
        if (pushInProgress.getAndSet(true)) {
            throw new IllegalStateException("Multiple pushes in progress");
        }
    }

    @Override
    public void onPushComplete() {
        lastEventNanos.set(System.nanoTime());
        if (pushInProgress.getAndSet(false)) {
            pushed.incrementAndGet();
        }
    }

    @Override
    public void onAwaitingAck() {
        lastEventNanos.set(System.nanoTime());
        messagesOutstanding.incrementAndGet();
    }

    @Override
    public void onMessageAcked() {
        lastEventNanos.set(System.nanoTime());
        messagesOutstanding.decrementAndGet();
    }

    public boolean isPushInProgress() {
        return pushInProgress.get();
    }

    public boolean awaitingAcks() {
        return messagesOutstanding.get() != 0;
    }

    @Override
    public boolean isIdle() {
        return !isPushInProgress() && !awaitingAcks() && System.nanoTime() - lastEventNanos.get() >= QUIET_NANOS;
    }

    public long getPushed() {
        return pushed.get();
    }
}
