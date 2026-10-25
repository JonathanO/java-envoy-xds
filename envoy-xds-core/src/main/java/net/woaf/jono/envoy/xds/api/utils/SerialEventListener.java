package net.woaf.jono.envoy.xds.api.utils;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hands updates to a listener on an executor, one at a time and in order, so a {@link
 * net.woaf.jono.envoy.xds.api.ClusterEventSource} can broadcast without calling into a stream itself. Many listeners
 * can share one executor: each runs in order, and different listeners run in parallel.
 *
 * <p>There's no merging here. A stream that's busy with a push already keeps later updates in its StateBacklog, so
 * choose MergingStateBacklog or QueueingStateBacklog there.
 *
 * <p>Updates already handed over are still delivered after {@link #close()}, then the listener is closed. If the
 * listener throws, it gets no more updates and is closed.
 * @param <StateUpdT> update type
 */
public final class SerialEventListener<StateUpdT> implements ClusterManagerEventListener<StateUpdT> {

    private static final Logger log = LoggerFactory.getLogger(SerialEventListener.class);

    private final Executor serial;

    private final ClusterManagerEventListener<StateUpdT> delegate;

    private volatile boolean closed = false;

    // Only touched by tasks on serial, which run one at a time and see each other's writes.
    private boolean delegateClosed = false;

    /**
     * Deliver on the shared executor, see {@link #sharedExecutor()}.
     * @param delegate the listener to deliver to
     */
    public SerialEventListener(ClusterManagerEventListener<StateUpdT> delegate) {
        this(sharedExecutor(), delegate);
    }

    /**
     * Deliver on your own executor.
     * @param executor runs the deliveries, never more than one at a time for this listener
     * @param delegate the listener to deliver to
     */
    public SerialEventListener(Executor executor, ClusterManagerEventListener<StateUpdT> delegate) {
        this.serial = MoreExecutors.newSequentialExecutor(executor);
        this.delegate = delegate;
    }

    /**
     * {@return the executor listeners share by default: one daemon thread per core}
     * Delivering an update runs your config builders and hands their messages to gRPC, which queues writes rather than
     * blocking. Ours never wait, so more threads than cores wouldn't help. If your config builders block, e.g. on a
     * database, pass your own executor instead.
     */
    public static Executor sharedExecutor() {
        return Shared.EXECUTOR;
    }

    private static final class Shared {
        static final Executor EXECUTOR = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setNameFormat("xds-update-%d").setDaemon(true).build());
    }

    @Override
    public void onNetworkChange(StateUpdT diff) {
        if (closed) {
            return;
        }
        serial.execute(() -> deliver(diff));
    }

    private void deliver(StateUpdT diff) {
        if (delegateClosed) {
            return;
        }
        try {
            delegate.onNetworkChange(diff);
        } catch (RuntimeException e) {
            log.error("Listener failed, so it gets no more updates:", e);
            closeDelegate();
        }
    }

    private void closeDelegate() {
        if (!delegateClosed) {
            delegateClosed = true;
            delegate.close();
        }
    }

    @Override
    public void close() {
        closed = true;
        serial.execute(this::closeDelegate);
    }
}
