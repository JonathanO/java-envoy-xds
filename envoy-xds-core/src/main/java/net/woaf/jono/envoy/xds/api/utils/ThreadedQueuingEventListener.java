package net.woaf.jono.envoy.xds.api.utils;

import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedQueuingEventListener<StateUpdT> implements ClusterManagerEventListener<StateUpdT> {

    private static final Logger log = LoggerFactory.getLogger(ThreadedQueuingEventListener.class);

    public interface QueueTakeStrategy<StateUpdT> {
        StateUpdT take(BlockingQueue<StateUpdT> queue) throws InterruptedException;
    }

    private final ProcessingThread<StateUpdT> processorThread;

    private static final AtomicLong threadNum = new AtomicLong(0);


    private static class ProcessingThread<StateUpdT> extends Thread {

        private final BlockingQueue<StateUpdT> queue = new LinkedBlockingQueue<>();

        private final ClusterManagerEventListener<StateUpdT> delegate;

        private final QueueTakeStrategy<StateUpdT> strategy;

        private volatile boolean stopping = false;

        ProcessingThread(QueueTakeStrategy<StateUpdT> strategy, ClusterManagerEventListener<StateUpdT> delegate) {
            this.setName("state-update-event-listener-" + threadNum.getAndIncrement());
            this.delegate = delegate;
            this.strategy = strategy;
        }

        @Override
        public void run() {
            try {
                while (!stopping || !queue.isEmpty()) {
                    try {
                        delegate.onNetworkChange(strategy.take(queue));
                    } catch (InterruptedException e) {
                        stopping = true;
                    }
                }
            } catch (Throwable t) {
                log.error("QueueingMergingEventListener dying of error:", t);
            } finally {
                delegate.close();
            }
            log.debug("QueueingMergingEventListener terminating");
        }

        void onNetworkChange(StateUpdT diff) {
            if (stopping) {
                return;
            }
            queue.add(diff);
        }
    }

    public ThreadedQueuingEventListener(QueueTakeStrategy<StateUpdT> strategy, ClusterManagerEventListener<StateUpdT> delegate) {
        processorThread = new ProcessingThread<>(strategy, delegate);
    }

    public void start() {
        processorThread.start();
    }

    @Override
    public void onNetworkChange(StateUpdT diff) {
        processorThread.onNetworkChange(diff);
    }

    @Override
    public void close() {
        processorThread.interrupt();
    }

    public void waitUntilFinished() {
        try {
            processorThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

}
