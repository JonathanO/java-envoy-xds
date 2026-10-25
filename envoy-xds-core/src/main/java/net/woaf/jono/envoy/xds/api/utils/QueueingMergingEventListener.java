package net.woaf.jono.envoy.xds.api.utils;

import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import net.woaf.jono.envoy.xds.api.Mergeable;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

/**
 * An event listener that accepts network changes, and queues them to be merged and executed asynchronously against a delegate.
 * This is subtly different to the plain QueueingEventListener, in that it will drain the queue and merge everything it finds together.
 * It will attempt to drain the queue fully after close() is called.
 * @param <StateUpdT> Update type
 */
public class QueueingMergingEventListener<StateUpdT extends Mergeable<StateUpdT>> extends ThreadedQueuingEventListener<StateUpdT> {

    public QueueingMergingEventListener(ClusterManagerEventListener<StateUpdT> delegate) {
        super(new MergingStrategy<>(), delegate);
    }

    public static class MergingStrategy<StateUpdT extends Mergeable<StateUpdT>> implements QueueTakeStrategy<StateUpdT> {

        @Override
        public StateUpdT take(BlockingQueue<StateUpdT> queue) throws InterruptedException {
            StateUpdT combined = queue.take();
            ArrayList<StateUpdT> updates = new ArrayList<>();
            queue.drainTo(updates);
            for (StateUpdT update: updates) {
                combined = combined.merge(update);
            }
            return combined;
        }
    }

    public static <StateUpdT extends Mergeable<StateUpdT>> QueueingMergingEventListener<StateUpdT> createAndStart(ClusterManagerEventListener<StateUpdT> delegate) {
        QueueingMergingEventListener<StateUpdT> runner = new QueueingMergingEventListener<>(delegate);
        runner.start();
        return runner;
    }

}
