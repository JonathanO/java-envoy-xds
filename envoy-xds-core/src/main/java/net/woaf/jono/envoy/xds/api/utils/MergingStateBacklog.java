package net.woaf.jono.envoy.xds.api.utils;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.jspecify.annotations.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import net.woaf.jono.envoy.xds.api.Mergeable;
import net.woaf.jono.envoy.xds.api.StateBacklog;
import net.woaf.jono.envoy.xds.api.StateBacklogFactory;

/**
 * A StateBacklog implementation that merges Mergable state together. An update that doesn't
 * {@linkplain Mergeable#mergesWithLater() merge with later ones} is taken on its own, and those after it are merged after it.
 * @param <StateUpdT> State update type
 */
public class MergingStateBacklog<StateUpdT extends Mergeable<StateUpdT>> implements StateBacklog<StateUpdT> {

    @GuardedBy("this")
    private final Deque<StateUpdT> backlog = new ArrayDeque<>();

    private MergingStateBacklog() {
    }

    @Override
    public synchronized boolean isEmpty() {
        return backlog.isEmpty();
    }

    @Override
    public synchronized void put(StateUpdT update) {
        StateUpdT last = backlog.peekLast();
        if (last != null && last.mergesWithLater()) {
            backlog.pollLast();
            backlog.addLast(last.merge(update));
        } else {
            backlog.addLast(update);
        }
    }

    @Override
    public synchronized @Nullable StateUpdT take() {
        return backlog.pollFirst();
    }

    /**
     * {@return a factory of backlogs that merges updates, for AggregatedDiscoveryService}
     * @param <StateUpdT> State update type
     */
    public static <StateUpdT extends Mergeable<StateUpdT>> MergingStateBacklogFactory<StateUpdT> factory() {
        return new MergingStateBacklogFactory<>();
    }

    /**
     * Builds a MergingStateBacklog per connection.
     *
     * @param <StateUpdT> State update type
     */
    public static final class MergingStateBacklogFactory<StateUpdT extends Mergeable<StateUpdT>> implements StateBacklogFactory<StateUpdT> {

        private MergingStateBacklogFactory() {
        }

        @Override
        public MergingStateBacklog<StateUpdT> build() {
            return new MergingStateBacklog<>();
        }
    }
}
