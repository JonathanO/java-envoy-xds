package net.woaf.jono.envoy.xds.api.utils;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.jspecify.annotations.Nullable;
import net.woaf.jono.envoy.xds.api.Mergeable;
import net.woaf.jono.envoy.xds.api.StateBacklog;
import net.woaf.jono.envoy.xds.api.StateBacklogFactory;

/**
 * A StateBacklog implementation that merges Mergable state together.
 * @param <StateUpdT> State update type
 */
public class MergingStateBacklog<StateUpdT extends Mergeable<StateUpdT>> implements StateBacklog<StateUpdT> {

    @GuardedBy("this")
    private @Nullable StateUpdT backlog = null;

    private MergingStateBacklog() {
    }

    @Override
    public synchronized boolean isEmpty() {
        return backlog == null;
    }

    @Override
    public synchronized void put(StateUpdT update) {
        if (backlog == null) {
            backlog = update;
        } else {
            backlog = backlog.merge(update);
        }
    }

    @Override
    public synchronized @Nullable StateUpdT take() {
        StateUpdT value = backlog;
        backlog = null;
        return value;
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
