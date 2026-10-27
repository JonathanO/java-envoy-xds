package net.woaf.jono.envoy.xds.api;

/**
 * A state update that can be combined with the next one, so a connection that falls behind sends one update, not many.
 *
 * @param <StateUpdT> the implementing type
 */
public interface Mergeable<StateUpdT extends Mergeable<StateUpdT>> {

    /**
     * Merge an update into this instance.
     * The result of the merge operation should be a state update equivalent to applying this instance followed by the supplied update.
     * @param update updated state
     * @return a StateUpdT representing the result of applying this instance and then update
     */
    StateUpdT merge(StateUpdT update);

    /**
     * Whether a later update may be merged into this one. If not, a client gets this one, and ACKs it, before anything
     * published after it, e.g. to order changes that make before break can't see.
     * @return false to keep later updates out of this one
     */
    default boolean mergesWithLater() {
        return true;
    }

}
