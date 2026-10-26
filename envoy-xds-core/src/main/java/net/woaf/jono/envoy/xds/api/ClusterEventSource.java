package net.woaf.jono.envoy.xds.api;

/**
 * The source of state updates for connected clients.
 *
 * <p>Threading: each client's listener holds its own lock while calling {@link #subscribe} and {@link #unsubscribe},
 * and takes that lock when it receives an update. So never call a listener while holding a lock that subscribe or
 * unsubscribe also take, or a client connecting while you broadcast deadlocks. A listener that fails during an update
 * unsubscribes itself from inside that call, so broadcasts must also tolerate unsubscribe while iterating listeners.
 * The simplest way to meet both is to wrap each listener in
 * {@link net.woaf.jono.envoy.xds.api.utils.SerialEventListener}, as {@code SnapshotSource} does, so
 * broadcasting only hands updates to an executor.</p>
 *
 * @param <StateUpdT> the state updates it publishes
 */
public interface ClusterEventSource<StateUpdT> {

    /**
     * Subscribe an event listener to this event source.
     * The event source is expected to hold a strong reference to the listener.
     * The event source must ensure that the initial state returned by this method is correctly sequenced with calls to the listener's onNetworkUpdate(),
     * i.e. you are responsible for ensuring that no network state changes are lost.
     * @param listener The listener who should receive future updates.
     * @return The last state update.
     */
    StateUpdT subscribe(ClusterManagerEventListener<StateUpdT> listener);

    /**
     * Subscribe a client's listener, telling the source which client it is, e.g. to pick the config it sees. The core
     * calls this; by default it ignores the details and calls {@link #subscribe(ClusterManagerEventListener)}.
     * @param listener The listener who should receive future updates.
     * @param clientDetails the client details your ClientConfigProvider returned for this client
     * @return The last state update.
     */
    default StateUpdT subscribe(ClusterManagerEventListener<StateUpdT> listener, Object clientDetails) {
        return subscribe(listener);
    }

    /**
     * Stop listening to this event source.
     * Implementations must call close() on the listener if the listener was subscribed to the event source, and must release any references to the listener.
     * This may be called with a listener that was never subscribed to the event source.
     * @param listener The instance that should stop receiving updates.
     */
    void unsubscribe(ClusterManagerEventListener<StateUpdT> listener);

}
