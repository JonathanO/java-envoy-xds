package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.TypeUrl;
import java.util.function.BooleanSupplier;

import com.google.protobuf.Message;

/**
 * Handles communication with a specific envoy client for a specific Discovery Service type.
 *
 * @param <RequestT> the DiscoveryRequest or DeltaDiscoveryRequest message type.
 */
public interface DiscoveryService<RequestT extends Message, StateUpdT> {

    enum SubState {
        COMPLETED,
        PRE,
        POST
    }

    /**
     * Called when envoy either acknowledges an outstanding update, or sends a new xDS request.
     * init must have be called before this method is called for the first time.
     * @param value the request sent by envoy.
     */
    void processUpdate(CommonDiscoveryRequest<RequestT> value);

    /**
     * Tells if it is envoy's turn in sending an ack for an outstanding update.
     *
     * @return true if an ack is outstanding.
     */
    boolean awaitingAck();

    /**
     * Called when the DiscoveryServiceManager is initialized.
     *
     * <p>The DSM is initialized the first time any message from envoy is received, regardless of which DiscoveryService will handle the message.</p>
     *
     * <p>The state passed can either be a genuinely full state (with only added services),
     * or a differential state (from which the full state can still be reconstructed).</p>
     */
    void init(StateUpdT state);

    /**
     * Stores the changes to be sent during the following sendNetworkUpdatePre/Post calls.
     * init must have be called before this method is called for the first time.
     */
    void onNetworkUpdate(StateUpdT changes);

    /**
     * Sends parts of the previously stored changes applicable during the preorder round.
     * These are typically the additions.
     */
    void sendNetworkUpdatePre();

    /**
     * Sends parts of the previously stored changes applicable during the postorder round.
     * These are typically removals.
     */
    void sendNetworkUpdatePost();

    /**
     * Drops whatever is left of the change from onNetworkUpdate, so nothing more of it is sent. Called when Envoy
     * NACKs: later answers to subscription changes cover only the names they add.
     */
    void abandonChange();

    /**
     * Says whether removals discovered from a reconnecting delta client's initial_resource_versions must wait, for the
     * manager to send in ADS remove order. Asked when Envoy subscribes; removals it holds are sent later with
     * sendDeferredReconnectRemovals().
     */
    default void setDeferReconnectRemovals(BooleanSupplier deferReconnectRemovals) {
    }

    /**
     * Whether we have config for this type, rather than standing in for a type nobody configured.
     */
    default boolean isServed() {
        return true;
    }

    /**
     * Whether this service has sent Envoy anything on this stream.
     */
    default boolean hasAnswered() {
        return false;
    }

    /**
     * Returns true if this service has removals discovered from a reconnecting delta client's
     * initial_resource_versions that still need to be sent in ADS remove order.
     */
    default boolean hasDeferredReconnectRemovals() {
        return false;
    }

    /**
     * Sends pending removals discovered from a reconnecting delta client's initial_resource_versions.
     */
    default void sendDeferredReconnectRemovals() {
    }

    /**
     * Identifies the specific Discover Service type this instance handles.
     */
    TypeUrl getTypeUrl();
}
