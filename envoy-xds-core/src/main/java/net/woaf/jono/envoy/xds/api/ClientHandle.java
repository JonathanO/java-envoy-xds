package net.woaf.jono.envoy.xds.api;

/**
 * One connected Envoy stream. See {@link XdsEventListener}.
 */
public interface ClientHandle {

    /**
     * Close this stream, so Envoy reconnects and syncs from scratch. Its first request on the new stream tells us
     * exactly what config it has. Use this to unfreeze an Envoy that NACKed, once you think its config is fixed: see
     * {@link XdsEventListener#onClientNack} and {@link net.woaf.jono.envoy.xds.api.utils.ResyncAfterNack}. Safe to
     * call from any thread, and does nothing if the stream has already closed.
     */
    void resync();
}
