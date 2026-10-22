package net.woaf.jono.envoy.xds;

import lombok.Builder;
import lombok.Value;

/**
 * This represents the metadata and configuration of the controlplane for a specific client.
 */
@Value
@Builder
public class XdsConfig<DetailsT> {

    /**
     * Whatever details about the client and configuration that you need to pass to your ConfigBuilders.
     */
    DetailsT clientDetails;

    /**
     * If set then we will delay sending mesh updates to the client until we have received the first ACK for this TypeUrl.
     * This allows us to prevent clients being sent endpoint updates, which interfere with the envoy init process, until
     * they have acked a later DS (for example RDS or LDS, which are only ACKed by envoy after clusters have been fully
     * initialised.)
     * If null then we don't delay the updates.
     * This is useful, for example, to work around bugs like https://github.com/envoyproxy/envoy/issues/16035 (we set this option to RDS in our own control plane for this reason.)
     */
    TypeUrl delayUpdatesUntilAckOf;

    /**
     * Set to true to log NACKs from this client at info rather than error, e.g. for developer laptops that may run out of
     * date Envoy versions. Either way we stop sending changes to a client once it has NACKed: retrying is spam, and
     * dropping the stream makes Envoy reconnect in a tight loop.
     */
    boolean silentNacks;
}
