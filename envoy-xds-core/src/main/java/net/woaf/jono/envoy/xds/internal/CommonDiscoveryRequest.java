package net.woaf.jono.envoy.xds.internal;

import org.jspecify.annotations.Nullable;
import com.google.protobuf.Message;
import com.google.rpc.Status;
import io.envoyproxy.envoy.config.core.v3.Node;

/**
 * Shared metadata between DiscoveryRequest and DeltaDiscoveryRequest, a missing "superclass".
 *
 * @param node the client, as it describes itself
 * @param typeUrl the requested type
 * @param message the request itself
 * @param errorDetail why the client rejected our last response, if it did
 * @param <RequestT> DiscoveryRequest or DeltaDiscoveryRequest
 */
public record CommonDiscoveryRequest<RequestT extends Message>(Node node, String typeUrl, RequestT message, @Nullable Status errorDetail) {
}
