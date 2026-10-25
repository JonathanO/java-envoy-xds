package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.XdsConfig;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.type.v3.SemanticVersion;

/**
 * How to talk to one client.
 *
 * @param xdsConfig what the ClientConfigProvider said about it
 * @param bugClientSendsResourceListOnReconnectToWildcard If true then treat a list of resource subscriptions as a
 *     wildcard on the first request for a discovery service that supports wildcards, even though the spec says only an
 *     empty list should be handled that way. This works around a bug in envoy versions prior to 1.19.0 which sent a list
 *     of resources after reconnecting to an xDS, even when it wanted a wildcard subscription. This is safe since the xDS
 *     spec for the affected versions explicitly stated that envoy would always use wildcards for the discovery services
 *     that supported it.
 * @param <DetailsT> Client details type as returned by the ClientConfigProvider
 */
public record NodeConfig<DetailsT>(XdsConfig<DetailsT> xdsConfig, boolean bugClientSendsResourceListOnReconnectToWildcard) {

    public static <DetailsT> NodeConfig<DetailsT> forNode(Node node, XdsConfig<DetailsT> xdsConfig) {
        SemanticVersion clientVersion = node.getUserAgentBuildVersion().getVersion();
        boolean before119 = node.getUserAgentName().equals("envoy")
            && clientVersion.getMajorNumber() <= 1 && clientVersion.getMinorNumber() < 19;
        return new NodeConfig<>(xdsConfig, before119);
    }

}
