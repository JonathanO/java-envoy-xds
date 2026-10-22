package net.woaf.jono.envoy.testing;

import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.ScopedRouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.Secret;
import io.envoyproxy.envoy.service.runtime.v3.Runtime;
import java.util.Map;
import java.util.Set;

/**
 * Type URLs of the xDS resource types.
 */
public final class XdsTypes {

    private static final String PREFIX = "type.googleapis.com/";

    public static final String LDS = PREFIX + Listener.getDescriptor().getFullName();
    public static final String RDS = PREFIX + RouteConfiguration.getDescriptor().getFullName();
    public static final String SRDS = PREFIX + ScopedRouteConfiguration.getDescriptor().getFullName();
    public static final String VHDS = PREFIX + VirtualHost.getDescriptor().getFullName();
    public static final String CDS = PREFIX + Cluster.getDescriptor().getFullName();
    public static final String EDS = PREFIX + ClusterLoadAssignment.getDescriptor().getFullName();
    public static final String SDS = PREFIX + Secret.getDescriptor().getFullName();
    public static final String RTDS = PREFIX + Runtime.getDescriptor().getFullName();

    private static final Set<String> WILDCARD = Set.of(LDS, CDS);

    private static final Map<String, Class<? extends Message>> CLASSES = Map.of(
        LDS, Listener.class, RDS, RouteConfiguration.class, SRDS, ScopedRouteConfiguration.class, VHDS, VirtualHost.class,
        CDS, Cluster.class, EDS, ClusterLoadAssignment.class, SDS, Secret.class, RTDS, Runtime.class);

    private XdsTypes() {
    }

    /**
     * Whether Envoy subscribes to this type as a wildcard (all resources) by default.
     * @param typeUrl xDS type URL
     * @return true for LDS and CDS
     */
    public static boolean isWildcard(String typeUrl) {
        return WILDCARD.contains(typeUrl);
    }

    /**
     * Resource class for a type URL.
     * @param typeUrl xDS type URL
     * @return the class, or null if it isn't an xDS resource type we know
     */
    public static Class<? extends Message> messageClass(String typeUrl) {
        return CLASSES.get(typeUrl);
    }
}
