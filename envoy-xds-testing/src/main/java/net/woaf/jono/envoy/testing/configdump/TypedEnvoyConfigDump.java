package net.woaf.jono.envoy.testing.configdump;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.envoyproxy.envoy.admin.v3.BootstrapConfigDump;
import io.envoyproxy.envoy.admin.v3.ClustersConfigDump;
import io.envoyproxy.envoy.admin.v3.ConfigDump;
import io.envoyproxy.envoy.admin.v3.EndpointsConfigDump;
import io.envoyproxy.envoy.admin.v3.ListenersConfigDump;
import io.envoyproxy.envoy.admin.v3.RoutesConfigDump;
import io.envoyproxy.envoy.admin.v3.ScopedRoutesConfigDump;
import io.envoyproxy.envoy.admin.v3.SecretsConfigDump;
import lombok.Getter;

/**
 * The typed sections of an Envoy config dump. A section Envoy left out is empty.
 */
@Getter
public class TypedEnvoyConfigDump {

    private BootstrapConfigDump bootstrapConfigDump = BootstrapConfigDump.getDefaultInstance();
    private ClustersConfigDump clustersConfigDump = ClustersConfigDump.getDefaultInstance();
    private EndpointsConfigDump endpointsConfigDump = EndpointsConfigDump.getDefaultInstance();
    private ListenersConfigDump listenersConfigDump = ListenersConfigDump.getDefaultInstance();
    private ScopedRoutesConfigDump scopedRoutesConfigDump = ScopedRoutesConfigDump.getDefaultInstance();
    private RoutesConfigDump routesConfigDump = RoutesConfigDump.getDefaultInstance();
    private SecretsConfigDump secretsConfigDump = SecretsConfigDump.getDefaultInstance();

    public TypedEnvoyConfigDump(ConfigDump dump) throws InvalidProtocolBufferException {
        for (Any any : dump.getConfigsList()) {
            if (any.is(BootstrapConfigDump.class)) {
                bootstrapConfigDump = any.unpack(BootstrapConfigDump.class);
            }
            if (any.is(ClustersConfigDump.class)) {
                clustersConfigDump = any.unpack(ClustersConfigDump.class);
            }
            if (any.is(EndpointsConfigDump.class)) {
                endpointsConfigDump = any.unpack(EndpointsConfigDump.class);
            }
            if (any.is(ListenersConfigDump.class)) {
                listenersConfigDump = any.unpack(ListenersConfigDump.class);
            }
            if (any.is(ScopedRoutesConfigDump.class)) {
                scopedRoutesConfigDump = any.unpack(ScopedRoutesConfigDump.class);
            }
            if (any.is(RoutesConfigDump.class)) {
                routesConfigDump = any.unpack(RoutesConfigDump.class);
            }
            if (any.is(SecretsConfigDump.class)) {
                secretsConfigDump = any.unpack(SecretsConfigDump.class);
            }
        }
    }
}
