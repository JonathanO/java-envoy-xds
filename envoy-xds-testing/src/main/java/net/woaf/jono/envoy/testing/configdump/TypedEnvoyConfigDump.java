package net.woaf.jono.envoy.testing.configdump;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.admin.v3.BootstrapConfigDump;
import io.envoyproxy.envoy.admin.v3.ClustersConfigDump;
import io.envoyproxy.envoy.admin.v3.ConfigDump;
import io.envoyproxy.envoy.admin.v3.EndpointsConfigDump;
import io.envoyproxy.envoy.admin.v3.ListenersConfigDump;
import io.envoyproxy.envoy.admin.v3.RoutesConfigDump;
import io.envoyproxy.envoy.admin.v3.ScopedRoutesConfigDump;
import io.envoyproxy.envoy.admin.v3.SecretsConfigDump;

/**
 * The typed sections of an Envoy config dump. A section Envoy left out is empty.
 */
public record TypedEnvoyConfigDump(
    BootstrapConfigDump bootstrapConfigDump,
    ClustersConfigDump clustersConfigDump,
    EndpointsConfigDump endpointsConfigDump,
    ListenersConfigDump listenersConfigDump,
    ScopedRoutesConfigDump scopedRoutesConfigDump,
    RoutesConfigDump routesConfigDump,
    SecretsConfigDump secretsConfigDump) {

    public static TypedEnvoyConfigDump of(ConfigDump dump) throws InvalidProtocolBufferException {
        return new TypedEnvoyConfigDump(
            section(dump, BootstrapConfigDump.getDefaultInstance()),
            section(dump, ClustersConfigDump.getDefaultInstance()),
            section(dump, EndpointsConfigDump.getDefaultInstance()),
            section(dump, ListenersConfigDump.getDefaultInstance()),
            section(dump, ScopedRoutesConfigDump.getDefaultInstance()),
            section(dump, RoutesConfigDump.getDefaultInstance()),
            section(dump, SecretsConfigDump.getDefaultInstance()));
    }

    @SuppressWarnings("unchecked") // unpack returns the class it's given
    private static <T extends Message> T section(ConfigDump dump, T empty) throws InvalidProtocolBufferException {
        for (Any any : dump.getConfigsList()) {
            if (any.is(empty.getClass())) {
                return (T) any.unpack(empty.getClass());
            }
        }
        return empty;
    }
}
