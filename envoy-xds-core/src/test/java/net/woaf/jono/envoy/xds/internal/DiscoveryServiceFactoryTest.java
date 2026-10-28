package net.woaf.jono.envoy.xds.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceManagerTest.DummyUpdate;
import net.woaf.jono.envoy.xds.internal.sotw.SotwDiscoveryServiceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DiscoveryServiceFactoryTest {

    @Test
    public void onlyTheTypesWithAConfigBuilderAreServed(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusters,
                                                        @Mock StreamObserver<DiscoveryResponse> responseObserver) {
        when(clusters.handlesType()).thenReturn(Cluster.class);
        SotwDiscoveryServiceFactory<DummyUpdate, Object> factory = new SotwDiscoveryServiceFactory<>(ImmutableList.of(clusters));

        Map<TypeUrl, DiscoveryService<DiscoveryRequest, DummyUpdate>> services = factory.createAll(responseObserver, XdsConfig.of(new Object()));

        assertThat(services).hasSize(TypeUrl.values().length);
        services.forEach((type, service) -> assertThat(service.isServed()).as("%s served", type).isEqualTo(type == TypeUrl.CDS));
    }

    @Test
    public void configBuilderForATypeWeDoNotKnowIsRejected(@Mock IncrementalConfigBuilder<Node, DummyUpdate, Object> nodes) {
        when(nodes.handlesType()).thenReturn(Node.class);

        assertThatThrownBy(() -> new SotwDiscoveryServiceFactory<>(ImmutableList.of(nodes)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("is not a known ADS message type");
    }
}
