package net.woaf.jono.envoy.xds;

import com.google.common.collect.ImmutableList;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;
import net.woaf.jono.envoy.xds.DiscoveryServiceManagerTest.DummyUpdate;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CommonDiscoveryStreamObserverTest {

    @Test
    public void testUnknownTypeIsIgnoredAndStreamStaysUp(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                         @Mock ClusterEventSource<DummyUpdate> source,
                                                         @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                                         @Mock DiscoveryServiceManager<DiscoveryRequest, DummyUpdate> dsm) {
        when(factory.build(any(StreamObserver.class), any(NodeConfig.class), any(DiscoveryServiceManagerMetrics.class))).thenReturn(dsm);
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer = new CommonDiscoveryStreamObserver<>(
            responseObserver,
            source,
            factory,
            r -> CommonDiscoveryRequest.<DiscoveryRequest>builder().node(r.getNode()).typeUrl(r.getTypeUrl()).message(r).build(),
            node -> XdsConfig.builder().clientDetails(new Object()).build(),
            ImmutableList.of(),
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );
        Node node = Node.newBuilder().setId("envoy").build();

        // e.g. ECDS, which we don't serve.
        observer.onNext(DiscoveryRequest.newBuilder().setNode(node).setTypeUrl("type.googleapis.com/envoy.config.core.v3.TypedExtensionConfig").build());
        observer.onNext(DiscoveryRequest.newBuilder().setNode(node).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());

        verify(responseObserver, never()).onError(any());
        verify(dsm).processUpdate(any());
    }
}
