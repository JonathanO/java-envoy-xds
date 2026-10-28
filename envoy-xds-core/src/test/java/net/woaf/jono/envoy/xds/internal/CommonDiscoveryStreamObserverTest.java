package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import com.google.common.collect.ImmutableList;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceManagerTest.DummyUpdate;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CommonDiscoveryStreamObserverTest {

    private static final Object DETAILS = new Object();

    @Test
    public void requestsAfterAResyncAreIgnored(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                               @Mock ClusterEventSource<DummyUpdate> source,
                                               @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                               @Mock DiscoveryServiceManager<DiscoveryRequest, DummyUpdate> dsm) {
        when(factory.build(any(StreamObserver.class), any(XdsConfig.class), any(DiscoveryServiceManagerMetrics.class))).thenReturn(dsm);
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer = observer(responseObserver, source, factory);
        DiscoveryRequest cds = DiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy")).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build();
        observer.onNext(cds);

        observer.resync();
        observer.onNext(cds); // Envoy sent it before it saw the stream close

        verify(dsm).processUpdate(any());
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @Test
    public void testUnknownTypeIsIgnoredAndStreamStaysUp(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                         @Mock ClusterEventSource<DummyUpdate> source,
                                                         @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                                         @Mock DiscoveryServiceManager<DiscoveryRequest, DummyUpdate> dsm) {
        when(factory.build(any(StreamObserver.class), any(XdsConfig.class), any(DiscoveryServiceManagerMetrics.class))).thenReturn(dsm);
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer = observer(responseObserver, source, factory);
        Node node = Node.newBuilder().setId("envoy").build();

        // e.g. ECDS, which we don't serve.
        observer.onNext(DiscoveryRequest.newBuilder().setNode(node).setTypeUrl("type.googleapis.com/envoy.config.core.v3.TypedExtensionConfig").build());
        observer.onNext(DiscoveryRequest.newBuilder().setNode(node).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());

        verify(responseObserver, never()).onError(any());
        verify(dsm).processUpdate(any());
    }

    @Test
    public void subscribesWithTheClientsDetails(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                @Mock ClusterEventSource<DummyUpdate> source,
                                                @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                                @Mock DiscoveryServiceManager<DiscoveryRequest, DummyUpdate> dsm) {
        when(factory.build(any(StreamObserver.class), any(XdsConfig.class), any(DiscoveryServiceManagerMetrics.class))).thenReturn(dsm);
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer = observer(responseObserver, source, factory);

        observer.onNext(DiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy")).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());

        verify(source).subscribe(observer, DETAILS);
    }

    @Test
    public void failedPushClosesTheStreamWithoutRethrowing(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                           @Mock ClusterEventSource<DummyUpdate> source,
                                                           @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                                           @Mock DiscoveryServiceManager<DiscoveryRequest, DummyUpdate> dsm) {
        when(factory.build(any(StreamObserver.class), any(XdsConfig.class), any(DiscoveryServiceManagerMetrics.class))).thenReturn(dsm);
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer = observer(responseObserver, source, factory);
        observer.onNext(DiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy")).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());
        IllegalStateException failure = new IllegalStateException("config builder failed");
        doThrow(failure).when(dsm).pushUpdates(any());

        // Logged here, so a rethrow would only get it logged again by the source.
        observer.onNetworkChange(new DummyUpdate());

        verify(responseObserver).onError(failure);
        verify(source).unsubscribe(observer);
    }

    private static CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer(
        StreamObserver<DiscoveryResponse> responseObserver, ClusterEventSource<DummyUpdate> source,
        DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory) {
        return new CommonDiscoveryStreamObserver<>(
            responseObserver,
            source,
            factory,
            r -> new CommonDiscoveryRequest<DiscoveryRequest>(r.getNode(), r.getTypeUrl(), r, null),
            node -> XdsConfig.of(DETAILS),
            ImmutableList.of(),
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );
    }
}
