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
import net.woaf.jono.envoy.xds.api.XdsEventListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    @Test
    public void clientCompletingEndsTheStreamAndClosesItsManager(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                                 @Mock ClusterEventSource<DummyUpdate> source,
                                                                 @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                                                 @Mock DiscoveryServiceManager<DiscoveryRequest, DummyUpdate> dsm) {
        when(factory.build(any(StreamObserver.class), any(XdsConfig.class), any(DiscoveryServiceManagerMetrics.class))).thenReturn(dsm);
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer = observer(responseObserver, source, factory);
        observer.onNext(DiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy")).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());

        observer.onCompleted();

        verify(source).unsubscribe(observer);
        verify(dsm).close();
        verify(responseObserver).onCompleted();
    }

    @Test
    public void listenersErrorIsThrownWithLaterFailuresSuppressed(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                                  @Mock ClusterEventSource<DummyUpdate> source,
                                                                  @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                                                  @Mock XdsEventListener<Object> first,
                                                                  @Mock XdsEventListener<Object> second,
                                                                  @Mock XdsEventListener<Object> third) {
        Error firstFailure = new Error("first listener failed");
        Error secondFailure = new Error("second listener failed");
        RuntimeException thirdFailure = new IllegalStateException("third listener failed");
        doThrow(firstFailure).when(first).onNewClient(any(), any(), any());
        doThrow(secondFailure).when(second).onNewClient(any(), any(), any());
        doThrow(thirdFailure).when(third).onNewClient(any(), any(), any());
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer =
            observer(responseObserver, source, factory, ImmutableList.of(first, second, third));

        // Every listener hears of the client, and the first Error wins over what the others threw.
        assertThatThrownBy(() -> observer.onNext(DiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy")).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build()))
            .isSameAs(firstFailure)
            .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(secondFailure, thirdFailure));
        verify(responseObserver).onError(firstFailure);
    }

    @Test
    public void listenersErrorWinsOverAnEarlierException(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                         @Mock ClusterEventSource<DummyUpdate> source,
                                                         @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                                         @Mock XdsEventListener<Object> first,
                                                         @Mock XdsEventListener<Object> second,
                                                         @Mock XdsEventListener<Object> third) {
        RuntimeException firstFailure = new IllegalStateException("first listener failed");
        Error secondFailure = new Error("second listener failed");
        RuntimeException thirdFailure = new IllegalStateException("third listener failed");
        doThrow(firstFailure).when(first).onNewClient(any(), any(), any());
        doThrow(secondFailure).when(second).onNewClient(any(), any(), any());
        doThrow(thirdFailure).when(third).onNewClient(any(), any(), any());
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer =
            observer(responseObserver, source, factory, ImmutableList.of(first, second, third));

        assertThatThrownBy(() -> observer.onNext(DiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy")).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build()))
            .isSameAs(secondFailure)
            .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(firstFailure, thirdFailure));
    }

    @Test
    public void resyncFinishesClosingTheStreamWhenAStepFails(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                             @Mock ClusterEventSource<DummyUpdate> source,
                                                             @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                                             @Mock DiscoveryServiceManager<DiscoveryRequest, DummyUpdate> dsm) {
        when(factory.build(any(StreamObserver.class), any(XdsConfig.class), any(DiscoveryServiceManagerMetrics.class))).thenReturn(dsm);
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer = observer(responseObserver, source, factory);
        observer.onNext(DiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy")).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());
        IllegalStateException failure = new IllegalStateException("unsubscribe failed");
        doThrow(failure).when(source).unsubscribe(observer);

        assertThatThrownBy(observer::resync).as("the step's own exception, as it was").isSameAs(failure);
        verify(dsm).close();
        verify(responseObserver).onCompleted();
    }

    @Test
    public void listenersAreToldOfNacks(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                        @Mock ClusterEventSource<DummyUpdate> source,
                                        @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
                                        @Mock DiscoveryServiceManager<DiscoveryRequest, DummyUpdate> dsm,
                                        @Mock XdsEventListener<Object> listener) {
        when(factory.build(any(StreamObserver.class), any(XdsConfig.class), any(DiscoveryServiceManagerMetrics.class))).thenReturn(dsm);
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer =
            observer(responseObserver, source, factory, ImmutableList.of(listener));
        Node node = Node.newBuilder().setId("envoy").build();
        com.google.rpc.Status rejected = com.google.rpc.Status.newBuilder().setMessage("rejected").build();

        observer.onNext(DiscoveryRequest.newBuilder().setNode(node).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());
        verify(listener, never()).onClientNack(any(), any(), any(), any());
        observer.onNext(DiscoveryRequest.newBuilder().setNode(node).setTypeUrl(TypeUrl.CDS.getTypeUrl()).setResponseNonce("1").setErrorDetail(rejected).build());

        verify(listener).onClientNack(observer, node, TypeUrl.CDS, rejected);
    }

    @Test
    public void streamErrorsAreWarnedAboutUnlessTheClientCancelled(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                                   @Mock ClusterEventSource<DummyUpdate> source,
                                                                   @Mock DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory) {
        try (LoggedWarnings warnings = new LoggedWarnings(CommonDiscoveryStreamObserver.class)) {
            // Envoy going away cancels its stream: nothing to warn about.
            observer(responseObserver, source, factory).onError(io.grpc.Status.CANCELLED.asRuntimeException());
            assertThat(warnings.messages()).isEmpty();

            observer(responseObserver, source, factory).onError(io.grpc.Status.UNAVAILABLE.asRuntimeException());
            assertThat(warnings.messages()).containsExactly("Error streaming to an ADS client");
        }
    }

    private static CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer(
        StreamObserver<DiscoveryResponse> responseObserver, ClusterEventSource<DummyUpdate> source,
        DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory) {
        return observer(responseObserver, source, factory, ImmutableList.of());
    }

    private static CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer(
        StreamObserver<DiscoveryResponse> responseObserver, ClusterEventSource<DummyUpdate> source,
        DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
        ImmutableList<XdsEventListener<Object>> listeners) {
        return new CommonDiscoveryStreamObserver<>(
            responseObserver,
            source,
            factory,
            r -> new CommonDiscoveryRequest<DiscoveryRequest>(r.getNode(), r.getTypeUrl(), r, r.hasErrorDetail() ? r.getErrorDetail() : null),
            node -> XdsConfig.of(DETAILS),
            listeners,
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );
    }
}
