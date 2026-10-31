package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import com.google.common.collect.ImmutableList;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceManagerTest.DummyUpdate;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.api.XdsEventListener;
import java.util.Set;
import net.woaf.jono.envoy.xds.api.utils.QueueingStateBacklog;
import net.woaf.jono.envoy.xds.internal.sotw.SotwDiscoveryServiceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
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

        verify(responseObserver).onError(argThat(e -> isInternalError(e)));
        verify(source).unsubscribe(observer);
    }

    @Test
    public void badRequestEndsTheStreamWithInvalidArgument(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                                           @Mock ClusterEventSource<DummyUpdate> source) {
        when(source.subscribe(any(), any())).thenReturn(new DummyUpdate());
        StreamObserver<DeltaDiscoveryRequest> requests = ads(source, ImmutableList.of()).deltaAggregatedResources(responseObserver);

        // The xDS spec doesn't say which should win, so it's Envoy's mistake.
        assertThatThrownBy(() -> requests.onNext(DeltaDiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy"))
            .setTypeUrl(TypeUrl.CDS.getTypeUrl()).addResourceNamesSubscribe("a").addResourceNamesUnsubscribe("a").build()));

        verify(responseObserver).onError(argThat(e -> io.grpc.Status.fromThrowable(e).getCode() == io.grpc.Status.Code.INVALID_ARGUMENT
            && io.grpc.Status.fromThrowable(e).getDescription().contains("subscribe and unsubscribe from the same resource")));
    }

    @Test
    public void failedConfigBuilderEndsTheStreamWithAnInternalError(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                                                     @Mock ClusterEventSource<DummyUpdate> source,
                                                                     @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusters) {
        when(source.subscribe(any(), any())).thenReturn(new DummyUpdate());
        when(clusters.handlesType()).thenReturn(Cluster.class);
        IllegalStateException failure = new IllegalStateException("what went wrong inside");
        when(clusters.getResourcesRemoveOrder(any(), any(), any())).thenThrow(failure);
        StreamObserver<DiscoveryRequest> requests = ads(source, ImmutableList.of(clusters)).streamAggregatedResources(responseObserver);

        try (LoggedWarnings warnings = new LoggedWarnings(CommonDiscoveryStreamObserver.class)) {
            assertThatThrownBy(() -> requests.onNext(DiscoveryRequest.newBuilder().setNode(Node.newBuilder().setId("envoy"))
                .setTypeUrl(TypeUrl.CDS.getTypeUrl()).build())).isSameAs(failure);
            assertThat(warnings.messages()).as("logged here, as Envoy isn't told").containsExactly("Error streaming to an ADS client");
        }
        verify(responseObserver).onError(argThat(e -> isInternalError(e)));
    }

    /** Envoy hears only that it was our fault: what went wrong is logged, not sent. */
    private static boolean isInternalError(Throwable e) {
        io.grpc.Status status = io.grpc.Status.fromThrowable(e);
        return status.getCode() == io.grpc.Status.Code.INTERNAL && "xds: internal error".equals(status.getDescription());
    }

    private static AggregatedDiscoveryService<DummyUpdate, Object> ads(ClusterEventSource<DummyUpdate> source,
                                                                      ImmutableList<IncrementalConfigBuilder<?, DummyUpdate, Object>> builders) {
        return new AggregatedDiscoveryService<>(source, builders, node -> XdsConfig.of(DETAILS), ImmutableList.of(),
            QueueingStateBacklog.factory(), DiscoveryServiceManagerMetrics.METRICS_DISABLED);
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
        verify(responseObserver).onError(argThat(e -> isInternalError(e)));
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

    @Test
    public void logsSayWhichEnvoyTheyAreAbout(@Mock StreamObserver<DiscoveryResponse> responseObserver,
                                              @Mock ClusterEventSource<DummyUpdate> source) {
        when(source.subscribe(any(), any())).thenReturn(new DummyUpdate());
        DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory =
            new DiscoveryServiceManagerFactory<>(new SotwDiscoveryServiceFactory<>(ImmutableList.of()), QueueingStateBacklog.factory());
        // The manager warns that it won't wait for LDS, which this client doesn't get.
        XdsConfig<Object> config = XdsConfig.of(DETAILS).withDelayUpdatesUntilAckOf(TypeUrl.LDS).withSubscribesTo(Set.of(TypeUrl.CDS));
        CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer =
            observer(responseObserver, source, factory, ImmutableList.of(), config);
        Node node = Node.newBuilder().setId("envoy").build();

        try (LoggedWarnings managerWarnings = new LoggedWarnings(DiscoveryServiceManager.class);
             LoggedWarnings observerWarnings = new LoggedWarnings(CommonDiscoveryStreamObserver.class)) {
            observer.onNext(DiscoveryRequest.newBuilder().setNode(node).setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());
            assertThat(managerWarnings.nodes()).containsExactly("envoy");
            assertThat(MDC.get("node")).as("cleared after, for the thread's next client").isNull();

            // A caller's own node is put back.
            MDC.put("node", "caller");
            observer.onNext(DiscoveryRequest.newBuilder().setNode(node).setTypeUrl("type.googleapis.com/unknown").build());
            assertThat(observerWarnings.nodes()).containsExactly("envoy");
            assertThat(MDC.get("node")).isEqualTo("caller");
            MDC.remove("node");

            // Every way in clears it after.
            observer.onNetworkChange(new DummyUpdate());
            assertThat(MDC.get("node")).isNull();
            observer.resync();
            assertThat(MDC.get("node")).isNull();
            observer.onError(new IllegalStateException("after the stream closed"));
            assertThat(MDC.get("node")).isNull();
            observer.onCompleted();
            assertThat(MDC.get("node")).isNull();
        } finally {
            MDC.remove("node");
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
        return observer(responseObserver, source, factory, listeners, XdsConfig.of(DETAILS));
    }

    private static CommonDiscoveryStreamObserver<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> observer(
        StreamObserver<DiscoveryResponse> responseObserver, ClusterEventSource<DummyUpdate> source,
        DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, DummyUpdate, Object> factory,
        ImmutableList<XdsEventListener<Object>> listeners, XdsConfig<Object> config) {
        return new CommonDiscoveryStreamObserver<>(
            responseObserver,
            source,
            factory,
            r -> new CommonDiscoveryRequest<DiscoveryRequest>(r.getNode(), r.getTypeUrl(), r, r.hasErrorDetail() ? r.getErrorDetail() : null),
            node -> config,
            listeners,
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );
    }
}
