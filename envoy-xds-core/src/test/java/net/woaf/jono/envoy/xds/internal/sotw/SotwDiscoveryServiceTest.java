package net.woaf.jono.envoy.xds.internal.sotw;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.UInt32Value;
import com.google.rpc.Status;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.NamedMessage;
import net.woaf.jono.envoy.xds.internal.CommonDiscoveryRequest;
import net.woaf.jono.envoy.xds.internal.DiscoveryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SotwDiscoveryServiceTest {

    private static class DummyUpdate {

    }

    private final DummyUpdate initState = new DummyUpdate();
    private final XdsConfig<Object> xdsConfig = XdsConfig.of(new Object());

    @Mock StreamObserver<DiscoveryResponse> responseObserver;
    @Captor ArgumentCaptor<DiscoveryResponse> responseCaptor;

    @Test
    public void requestIsAnsweredWithWhatItNamesAndAwaitsItsAck(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, endpoints("a"), endpoints("b"));
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();
        assertThat(ds.hasAnswered()).isFalse();

        ds.processUpdate(request(TypeUrl.EDS, "a"));

        DiscoveryResponse resp = onlyResponse();
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(resp.getResourcesList()).containsExactlyElementsOf(packed(endpoints("a")));
        assertThat(ds.hasAnswered()).isTrue();
        assertThat(ds.awaitingAck()).isTrue();

        ds.processUpdate(ack(TypeUrl.EDS, resp, "a"));
        assertThat(ds.awaitingAck()).isFalse();
    }

    @Test
    public void nackIsAnAnswer(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, endpoints("a"));
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);

        ds.processUpdate(request(TypeUrl.EDS, "a"));
        DiscoveryResponse resp = onlyResponse();
        assertThat(ds.awaitingAck()).isTrue();

        DiscoveryRequest nack = DiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.EDS.getTypeUrl())
            .addResourceNames("a")
            .setResponseNonce(resp.getNonce())
            .setErrorDetail(Status.newBuilder().setMessage("rejected"))
            .build();
        ds.processUpdate(new CommonDiscoveryRequest<>(null, TypeUrl.EDS.getTypeUrl(), nack, nack.getErrorDetail()));

        assertThat(ds.awaitingAck()).isFalse();
    }

    @Test
    public void reconnectingFindsNothingStaleToRemove(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);

        // Unlike delta's initial resource versions, nothing in a SotW request says what the client already has.
        assertThat(ds.hasDeferredReconnectRemovals()).isFalse();
    }

    @Test
    public void unsubscribingIsNotAnsweredAndDropsTheNameFromLaterAnswers(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, endpoints("a"), endpoints("b"), endpoints("c"));
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);

        ds.processUpdate(request(TypeUrl.EDS, "a", "b"));
        DiscoveryResponse resp = nextResponse(inOrder);
        assertThat(resp.getResourcesList()).containsExactlyElementsOf(packed(endpoints("a"), endpoints("b")));

        // Envoy drops b's endpoints in the ACK, as when it removes cluster b. It doesn't wait for an answer.
        ds.processUpdate(ack(TypeUrl.EDS, resp, "a"));
        assertNoAdditionalResponsesSent(1);

        ds.processUpdate(ack(TypeUrl.EDS, resp, "a", "c"));
        assertThat(nextResponse(inOrder).getResourcesList()).as("b is gone from the set")
            .containsExactlyElementsOf(packed(endpoints("a"), endpoints("c")));
    }

    @Test
    public void requestDuringAChangeGetsThatTypesPartOfItWhichIsNotSentAgain(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        NamedMessage<ClusterLoadAssignment> movedA = NamedMessage.of(ClusterLoadAssignment.newBuilder().setClusterName("a")
            .setPolicy(ClusterLoadAssignment.Policy.newBuilder().setOverprovisioningFactor(UInt32Value.of(200))).build());
        serves(configBuilder, endpoints("a"));
        when(configBuilder.getResourcesAddOrder(eq(next), any(), any())).thenAnswer(inv -> named(inv.getArgument(1), movedA, endpoints("b")));
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(movedA, endpoints("b")), List.of()));
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);
        ds.processUpdate(request(TypeUrl.EDS, "a"));
        DiscoveryResponse first = nextResponse(inOrder);
        ds.processUpdate(ack(TypeUrl.EDS, first, "a"));

        // A change moves a and adds b. Envoy asks for b before the change gets to EDS.
        ds.onNetworkUpdate(next);
        ds.processUpdate(ack(TypeUrl.EDS, first, "a", "b"));
        DiscoveryResponse answer = nextResponse(inOrder);
        assertThat(answer.getResourcesList()).as("the whole set, as the change has it").containsExactlyElementsOf(packed(movedA, endpoints("b")));
        ds.processUpdate(ack(TypeUrl.EDS, answer, "a", "b"));

        ds.sendNetworkUpdatePre();
        ds.sendNetworkUpdatePost();
        assertNoAdditionalResponsesSent(2);
    }

    @Test
    public void requestBeforeItsTypesRemoveRoundKeepsWhatTheRoundRemoves(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        serves(configBuilder, endpoints("a"), endpoints("c"));
        when(configBuilder.getResourcesAddOrder(eq(next), any(), any())).thenAnswer(inv -> named(inv.getArgument(1), endpoints("a"), endpoints("c")));
        lenient().when(configBuilder.getResourcesRemoveOrder(eq(next), any(), any())).thenAnswer(inv -> named(inv.getArgument(1), endpoints("c")));
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(), List.of("a")));
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);
        ds.processUpdate(request(TypeUrl.EDS, "a"));
        DiscoveryResponse first = nextResponse(inOrder);
        ds.processUpdate(ack(TypeUrl.EDS, first, "a"));

        // A change removes a's endpoints, and the route using them. Envoy asks for c while the route's removal waits.
        ds.onNetworkUpdate(next);
        ds.sendNetworkUpdatePre();
        ds.processUpdate(ack(TypeUrl.EDS, first, "a", "c"));
        DiscoveryResponse answer = nextResponse(inOrder);
        assertThat(answer.getResourcesList()).as("a's endpoints kept while the route using them is still there")
            .containsExactlyElementsOf(packed(endpoints("a"), endpoints("c")));
        ds.processUpdate(ack(TypeUrl.EDS, answer, "a", "c"));

        ds.sendNetworkUpdatePost();
        assertThat(nextResponse(inOrder).getResourcesList()).as("a's endpoints removed in turn").containsExactlyElementsOf(packed(endpoints("c")));
    }

    @Test
    public void firstRequestForAWildcardTypeKeepsWhatItsRemoveRoundRemoves(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        when(configBuilder.getResourcesAddOrder(eq(next), any(), any())).thenAnswer(inv -> named(inv.getArgument(1), cluster("a")));
        lenient().when(configBuilder.getResourcesRemoveOrder(eq(next), any(), any())).thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(), List.of("a")));
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);

        // A change removes cluster a, and the listener that routes to it, whose removal isn't sent yet. A reconnected
        // Envoy, which kept a, only now asks for clusters.
        ds.onNetworkUpdate(next);
        ds.sendNetworkUpdatePre();
        ds.processUpdate(request(TypeUrl.CDS));
        DiscoveryResponse answer = nextResponse(inOrder);
        assertThat(answer.getResourcesList()).as("a: an answer without it removes it").containsExactlyElementsOf(packed(cluster("a")));
        ds.processUpdate(ack(TypeUrl.CDS, answer));

        ds.sendNetworkUpdatePost();
        assertThat(nextResponse(inOrder).getResourcesList()).as("a removed in turn").isEmpty();
    }

    @Test
    public void requestAfterANackKeepsWhatTheRejectedChangeRemoves(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        serves(configBuilder, endpoints("a"));
        when(configBuilder.getResourcesAddOrder(eq(next), any(), any())).thenAnswer(inv -> named(inv.getArgument(1), endpoints("a"), endpoints("b")));
        lenient().when(configBuilder.getResourcesRemoveOrder(eq(next), any(), any())).thenAnswer(inv -> named(inv.getArgument(1), endpoints("a")));
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);
        ds.processUpdate(request(TypeUrl.EDS, "a"));
        DiscoveryResponse first = nextResponse(inOrder);
        ds.processUpdate(ack(TypeUrl.EDS, first, "a"));

        // A change adds a cluster and removes b's endpoints. Envoy NACKs the cluster, so the change stops.
        ds.onNetworkUpdate(next);
        ds.abandonChange();
        assertThat(ds.addsToCome()).as("none of the change goes now").isFalse();
        ds.processUpdate(ack(TypeUrl.EDS, first, "a", "b"));
        assertThat(nextResponse(inOrder).getResourcesList()).as("b's endpoints: their removal never went out")
            .containsExactlyElementsOf(packed(endpoints("a"), endpoints("b")));
    }

    @Test
    public void changesAreSentAsTheWholeSetAndChangesOfNothingAreNot(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        final DummyUpdate unchanged = new DummyUpdate();
        NamedMessage<ClusterLoadAssignment> movedA = NamedMessage.of(ClusterLoadAssignment.newBuilder().setClusterName("a")
            .setPolicy(ClusterLoadAssignment.Policy.newBuilder().setOverprovisioningFactor(UInt32Value.of(200))).build());
        serves(configBuilder, endpoints("a"), endpoints("b"));
        when(configBuilder.addOrder(eq(next), any(), any())).thenAnswer(inv ->
            new IncrementalConfigBuilder.Response<>(named(inv.getArgument(1), movedA).resources(), List.of()));
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(), List.of("b")));
        when(configBuilder.addOrder(eq(unchanged), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());
        when(configBuilder.removeOrder(eq(unchanged), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);
        ds.processUpdate(request(TypeUrl.EDS, "a", "b"));
        ds.processUpdate(ack(TypeUrl.EDS, nextResponse(inOrder), "a", "b"));

        ds.onNetworkUpdate(next);
        ds.sendNetworkUpdatePre();
        DiscoveryResponse pre = nextResponse(inOrder);
        assertThat(pre.getResourcesList()).containsExactlyElementsOf(packed(movedA, endpoints("b")));
        ds.processUpdate(ack(TypeUrl.EDS, pre, "a", "b"));
        ds.sendNetworkUpdatePost();
        DiscoveryResponse post = nextResponse(inOrder);
        assertThat(post.getResourcesList()).containsExactlyElementsOf(packed(movedA));
        ds.processUpdate(ack(TypeUrl.EDS, post, "a", "b"));

        ds.onNetworkUpdate(unchanged);
        ds.sendNetworkUpdatePre();
        ds.sendNetworkUpdatePost();
        assertNoAdditionalResponsesSent(3);
    }

    @Test
    public void responsesListResourcesByName(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        serves(configBuilder, cluster("c"), cluster("a"));
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(cluster("d"), cluster("b")), List.of()));
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<Cluster>empty());
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);

        ds.processUpdate(request(TypeUrl.CDS));
        DiscoveryResponse answer = nextResponse(inOrder);
        assertThat(answer.getResourcesList()).as("not in the order the builder gave").containsExactlyElementsOf(packed(cluster("a"), cluster("c")));

        ds.processUpdate(ack(TypeUrl.CDS, answer));
        ds.onNetworkUpdate(next);
        ds.sendNetworkUpdatePre();
        DiscoveryResponse pushed = nextResponse(inOrder);
        assertThat(pushed.getResourcesList()).as("nor in the order they were added")
            .containsExactlyElementsOf(packed(cluster("a"), cluster("b"), cluster("c"), cluster("d")));
        ds.processUpdate(ack(TypeUrl.CDS, pushed));
        ds.sendNetworkUpdatePost();
        assertNoAdditionalResponsesSent(2);
    }

    @Test
    public void anUpdateEqualToTheCurrentStateStillGoesThroughTheBuilders(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, endpoints("a"));
        // Removing duplicates is the source's job: this builder resends a for any update.
        when(configBuilder.addOrder(eq(initState), any(), any())).thenAnswer(inv ->
            new IncrementalConfigBuilder.Response<>(named(inv.getArgument(1), endpoints("a")).resources(), List.of()));
        when(configBuilder.removeOrder(eq(initState), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);
        ds.processUpdate(request(TypeUrl.EDS, "a"));
        ds.processUpdate(ack(TypeUrl.EDS, nextResponse(inOrder), "a"));

        ds.onNetworkUpdate(initState);
        ds.sendNetworkUpdatePre();

        DiscoveryResponse again = nextResponse(inOrder);
        assertThat(again.getResourcesList()).containsExactlyElementsOf(packed(endpoints("a")));
        ds.processUpdate(ack(TypeUrl.EDS, again, "a"));
        ds.sendNetworkUpdatePost();
        assertNoAdditionalResponsesSent(2); // the remove round had nothing
    }

    @Test
    public void anUpdateBeforeTheLastIsSentIsRefused(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        DummyUpdate next = new DummyUpdate();
        ds.onNetworkUpdate(next);

        assertThatThrownBy(() -> ds.onNetworkUpdate(next)).as("even one equal to it")
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("dirty state");
    }

    @Test
    public void legacyWildcardRequestIsAnsweredWithEverything(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, cluster("foo"), cluster("bar"));
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);
        ds.init(initState);

        ds.processUpdate(request(TypeUrl.CDS));

        DiscoveryResponse resp = onlyResponse();
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(resp.getResourcesList()).containsExactlyElementsOf(packed(cluster("bar"), cluster("foo")));
    }

    @Test
    public void switchingFromLegacyToExplicitWildcardIsNotAnswered(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, cluster("foo"));
        DiscoveryService<DiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);
        ds.init(initState);

        ds.processUpdate(request(TypeUrl.CDS));
        ds.processUpdate(ack(TypeUrl.CDS, onlyResponse(), "*"));

        assertNoAdditionalResponsesSent(1);
    }

    /** A service for this type over responseObserver, as the factory builds it. */
    @SuppressWarnings("unchecked")
    private <E extends Message> DiscoveryService<DiscoveryRequest, DummyUpdate> service(TypeUrl typeUrl, IncrementalConfigBuilder<E, DummyUpdate, Object> configBuilder) {
        when(configBuilder.handlesType()).thenReturn((Class<E>) typeUrl.getMessageClazz());
        return new SotwDiscoveryServiceFactory<DummyUpdate, Object>(ImmutableList.of(configBuilder))
            .createDiscoveryService(responseObserver, xdsConfig, typeUrl);
    }

    /** Stubs the builder to have these resources in initState, giving only those asked for. */
    @SafeVarargs
    private <E extends Message> void serves(IncrementalConfigBuilder<E, DummyUpdate, Object> configBuilder, NamedMessage<E>... resources) {
        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenAnswer(inv -> named(inv.getArgument(1), resources));
    }

    @SafeVarargs
    private static <E extends Message> IncrementalConfigBuilder.Resources<E> named(Predicate<String> asked, NamedMessage<E>... resources) {
        return new IncrementalConfigBuilder.Resources<>(Stream.of(resources).filter(r -> asked.test(r.name())).toList());
    }

    private static List<Any> packed(NamedMessage<?>... resources) {
        return Stream.of(resources).map(r -> Any.pack(r.message())).toList();
    }

    private static NamedMessage<Cluster> cluster(String name) {
        return NamedMessage.of(Cluster.newBuilder().setName(name).build());
    }

    private static NamedMessage<ClusterLoadAssignment> endpoints(String clusterName) {
        return NamedMessage.of(ClusterLoadAssignment.newBuilder().setClusterName(clusterName).build());
    }

    private CommonDiscoveryRequest<DiscoveryRequest> sotwRequest(TypeUrl typeUrl, DiscoveryRequest.Builder request) {
        return new CommonDiscoveryRequest<DiscoveryRequest>(null, typeUrl.getTypeUrl(), request.setTypeUrl(typeUrl.getTypeUrl()).build(), null);
    }

    private CommonDiscoveryRequest<DiscoveryRequest> request(TypeUrl typeUrl, String... names) {
        return sotwRequest(typeUrl, DiscoveryRequest.newBuilder().addAllResourceNames(List.of(names)));
    }

    /** Envoy's ACK of resp, naming what it now wants: SotW requests after the first carry the last nonce. */
    private CommonDiscoveryRequest<DiscoveryRequest> ack(TypeUrl typeUrl, DiscoveryResponse resp, String... names) {
        return sotwRequest(typeUrl, DiscoveryRequest.newBuilder().setVersionInfo(resp.getVersionInfo()).setResponseNonce(resp.getNonce())
            .addAllResourceNames(List.of(names)));
    }

    private void assertNoAdditionalResponsesSent(int expectedResponsesSoFar) {
        verify(responseObserver, times(expectedResponsesSoFar)).onNext(any());
    }

    /** The one response sent so far. */
    private DiscoveryResponse onlyResponse() {
        verify(responseObserver).onNext(responseCaptor.capture());
        return responseCaptor.getValue();
    }

    private DiscoveryResponse nextResponse(InOrder inOrder) {
        inOrder.verify(responseObserver).onNext(responseCaptor.capture());
        return responseCaptor.getValue();
    }

}
