package net.woaf.jono.envoy.xds.internal.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.internal.AbstractDiscoveryService;
import net.woaf.jono.envoy.xds.internal.CommonDiscoveryRequest;
import net.woaf.jono.envoy.xds.internal.DiscoveryService;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceManager;
import net.woaf.jono.envoy.xds.internal.LoggedWarnings;
import net.woaf.jono.envoy.xds.internal.NoOpConfigBuilder;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.NamedMessage;
import net.woaf.jono.envoy.xds.api.utils.QueueingStateBacklog;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IncrementalDiscoveryServiceTest {

    private static class DummyUpdate {

    }

    private final DummyUpdate initState = new DummyUpdate();
    private final XdsConfig<Object> xdsConfig = XdsConfig.of(new Object());

    @Mock StreamObserver<DeltaDiscoveryResponse> responseObserver;
    @Captor ArgumentCaptor<DeltaDiscoveryResponse> responseCaptor;

    @Test
    public void testUpdateNotSubscribedNoSendUpdates(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);

        ds.init(initState);
        ds.onNetworkUpdate(new DummyUpdate());
        ds.sendNetworkUpdatePre();
        ds.sendNetworkUpdatePost();

        // If envoy has never sent a request for EDS, we do not expect any attempt to build a response.
        // If we did return a response due to a leaky config builder (one that returns unasked for resources) then envoy would throw an error.
        verify(responseObserver, never()).onNext(any());
        verifyNoInteractions(configBuilder);
    }

    @Test
    public void testSubscribeSubListButNoResourcesThenUpdates(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        serves(configBuilder);
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(endpoints("foo")), List.of()));
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);

        // Envoy sends an EDS request:
        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();
        ds.processUpdate(subscribeRequest(TypeUrl.EDS, "foo"));

        InOrder inOrder = inOrder(responseObserver);
        // Our initial state has no resources, so we should tell envoy that the resource (foo) that it asked for has been removed.
        DeltaDiscoveryResponse resp = nextResponse(inOrder);
        assertThat(resp.getRemovedResourcesList()).containsExactly("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(resp.getResourcesList()).isEmpty();

        assertThat(ds.awaitingAck()).isTrue();

        // Envoy acks the update, without changing the subs list.
        ds.processUpdate(ackRequest(TypeUrl.EDS, resp.getNonce()));

        assertThat(ds.awaitingAck()).isFalse();

        ds.onNetworkUpdate(next);
        ds.sendNetworkUpdatePre();
        ds.sendNetworkUpdatePost();

        resp = nextResponse(inOrder);
        assertThat(resp.getRemovedResourcesList()).isEmpty();
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void namesAnsweredWhileAChangeWaitsItsTurnAreNotSentAgainByTheChange(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        serves(configBuilder, endpoints("a"));
        when(configBuilder.getResourcesAddOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(endpoints("b"))));
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(endpoints("a"), endpoints("b")), List.of()));
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);
        ds.processUpdate(subscribeRequest(TypeUrl.EDS, "a"));
        ds.processUpdate(ackRequest(TypeUrl.EDS, nextResponse(inOrder).getNonce()));

        // A change adds cluster b. Envoy has it, and asks for its endpoints, before the change gets to EDS.
        ds.onNetworkUpdate(next);
        ds.processUpdate(subscribeRequest(TypeUrl.EDS, "b"));
        DeltaDiscoveryResponse answer = nextResponse(inOrder);
        assertThat(answer.getResourcesList()).extracting(Resource::getName).containsExactly("b");
        ds.processUpdate(ackRequest(TypeUrl.EDS, answer.getNonce()));

        ds.sendNetworkUpdatePre();
        ds.sendNetworkUpdatePost();

        assertThat(nextResponse(inOrder).getResourcesList()).extracting(Resource::getName).as("b already went").containsExactly("a");
        assertNoAdditionalResponsesSent(3);
    }

    @Test
    public void whatAChangeSkippedIsSentByALaterChange(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        final DummyUpdate moved = new DummyUpdate();
        final DummyUpdate back = new DummyUpdate();
        NamedMessage<ClusterLoadAssignment> movedB = NamedMessage.of(ClusterLoadAssignment.newBuilder().setClusterName("b")
            .setPolicy(ClusterLoadAssignment.Policy.newBuilder().setOverprovisioningFactor(UInt32Value.of(200))).build());
        when(configBuilder.getResourcesAddOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(endpoints("b"))));
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(endpoints("b")), List.of()));
        when(configBuilder.addOrder(eq(moved), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(movedB), List.of()));
        when(configBuilder.addOrder(eq(back), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(endpoints("b")), List.of()));
        when(configBuilder.removeOrder(any(), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);

        // b is answered mid-change, so that change skips it.
        ds.onNetworkUpdate(next);
        ds.processUpdate(subscribeRequest(TypeUrl.EDS, "b"));
        ds.processUpdate(ackRequest(TypeUrl.EDS, nextResponse(inOrder).getNonce()));
        ds.sendNetworkUpdatePre();
        ds.sendNetworkUpdatePost();

        // Later changes move b, then move it back: the change back is news to Envoy.
        for (DummyUpdate change : List.of(moved, back)) {
            ds.onNetworkUpdate(change);
            ds.sendNetworkUpdatePre();
            ds.sendNetworkUpdatePost();
            DeltaDiscoveryResponse resp = nextResponse(inOrder);
            ds.processUpdate(ackRequest(TypeUrl.EDS, resp.getNonce()));
            assertThat(resp.getResourcesList()).extracting(Resource::getName).containsExactly("b");
        }
        assertNoAdditionalResponsesSent(3);
    }

    @Test
    public void requestNamingNothingGetsNoAnswer(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends an EDS request that asks for nothing
        ds.processUpdate(subscribeRequest(TypeUrl.EDS));

        // And again
        ds.processUpdate(subscribeRequest(TypeUrl.EDS));

        // Neither changes a subscription, so neither needs an answer, and an empty one would hold up pushes until Envoy
        // ACKs it (SubListSubManager).
        verify(responseObserver, never()).onNext(any());
        assertThat(ds.awaitingAck()).isFalse();
        verifyNoInteractions(configBuilder);
    }

    @Test
    public void testOldWildcardSubNoState(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        serves(configBuilder);
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for nothing, this should cause it to sub to wildcard.
        ds.processUpdate(subscribeRequest(TypeUrl.CDS));

        // We should tell envoy about everything
        DeltaDiscoveryResponse resp = onlyResponse();
        assertThat(resp.getResourcesList()).isEmpty();
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(resp.getRemovedResourcesList()).isEmpty();

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testOldWildcardSub(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, cluster("foo"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for nothing, this should cause it to sub to wildcard.
        ds.processUpdate(subscribeRequest(TypeUrl.CDS));

        // We should tell envoy about everything
        DeltaDiscoveryResponse resp = onlyResponse();
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(resp.getRemovedResourcesList()).isEmpty();

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testAskingAgain(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, cluster("foo"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        String n;
        InOrder inOrder = inOrder(responseObserver);

        {
            ds.processUpdate(subscribeRequest(TypeUrl.EDS, "foo"));

            DeltaDiscoveryResponse resp = nextResponse(inOrder);
            assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
            assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
            assertThat(resp.getRemovedResourcesList()).isEmpty();
            n = resp.getNonce();

            assertThat(ds.awaitingAck()).isTrue();
        }

        {
            // Ask again, as if envoy has forgotten about the resource
            ds.processUpdate(subscribeRequest(TypeUrl.EDS, "foo"));

            DeltaDiscoveryResponse resp = nextResponse(inOrder);
            assertThat(resp.getNonce()).isNotEqualTo(n); // Validate we captured the right response and aren't just verifying the first one again!
            assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
            assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
            assertThat(resp.getRemovedResourcesList()).isEmpty();

            assertThat(ds.awaitingAck()).isTrue();
        }
    }

    @Test
    public void testNackIsAnAnswer(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, cluster("foo"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);

        ds.processUpdate(subscribeRequest(TypeUrl.EDS, "foo"));
        DeltaDiscoveryResponse resp = onlyResponse();
        assertThat(ds.awaitingAck()).isTrue();

        DeltaDiscoveryRequest nack = DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.EDS.getTypeUrl())
            .setResponseNonce(resp.getNonce())
            .setErrorDetail(com.google.rpc.Status.newBuilder().setMessage("rejected"))
            .build();
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.EDS.getTypeUrl(), nack, nack.getErrorDetail()));

        assertThat(ds.awaitingAck()).isFalse();
    }

    @Test
    public void testNewWildcardSub(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, cluster("foo"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for "*", this should cause it to sub to wildcard.
        ds.processUpdate(subscribeRequest(TypeUrl.CDS, "*"));

        // We should tell envoy about everything
        DeltaDiscoveryResponse resp = onlyResponse();
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(resp.getRemovedResourcesList()).isEmpty();

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testExplicitSubscribeInWildcardModeIsAnswered(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenAnswer(inv -> {
            Predicate<String> filter = inv.getArgument(1);
            List<NamedMessage<Cluster>> resources = new ArrayList<>();
            if (filter.test("foo")) {
                resources.add(cluster("foo"));
            }
            return new IncrementalConfigBuilder.Resources<>(resources);
        });
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);
        ds.init(initState);

        ds.processUpdate(subscribeRequest(TypeUrl.CDS, "*"));
        ds.processUpdate(ackRequest(TypeUrl.CDS, onlyResponse().getNonce()));

        // On-demand style subscribe to a name that doesn't exist, without touching "*": the spec requires an answer
        // that removes it, or Envoy waits out initial_fetch_timeout.
        ds.processUpdate(subscribeRequest(TypeUrl.CDS, "missing"));

        verify(responseObserver, times(2)).onNext(responseCaptor.capture());
        DeltaDiscoveryResponse resp = responseCaptor.getValue();
        assertThat(resp.getResourcesList()).isEmpty();
        assertThat(resp.getRemovedResourcesList()).containsExactly("missing");
    }

    @Test
    public void testRemovesInitialStateIfNotExists(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, cluster("foo"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for "*", this should cause it to sub to wildcard.
        // This time we also tell envoy we already now about foo, bar and baz - which is what happens when envoy already has state when it connects to ADS (e.g. it's reconnecting.)
        ds.processUpdate(deltaRequest(TypeUrl.CDS, DeltaDiscoveryRequest.newBuilder()
            .addResourceNamesSubscribe("*")
            .putInitialResourceVersions("foo", "1")
            .putInitialResourceVersions("bar", "1")
            .putInitialResourceVersions("baz", "1")));

        // We should tell envoy about everything we know about (which is just foo)
        DeltaDiscoveryResponse resp = onlyResponse();
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        // With no manager-level ACK barrier configured, stale resources from initial_resource_versions are removed
        // in the initial subscription response.
        assertThat(resp.getRemovedResourcesList()).containsExactlyInAnyOrder("bar", "baz");

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testDoesNotRemoveInitialStateIfNotSubscribed(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, cluster("foo"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.CDS, configBuilder);

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for "foo", this should cause it to sub to the specific resource.
        // This time we also tell envoy we already now about foo, bar and baz - which is what happens when envoy already has state when it connects to ADS (e.g. it's reconnecting.)
        ds.processUpdate(deltaRequest(TypeUrl.CDS, DeltaDiscoveryRequest.newBuilder()
            .addResourceNamesSubscribe("foo")
            .addResourceNamesSubscribe("bar")
            .putInitialResourceVersions("foo", "1")
            .putInitialResourceVersions("bar", "1")
            .putInitialResourceVersions("baz", "1")));

        // We should tell envoy about the thing it asked for that still exists (foo)
        DeltaDiscoveryResponse resp = onlyResponse();
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        // Envoy asked for bar, and reported that it already knows bar in initial_resource_versions.
        // With no manager-level ACK barrier configured, that stale subscribed resource is removed immediately.
        // Baz exists in the initial versions, but the client has not asked to subscribe to it, so we don't remove it either.
        assertThat(resp.getRemovedResourcesList()).containsExactly("bar");

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testReconnectInitialResourceVersionRemovalsWaitForConfiguredAck(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        serves(clusterConfigBuilder);
        serves(routeConfigBuilder, routes("route-x")); // doesn't use the deleted cluster
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = manager(Map.of(
            TypeUrl.CDS, service(TypeUrl.CDS, clusterConfigBuilder),
            TypeUrl.RDS, service(TypeUrl.RDS, routeConfigBuilder)));

        dsm.init(initState, TypeUrl.RDS, null);
        InOrder inOrder = inOrder(responseObserver);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-a"));

        DeltaDiscoveryResponse cdsReconnectResponse = nextResponse(inOrder);
        assertThat(cdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsReconnectResponse.getResourcesList()).isEmpty();
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        dsm.processUpdate(ackRequest(TypeUrl.CDS, cdsReconnectResponse.getNonce()));

        dsm.processUpdate(reconnectSubscription(TypeUrl.RDS, "route-x", "route-x"));

        DeltaDiscoveryResponse rdsReconnectResponse = nextResponse(inOrder);
        assertThat(rdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.RDS.getTypeUrl());
        assertThat(rdsReconnectResponse.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("route-x");
        assertThat(rdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        dsm.processUpdate(ackRequest(TypeUrl.RDS, rdsReconnectResponse.getNonce()));

        DeltaDiscoveryResponse cdsDeferredRemovalResponse = nextResponse(inOrder);
        assertThat(cdsDeferredRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsDeferredRemovalResponse.getResourcesList()).isEmpty();
        assertThat(cdsDeferredRemovalResponse.getRemovedResourcesList()).containsExactly("cluster-a");
    }

    @Test
    public void testNoConfiguredDelaySendsInitialResourceVersionRemovalsInInitialResponse(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder) {
        serves(clusterConfigBuilder);
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> cds = service(TypeUrl.CDS, clusterConfigBuilder);
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = manager(Map.of(TypeUrl.CDS, cds));

        dsm.init(initState, null, null);
        InOrder inOrder = inOrder(responseObserver);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-a"));

        DeltaDiscoveryResponse reconnectResponse = nextResponse(inOrder);
        assertThat(reconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(reconnectResponse.getResourcesList()).isEmpty();
        assertThat(reconnectResponse.getRemovedResourcesList()).containsExactly("cluster-a");
        assertThat(cds.hasDeferredReconnectRemovals()).isFalse();

        dsm.processUpdate(ackRequest(TypeUrl.CDS, reconnectResponse.getNonce()));
        assertNoAdditionalResponsesSent(1);
    }

    @Test
    public void testStaleClusterRemovalWaitsForRoutes(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        serves(clusterConfigBuilder);
        serves(routeConfigBuilder);
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = cdsEdsRds(clusterConfigBuilder, endpointConfigBuilder, routeConfigBuilder);
        dsm.init(initState, null, null);
        InOrder inOrder = inOrder(responseObserver);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-stale"));
        DeltaDiscoveryResponse cdsReconnectResponse = nextResponse(inOrder);
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).as("routes may still use it").isEmpty();
        dsm.processUpdate(ackRequest(TypeUrl.CDS, cdsReconnectResponse.getNonce()));
        assertNoAdditionalResponsesSent(1);

        dsm.processUpdate(reconnectSubscription(TypeUrl.RDS, "route-stale", "route-stale"));
        DeltaDiscoveryResponse rdsReconnectResponse = nextResponse(inOrder);
        assertThat(rdsReconnectResponse.getRemovedResourcesList()).containsExactly("route-stale");
        dsm.processUpdate(ackRequest(TypeUrl.RDS, rdsReconnectResponse.getNonce()));

        DeltaDiscoveryResponse cdsRemovalResponse = nextResponse(inOrder);
        assertThat(cdsRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsRemovalResponse.getRemovedResourcesList()).containsExactly("cluster-stale");
    }

    @Test
    public void testStaleClusterRemovalDoesNotWaitForTypesTheClientDoesNotUse(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        serves(clusterConfigBuilder);
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = cdsEdsRds(clusterConfigBuilder, endpointConfigBuilder, routeConfigBuilder);
        dsm.init(initState, null, Set.of(TypeUrl.CDS, TypeUrl.EDS)); // static routes

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-stale"));
        assertThat(nextResponse(inOrder(responseObserver)).getRemovedResourcesList()).containsExactly("cluster-stale");
    }

    @Test
    public void testStaleClusterRemovalDoesNotWaitForTypesWeDoNotServe(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder) {
        serves(clusterConfigBuilder);
        // As the factory builds it: every type has a service, with a NoOpConfigBuilder for those nobody configured.
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = manager(Map.of(
            TypeUrl.CDS, service(TypeUrl.CDS, clusterConfigBuilder),
            TypeUrl.RDS, service(TypeUrl.RDS, new NoOpConfigBuilder<RouteConfiguration, DummyUpdate, Object>(RouteConfiguration.class))));
        dsm.init(initState, null, null);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-stale"));
        assertThat(nextResponse(inOrder(responseObserver)).getRemovedResourcesList()).containsExactly("cluster-stale");
    }

    @Test
    public void testStaleRemovalsGoAfterTheTimeoutAtTheNextEvent(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        serves(clusterConfigBuilder);
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = cdsEdsRds(clusterConfigBuilder, endpointConfigBuilder, routeConfigBuilder);
        AtomicLong now = new AtomicLong();
        dsm.setClock(now::get);
        dsm.init(initState, null, null);
        InOrder inOrder = inOrder(responseObserver);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-stale"));
        DeltaDiscoveryResponse cdsReconnectResponse = nextResponse(inOrder);
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        now.addAndGet(DiscoveryServiceManager.WAIT_FOR_ACK_TIMEOUT_NANOS); // RDS never subscribes
        dsm.processUpdate(ackRequest(TypeUrl.CDS, cdsReconnectResponse.getNonce()));
        assertThat(nextResponse(inOrder).getRemovedResourcesList()).containsExactly("cluster-stale");
    }

    @Test
    public void testRoutesWaitForTheClustersAndEndpointsTheyMayUse(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        serves(clusterConfigBuilder, cluster("new"));
        serves(endpointConfigBuilder, endpoints("new"));
        serves(routeConfigBuilder, routes("routes"));
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = cdsEdsRds(clusterConfigBuilder, endpointConfigBuilder, routeConfigBuilder);
        dsm.init(initState, null, null);
        InOrder inOrder = inOrder(responseObserver);

        // Envoy's reconnect burst: everything at once.
        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", null));
        final DeltaDiscoveryResponse cds = nextResponse(inOrder);
        dsm.processUpdate(reconnectSubscription(TypeUrl.RDS, "routes", "routes"));
        assertNoAdditionalResponsesSent(1); // held: the clusters it may use aren't ACKed
        dsm.processUpdate(reconnectSubscription(TypeUrl.EDS, "new", null));
        DeltaDiscoveryResponse eds = nextResponse(inOrder);
        assertThat(eds.getTypeUrl()).as("EDS is never held: Envoy's CDS ACK waits for it").isEqualTo(TypeUrl.EDS.getTypeUrl());

        dsm.processUpdate(ackRequest(TypeUrl.CDS, cds.getNonce()));
        assertNoAdditionalResponsesSent(2);
        dsm.processUpdate(ackRequest(TypeUrl.EDS, eds.getNonce()));
        DeltaDiscoveryResponse rds = nextResponse(inOrder);
        assertThat(rds.getTypeUrl()).isEqualTo(TypeUrl.RDS.getTypeUrl());
        assertThat(rds.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("routes");
    }

    @Test
    public void testReconnectNetworkUpdatesWaitForDeferredInitialRemovals(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        final DummyUpdate nextState = new DummyUpdate();
        serves(clusterConfigBuilder);
        serves(routeConfigBuilder, routes("route-x")); // doesn't use the deleted cluster
        when(clusterConfigBuilder.addOrder(eq(nextState), any(), any()))
            .thenReturn(new IncrementalConfigBuilder.Response<>(List.of(cluster("cluster-b")), List.of()));
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = manager(Map.of(
            TypeUrl.CDS, service(TypeUrl.CDS, clusterConfigBuilder),
            TypeUrl.RDS, service(TypeUrl.RDS, routeConfigBuilder)));

        dsm.init(initState, TypeUrl.RDS, null);
        InOrder inOrder = inOrder(responseObserver);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-a"));
        DeltaDiscoveryResponse cdsReconnectResponse = nextResponse(inOrder);
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        dsm.processUpdate(ackRequest(TypeUrl.CDS, cdsReconnectResponse.getNonce()));

        dsm.processUpdate(reconnectSubscription(TypeUrl.RDS, "route-x", "route-x"));
        DeltaDiscoveryResponse rdsReconnectResponse = nextResponse(inOrder);
        assertThat(rdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.RDS.getTypeUrl());
        assertThat(rdsReconnectResponse.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("route-x");
        assertThat(rdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        dsm.pushUpdates(nextState);
        assertNoAdditionalResponsesSent(2);

        dsm.processUpdate(ackRequest(TypeUrl.RDS, rdsReconnectResponse.getNonce()));

        DeltaDiscoveryResponse cdsDeferredRemovalResponse = nextResponse(inOrder);
        assertThat(cdsDeferredRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsDeferredRemovalResponse.getResourcesList()).isEmpty();
        assertThat(cdsDeferredRemovalResponse.getRemovedResourcesList()).containsExactly("cluster-a");

        dsm.processUpdate(ackRequest(TypeUrl.CDS, cdsDeferredRemovalResponse.getNonce()));

        DeltaDiscoveryResponse cdsNetworkUpdateResponse = nextResponse(inOrder);
        assertThat(cdsNetworkUpdateResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsNetworkUpdateResponse.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("cluster-b");
        assertThat(cdsNetworkUpdateResponse.getRemovedResourcesList()).isEmpty();
    }

    @Test
    public void testDeferredReconnectRemovalsUseRemoveOrderAndWaitForAck(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        serves(routeConfigBuilder, routes("route-current"));
        serves(clusterConfigBuilder);
        serves(endpointConfigBuilder);
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = cdsEdsRds(clusterConfigBuilder, endpointConfigBuilder, routeConfigBuilder);

        dsm.init(initState, TypeUrl.RDS, null);
        InOrder inOrder = inOrder(responseObserver);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-stale"));
        DeltaDiscoveryResponse cdsReconnectResponse = nextResponse(inOrder);
        assertThat(cdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsReconnectResponse.getResourcesList()).isEmpty();
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).isEmpty();
        dsm.processUpdate(ackRequest(TypeUrl.CDS, cdsReconnectResponse.getNonce()));

        dsm.processUpdate(reconnectSubscription(TypeUrl.EDS, "endpoint-stale", "endpoint-stale"));
        DeltaDiscoveryResponse edsReconnectResponse = nextResponse(inOrder);
        assertThat(edsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(edsReconnectResponse.getResourcesList()).isEmpty();
        assertThat(edsReconnectResponse.getRemovedResourcesList()).isEmpty();
        dsm.processUpdate(ackRequest(TypeUrl.EDS, edsReconnectResponse.getNonce()));

        dsm.processUpdate(deltaRequest(TypeUrl.RDS, DeltaDiscoveryRequest.newBuilder()
            .addResourceNamesSubscribe("route-current")
            .addResourceNamesSubscribe("route-stale")
            .putInitialResourceVersions("route-stale", "1")));
        DeltaDiscoveryResponse rdsReconnectResponse = nextResponse(inOrder);
        assertThat(rdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.RDS.getTypeUrl());
        assertThat(rdsReconnectResponse.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("route-current");
        // Nothing before RDS in remove order is served, so its stale route config goes straight away.
        assertThat(rdsReconnectResponse.getRemovedResourcesList()).containsExactly("route-stale");
        assertNoAdditionalResponsesSent(3);

        dsm.processUpdate(ackRequest(TypeUrl.RDS, rdsReconnectResponse.getNonce()));

        DeltaDiscoveryResponse cdsRemovalResponse = nextResponse(inOrder);
        assertThat(cdsRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsRemovalResponse.getResourcesList()).isEmpty();
        assertThat(cdsRemovalResponse.getRemovedResourcesList()).containsExactly("cluster-stale");
        assertNoAdditionalResponsesSent(4);

        dsm.processUpdate(nonAckRequest(TypeUrl.CDS));
        assertNoAdditionalResponsesSent(4);

        dsm.processUpdate(ackRequest(TypeUrl.CDS, cdsRemovalResponse.getNonce()));

        DeltaDiscoveryResponse edsRemovalResponse = nextResponse(inOrder);
        assertThat(edsRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(edsRemovalResponse.getResourcesList()).isEmpty();
        assertThat(edsRemovalResponse.getRemovedResourcesList()).containsExactly("endpoint-stale");
    }

    @Test
    public void testDoesNotDeferInitialStateRemovalForUnsubscribedSubListResource(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, endpoints("endpoint-current"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);

        ds.init(initState);
        ds.processUpdate(deltaRequest(TypeUrl.EDS, DeltaDiscoveryRequest.newBuilder()
            .addResourceNamesSubscribe("endpoint-current")
            .putInitialResourceVersions("endpoint-unsubscribed", "1")));

        DeltaDiscoveryResponse resp = onlyResponse();
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("endpoint-current");
        assertThat(resp.getRemovedResourcesList()).isEmpty();
        assertThat(ds.hasDeferredReconnectRemovals()).isFalse();
    }

    @Test
    public void removalsInAChangeGoInTheRemoveRound(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        serves(configBuilder, endpoints("a"));
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<ClusterLoadAssignment>(List.of(), List.of("a")));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);
        ds.processUpdate(subscribeRequest(TypeUrl.EDS, "a"));
        ds.processUpdate(ackRequest(TypeUrl.EDS, nextResponse(inOrder).getNonce()));

        ds.onNetworkUpdate(next);
        ds.sendNetworkUpdatePre();
        assertNoAdditionalResponsesSent(1);
        ds.sendNetworkUpdatePost();

        assertThat(nextResponse(inOrder).getRemovedResourcesList()).containsExactly("a");
    }

    @Test
    public void eachResponseHasAHigherVersionThanTheLast(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, endpoints("a"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        InOrder inOrder = inOrder(responseObserver);

        ds.processUpdate(subscribeRequest(TypeUrl.EDS, "a"));
        DeltaDiscoveryResponse first = nextResponse(inOrder);
        ds.processUpdate(ackRequest(TypeUrl.EDS, first.getNonce()));
        ds.processUpdate(subscribeRequest(TypeUrl.EDS, "b"));
        DeltaDiscoveryResponse second = nextResponse(inOrder);

        // So the newest is the highest, e.g. in Envoy's config dump.
        assertThat(first.getResources(0).getVersion()).isEqualTo(first.getSystemVersionInfo());
        assertThat(Long.parseLong(second.getSystemVersionInfo())).isGreaterThan(Long.parseLong(first.getSystemVersionInfo()));
    }

    @Test
    public void builderAnsweringWithResourcesNobodyAskedForIsWarnedAbout(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        serves(configBuilder, endpoints("a"), endpoints("b"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);

        try (LoggedWarnings warnings = new LoggedWarnings(AbstractDiscoveryService.class)) {
            ds.processUpdate(subscribeRequest(TypeUrl.EDS, "a"));

            assertThat(warnings.messages()).containsExactly("Config builder for " + TypeUrl.EDS.getTypeUrl() + " returned unasked for resources!");
        }
    }

    @Test
    public void builderChangingWhatNobodyAskedForIsWarnedAboutInEachRound(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        serves(configBuilder, endpoints("a"));
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(endpoints("a"), endpoints("x")), List.of()));
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<ClusterLoadAssignment>(List.of(), List.of("y")));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);
        ds.processUpdate(subscribeRequest(TypeUrl.EDS, "a"));
        String unasked = "Config builder for " + TypeUrl.EDS.getTypeUrl() + " returned unasked for messages!";

        try (LoggedWarnings warnings = new LoggedWarnings(AbstractDiscoveryService.class)) {
            ds.onNetworkUpdate(next);
            ds.sendNetworkUpdatePre();
            assertThat(warnings.messages()).as("adding x").containsExactly(unasked);
            ds.sendNetworkUpdatePost();
            assertThat(warnings.messages()).as("and removing y").containsExactly(unasked, unasked);
        }
    }

    @Test
    public void builderGivingOnlyWhatWasAskedForIsNotWarnedAbout(@Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {
        final DummyUpdate next = new DummyUpdate();
        serves(configBuilder, endpoints("a"));
        IncrementalConfigBuilder.Response<ClusterLoadAssignment> asked = new IncrementalConfigBuilder.Response<>(List.of(endpoints("a")), List.of("b"));
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(asked);
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(asked);
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = service(TypeUrl.EDS, configBuilder);
        ds.init(initState);

        try (LoggedWarnings warnings = new LoggedWarnings(AbstractDiscoveryService.class)) {
            ds.processUpdate(subscribeRequest(TypeUrl.EDS, "a", "b"));
            ds.onNetworkUpdate(next);
            ds.sendNetworkUpdatePre();
            ds.sendNetworkUpdatePost();

            assertThat(warnings.messages()).isEmpty();
        }
    }

    @Test
    public void theFactoryGivesWildcardTypesWildcardSubscriptions(@Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        when(configBuilder.handlesType()).thenReturn(Cluster.class);
        serves(configBuilder, cluster("foo"));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryServiceFactory<DummyUpdate, Object>(ImmutableList.of(configBuilder))
            .createDiscoveryService(responseObserver, xdsConfig, TypeUrl.CDS);
        ds.init(initState);

        // Asking for nothing subscribes to everything, for a wildcard type.
        ds.processUpdate(subscribeRequest(TypeUrl.CDS));

        assertThat(onlyResponse().getResourcesList()).extracting(Resource::getName).containsExactly("foo");
    }

    /** A service over responseObserver, with the sub manager the factory would give this type. */
    private <E extends Message> DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> service(TypeUrl typeUrl, IncrementalConfigBuilder<E, DummyUpdate, Object> configBuilder) {
        return new IncrementalDiscoveryService<>(typeUrl, responseObserver, configBuilder, xdsConfig,
            typeUrl.isWildcard() ? new WildcardSubManager() : new SubListSubManager());
    }

    /** A manager for these services, in ADS add and remove order. */
    private DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> manager(Map<TypeUrl, DiscoveryService<DeltaDiscoveryRequest, DummyUpdate>> services) {
        return new DiscoveryServiceManager<>(
            services,
            TypeUrl.ADD_ORDER.stream().filter(services::containsKey).toList(),
            TypeUrl.REMOVE_ORDER.stream().filter(services::containsKey).toList(),
            QueueingStateBacklog.<DummyUpdate>factory().build(),
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );
    }

    /** Builds CDS, EDS and RDS services over one stream, and a manager for them. */
    private DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> cdsEdsRds(IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusters,
        IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpoints, IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routes) {
        return manager(Map.of(TypeUrl.CDS, service(TypeUrl.CDS, clusters), TypeUrl.EDS, service(TypeUrl.EDS, endpoints), TypeUrl.RDS, service(TypeUrl.RDS, routes)));
    }

    /** Stubs the builder to have these resources in initState, whatever is asked for. */
    @SafeVarargs
    private <E extends Message> void serves(IncrementalConfigBuilder<E, DummyUpdate, Object> configBuilder, NamedMessage<E>... resources) {
        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(resources)));
    }

    private static NamedMessage<Cluster> cluster(String name) {
        return NamedMessage.of(Cluster.newBuilder().setName(name).build());
    }

    private static NamedMessage<ClusterLoadAssignment> endpoints(String clusterName) {
        return NamedMessage.of(ClusterLoadAssignment.newBuilder().setClusterName(clusterName).build());
    }

    private static NamedMessage<RouteConfiguration> routes(String name) {
        return NamedMessage.of(RouteConfiguration.newBuilder().setName(name).build());
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> reconnectSubscription(TypeUrl typeUrl, String subscribe, String kept) {
        DeltaDiscoveryRequest.Builder request = DeltaDiscoveryRequest.newBuilder().addResourceNamesSubscribe(subscribe);
        if (kept != null) {
            request.putInitialResourceVersions(kept, "1");
        }
        return deltaRequest(typeUrl, request);
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> deltaRequest(TypeUrl typeUrl, DeltaDiscoveryRequest.Builder request) {
        return new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, typeUrl.getTypeUrl(), request.setTypeUrl(typeUrl.getTypeUrl()).build(), null);
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> subscribeRequest(TypeUrl typeUrl, String... names) {
        return deltaRequest(typeUrl, DeltaDiscoveryRequest.newBuilder().addAllResourceNamesSubscribe(List.of(names)));
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> ackRequest(TypeUrl typeUrl, String nonce) {
        return deltaRequest(typeUrl, DeltaDiscoveryRequest.newBuilder().setResponseNonce(nonce));
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> nonAckRequest(TypeUrl typeUrl) {
        return deltaRequest(typeUrl, DeltaDiscoveryRequest.newBuilder());
    }

    private void assertNoAdditionalResponsesSent(int expectedResponsesSoFar) {
        verify(responseObserver, times(expectedResponsesSoFar)).onNext(any());
    }

    /** The one response sent so far. */
    private DeltaDiscoveryResponse onlyResponse() {
        verify(responseObserver).onNext(responseCaptor.capture());
        return responseCaptor.getValue();
    }

    private DeltaDiscoveryResponse nextResponse(InOrder inOrder) {
        inOrder.verify(responseObserver).onNext(responseCaptor.capture());
        return responseCaptor.getValue();
    }

}
