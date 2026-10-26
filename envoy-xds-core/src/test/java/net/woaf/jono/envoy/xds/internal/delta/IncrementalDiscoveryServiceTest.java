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

import net.woaf.jono.envoy.xds.internal.CommonDiscoveryRequest;
import net.woaf.jono.envoy.xds.internal.DiscoveryService;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceManager;
import net.woaf.jono.envoy.xds.internal.NoOpConfigBuilder;
import net.woaf.jono.envoy.xds.internal.NodeConfig;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
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

    @Test
    public void testUpdateNotSubscribedNoSendUpdates(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                                @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {

        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.EDS, responseObserver, configBuilder, nodeConfig, new SubListSubManager(nodeConfig));
        var initState = new DummyUpdate();

        ds.init(initState);
        ds.onNetworkUpdate(new DummyUpdate());
        ds.sendNetworkUpdatePre();
        ds.sendNetworkUpdatePost();

        // If envoy has never sent a request for EDS, we do not expect any attempt to build a response.
        // If we did return a response due to a leaky config builder (one that returns unasked for resources) then envoy would throw an error.
        verify(responseObserver, never()).onNext(any());
        verifyNoInteractions(configBuilder);
    }

    @Captor ArgumentCaptor<DeltaDiscoveryResponse> responseCaptor;

    @Test
    public void testSubscribeSubListButNoResourcesThenUpdates(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                                @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final DummyUpdate next = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);
        final ClusterLoadAssignment newCluster = ClusterLoadAssignment.newBuilder()
            .setClusterName("foo")
            .build();

        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(IncrementalConfigBuilder.Resources.<ClusterLoadAssignment>empty());
        when(configBuilder.addOrder(eq(next), any(), any())).thenReturn(new IncrementalConfigBuilder.Response<>(List.of(IncrementalConfigBuilder.NamedMessage.of(newCluster)), List.of()));
        when(configBuilder.removeOrder(eq(next), any(), any())).thenReturn(IncrementalConfigBuilder.Response.<ClusterLoadAssignment>empty());

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.EDS, responseObserver, configBuilder, nodeConfig, new SubListSubManager(nodeConfig));

        // Envoy sends an EDS request:
        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.EDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.EDS.getTypeUrl())
                .addResourceNamesSubscribe("foo")
                .build(), null));

        InOrder inOrder = inOrder(responseObserver);
        inOrder.verify(responseObserver).onNext(responseCaptor.capture());

        // Our initial state has no resources, so we should tell envoy that the resource (foo) that it asked for has been removed.
        DeltaDiscoveryResponse resp = responseCaptor.getValue();
        final String nonce = resp.getNonce();
        assertThat(resp.getRemovedResourcesList()).containsExactly("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(resp.getResourcesList()).isEmpty();

        assertThat(ds.awaitingAck()).isTrue();

        // Envoy acks the update, without changing the subs list.
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.EDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.EDS.getTypeUrl())
                .setResponseNonce(nonce)
                .build(), null));

        assertThat(ds.awaitingAck()).isFalse();

        ds.onNetworkUpdate(next);
        ds.sendNetworkUpdatePre();
        ds.sendNetworkUpdatePost();

        inOrder.verify(responseObserver).onNext(responseCaptor.capture());

        resp = responseCaptor.getValue();
        assertThat(resp.getRemovedResourcesList()).isEmpty();
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void requestNamingNothingGetsNoAnswer(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                                              @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.EDS, responseObserver, configBuilder, nodeConfig, new SubListSubManager(nodeConfig));

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends an EDS request that asks for nothing
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.EDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.EDS.getTypeUrl())
                .build(), null));

        // And again
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.EDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.EDS.getTypeUrl())
                .build(), null));

        // Neither changes a subscription, so neither needs an answer, and an empty one would hold up pushes until Envoy
        // ACKs it (SubListSubManager).
        verify(responseObserver, never()).onNext(any());
        assertThat(ds.awaitingAck()).isFalse();
        verifyNoInteractions(configBuilder);
    }

    @Test
    public void testOldWildcardSubNoState(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                   @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.CDS, responseObserver, configBuilder, nodeConfig, new WildcardSubManager(nodeConfig));

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for nothing, this should cause it to sub to wildcard.
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.CDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.CDS.getTypeUrl())
                .build(), null));

        verify(responseObserver).onNext(responseCaptor.capture());
        // We should tell envoy about everything
        DeltaDiscoveryResponse resp = responseCaptor.getValue();
        assertThat(resp.getResourcesList()).isEmpty();
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(resp.getRemovedResourcesList()).isEmpty();

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testOldWildcardSub(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                   @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        final Cluster newCluster = Cluster.newBuilder()
            .setName("foo")
            .build();

        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(newCluster))));

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.CDS, responseObserver, configBuilder, nodeConfig, new WildcardSubManager(nodeConfig));

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for nothing, this should cause it to sub to wildcard.
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.CDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.CDS.getTypeUrl())
                .build(), null));

        verify(responseObserver).onNext(responseCaptor.capture());
        // We should tell envoy about everything
        DeltaDiscoveryResponse resp = responseCaptor.getValue();
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(resp.getRemovedResourcesList()).isEmpty();

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testAskingAgain(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                   @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        final Cluster newCluster = Cluster.newBuilder()
            .setName("foo")
            .build();

        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(newCluster))));

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.EDS, responseObserver, configBuilder, nodeConfig, new SubListSubManager(nodeConfig));

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        String n;
        InOrder inOrder = inOrder(responseObserver);

        {
            ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.EDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                    .setTypeUrl(TypeUrl.EDS.getTypeUrl())
                    .addResourceNamesSubscribe("foo")
                    .build(), null));

            inOrder.verify(responseObserver).onNext(responseCaptor.capture());

            DeltaDiscoveryResponse resp = responseCaptor.getValue();
            assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
            assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
            assertThat(resp.getRemovedResourcesList()).isEmpty();
            n = resp.getNonce();

            assertThat(ds.awaitingAck()).isTrue();
        }

        {
            // Ask again, as if envoy has forgotten about the resource
            ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.EDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                    .setTypeUrl(TypeUrl.EDS.getTypeUrl())
                    .addResourceNamesSubscribe("foo")
                    .build(), null));

            inOrder.verify(responseObserver).onNext(responseCaptor.capture());
            DeltaDiscoveryResponse resp = responseCaptor.getValue();
            assertThat(resp.getNonce()).isNotEqualTo(n); // Validate we captured the right response and aren't just verifying the first one again!
            assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
            assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
            assertThat(resp.getRemovedResourcesList()).isEmpty();


            assertThat(ds.awaitingAck()).isTrue();
        }
    }

    @Test
    public void testNackIsAnAnswer(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                   @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {
        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);
        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(Cluster.newBuilder().setName("foo").build()))));
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.EDS, responseObserver, configBuilder, nodeConfig, new SubListSubManager(nodeConfig));
        ds.init(initState);

        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.EDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder().setTypeUrl(TypeUrl.EDS.getTypeUrl()).addResourceNamesSubscribe("foo").build(), null));
        verify(responseObserver).onNext(responseCaptor.capture());
        assertThat(ds.awaitingAck()).isTrue();

        DeltaDiscoveryRequest nack = DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.EDS.getTypeUrl())
            .setResponseNonce(responseCaptor.getValue().getNonce())
            .setErrorDetail(com.google.rpc.Status.newBuilder().setMessage("rejected"))
            .build();
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.EDS.getTypeUrl(), nack, nack.getErrorDetail()));

        assertThat(ds.awaitingAck()).isFalse();
    }

    @Test
    public void testNewWildcardSub(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                   @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        final Cluster newCluster = Cluster.newBuilder()
            .setName("foo")
            .build();

        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(newCluster))));

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.CDS, responseObserver, configBuilder, nodeConfig, new WildcardSubManager(nodeConfig));

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for "*", this should cause it to sub to wildcard.
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.CDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.CDS.getTypeUrl())
                .addResourceNamesSubscribe("*")
                .build(), null));

        verify(responseObserver).onNext(responseCaptor.capture());
        // We should tell envoy about everything
        DeltaDiscoveryResponse resp = responseCaptor.getValue();
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(resp.getRemovedResourcesList()).isEmpty();

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testExplicitSubscribeInWildcardModeIsAnswered(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                                              @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        final Cluster foo = Cluster.newBuilder()
            .setName("foo")
            .build();

        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenAnswer(inv -> {
            Predicate<String> filter = inv.getArgument(1);
            List<IncrementalConfigBuilder.NamedMessage<Cluster>> resources = new ArrayList<>();
            if (filter.test("foo")) {
                resources.add(IncrementalConfigBuilder.NamedMessage.of(foo));
            }
            return new IncrementalConfigBuilder.Resources<>(resources);
        });

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.CDS, responseObserver, configBuilder, nodeConfig, new WildcardSubManager(nodeConfig));
        ds.init(initState);

        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.CDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.CDS.getTypeUrl())
                .addResourceNamesSubscribe("*")
                .build(), null));
        verify(responseObserver).onNext(responseCaptor.capture());
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.CDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.CDS.getTypeUrl())
                .setResponseNonce(responseCaptor.getValue().getNonce())
                .build(), null));

        // On-demand style subscribe to a name that doesn't exist, without touching "*": the spec requires an answer
        // that removes it, or Envoy waits out initial_fetch_timeout.
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.CDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.CDS.getTypeUrl())
                .addResourceNamesSubscribe("missing")
                .build(), null));

        verify(responseObserver, times(2)).onNext(responseCaptor.capture());
        DeltaDiscoveryResponse resp = responseCaptor.getValue();
        assertThat(resp.getResourcesList()).isEmpty();
        assertThat(resp.getRemovedResourcesList()).containsExactly("missing");
    }

    @Test
    public void testRemovesInitialStateIfNotExists(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        final Cluster newCluster = Cluster.newBuilder()
            .setName("foo")
            .build();

        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(newCluster))));

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.CDS, responseObserver, configBuilder, nodeConfig, new WildcardSubManager(nodeConfig));

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for "*", this should cause it to sub to wildcard.
        // This time we also tell envoy we already now about foo, bar and baz - which is what happens when envoy already has state when it connects to ADS (e.g. it's reconnecting.)
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.CDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.CDS.getTypeUrl())
                .addResourceNamesSubscribe("*")
                .putInitialResourceVersions("foo", "1")
                .putInitialResourceVersions("bar", "1")
                .putInitialResourceVersions("baz", "1")
                .build(), null));

        verify(responseObserver).onNext(responseCaptor.capture());
        // We should tell envoy about everything we know about (which is just foo)
        DeltaDiscoveryResponse resp = responseCaptor.getValue();
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        // With no manager-level ACK barrier configured, stale resources from initial_resource_versions are removed
        // in the initial subscription response.
        assertThat(resp.getRemovedResourcesList()).containsExactlyInAnyOrder("bar", "baz");

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testDoesNotRemoveInitialStateIfNotSubscribed(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        final Cluster newCluster = Cluster.newBuilder()
            .setName("foo")
            .build();

        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(newCluster))));

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(TypeUrl.CDS, responseObserver, configBuilder, nodeConfig, new WildcardSubManager(nodeConfig));

        ds.init(initState);
        assertThat(ds.awaitingAck()).isFalse();

        // Envoy sends a CDS request that asks for "foo", this should cause it to sub to the specific resource.
        // This time we also tell envoy we already now about foo, bar and baz - which is what happens when envoy already has state when it connects to ADS (e.g. it's reconnecting.)
        ds.processUpdate(new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, TypeUrl.CDS.getTypeUrl(), DeltaDiscoveryRequest.newBuilder()
                .setTypeUrl(TypeUrl.CDS.getTypeUrl())
                .addResourceNamesSubscribe("foo")
                .addResourceNamesSubscribe("bar")
                .putInitialResourceVersions("foo", "1")
                .putInitialResourceVersions("bar", "1")
                .putInitialResourceVersions("baz", "1")
                .build(), null));

        verify(responseObserver).onNext(responseCaptor.capture());
        // We should tell envoy about the thing it asked for that still exists (foo)
        DeltaDiscoveryResponse resp = responseCaptor.getValue();
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("foo");
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        // Envoy asked for bar, and reported that it already knows bar in initial_resource_versions.
        // With no manager-level ACK barrier configured, that stale subscribed resource is removed immediately.
        // Baz exists in the initial versions, but the client has not asked to subscribe to it, so we don't remove it either.
        assertThat(resp.getRemovedResourcesList()).containsExactly("bar");

        assertThat(ds.awaitingAck()).isTrue();
    }

    @Test
    public void testReconnectInitialResourceVersionRemovalsWaitForConfiguredAck(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        final RouteConfiguration routeWithoutDeletedCluster = RouteConfiguration.newBuilder()
            .setName("route-x")
            .build();

        when(clusterConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());
        when(routeConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(routeWithoutDeletedCluster))));

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> cds = new IncrementalDiscoveryService<>(
            TypeUrl.CDS,
            responseObserver,
            clusterConfigBuilder,
            nodeConfig,
            new WildcardSubManager(nodeConfig)
        );
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> rds = new IncrementalDiscoveryService<>(
            TypeUrl.RDS,
            responseObserver,
            routeConfigBuilder,
            nodeConfig,
            new SubListSubManager(nodeConfig)
        );
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> discoveryServiceManager = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds, TypeUrl.RDS, rds),
            List.of(TypeUrl.CDS, TypeUrl.RDS),
            List.of(TypeUrl.RDS, TypeUrl.CDS),
            QueueingStateBacklog.<DummyUpdate>factory().build(),
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );

        discoveryServiceManager.init(initState, TypeUrl.RDS);
        InOrder inOrder = inOrder(responseObserver);

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.CDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.CDS.getTypeUrl())
            .addResourceNamesSubscribe("*")
            .putInitialResourceVersions("cluster-a", "1")
            .build()));

        inOrder.verify(responseObserver).onNext(responseCaptor.capture());
        DeltaDiscoveryResponse cdsReconnectResponse = responseCaptor.getValue();
        assertThat(cdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsReconnectResponse.getResourcesList()).isEmpty();
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.CDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.CDS.getTypeUrl())
            .setResponseNonce(cdsReconnectResponse.getNonce())
            .build()));

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.RDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.RDS.getTypeUrl())
            .addResourceNamesSubscribe("route-x")
            .putInitialResourceVersions("route-x", "1")
            .build()));

        inOrder.verify(responseObserver).onNext(responseCaptor.capture());
        DeltaDiscoveryResponse rdsReconnectResponse = responseCaptor.getValue();
        assertThat(rdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.RDS.getTypeUrl());
        assertThat(rdsReconnectResponse.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("route-x");
        assertThat(rdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.RDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.RDS.getTypeUrl())
            .setResponseNonce(rdsReconnectResponse.getNonce())
            .build()));

        inOrder.verify(responseObserver).onNext(responseCaptor.capture());
        DeltaDiscoveryResponse cdsDeferredRemovalResponse = responseCaptor.getValue();
        assertThat(cdsDeferredRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsDeferredRemovalResponse.getResourcesList()).isEmpty();
        assertThat(cdsDeferredRemovalResponse.getRemovedResourcesList()).containsExactly("cluster-a");
    }

    @Test
    public void testNoConfiguredDelaySendsInitialResourceVersionRemovalsInInitialResponse(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        when(clusterConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> cds = new IncrementalDiscoveryService<>(
            TypeUrl.CDS,
            responseObserver,
            clusterConfigBuilder,
            nodeConfig,
            new WildcardSubManager(nodeConfig)
        );
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> discoveryServiceManager = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds),
            List.of(TypeUrl.CDS),
            List.of(TypeUrl.CDS),
            QueueingStateBacklog.<DummyUpdate>factory().build(),
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );

        discoveryServiceManager.init(initState);
        InOrder inOrder = inOrder(responseObserver);

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.CDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.CDS.getTypeUrl())
            .addResourceNamesSubscribe("*")
            .putInitialResourceVersions("cluster-a", "1")
            .build()));

        DeltaDiscoveryResponse reconnectResponse = nextResponse(inOrder, responseObserver);
        assertThat(reconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(reconnectResponse.getResourcesList()).isEmpty();
        assertThat(reconnectResponse.getRemovedResourcesList()).containsExactly("cluster-a");
        assertThat(cds.hasDeferredReconnectRemovals()).isFalse();

        discoveryServiceManager.processUpdate(ackRequest(TypeUrl.CDS, reconnectResponse.getNonce()));
        assertNoAdditionalResponsesSent(responseObserver, 1);
    }

    /** Builds CDS, EDS and RDS services over one stream, and a manager for them. */
    private DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> cdsEdsRds(StreamObserver<DeltaDiscoveryResponse> responseObserver,
        IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusters, IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpoints,
        IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routes) {
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);
        return new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, new IncrementalDiscoveryService<>(TypeUrl.CDS, responseObserver, clusters, nodeConfig, new WildcardSubManager(nodeConfig)),
                TypeUrl.EDS, new IncrementalDiscoveryService<>(TypeUrl.EDS, responseObserver, endpoints, nodeConfig, new SubListSubManager(nodeConfig)),
                TypeUrl.RDS, new IncrementalDiscoveryService<>(TypeUrl.RDS, responseObserver, routes, nodeConfig, new SubListSubManager(nodeConfig))),
            TypeUrl.ADD_ORDER,
            TypeUrl.REMOVE_ORDER,
            QueueingStateBacklog.<DummyUpdate>factory().build(),
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> reconnectSubscription(TypeUrl typeUrl, String subscribe, String kept) {
        DeltaDiscoveryRequest.Builder request = DeltaDiscoveryRequest.newBuilder().setTypeUrl(typeUrl.getTypeUrl()).addResourceNamesSubscribe(subscribe);
        if (kept != null) {
            request.putInitialResourceVersions(kept, "1");
        }
        return deltaRequest(typeUrl, request.build());
    }

    @Test
    public void testStaleClusterRemovalWaitsForRoutes(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        final DummyUpdate initState = new DummyUpdate();
        when(clusterConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());
        when(routeConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(IncrementalConfigBuilder.Resources.<RouteConfiguration>empty());
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = cdsEdsRds(responseObserver, clusterConfigBuilder, endpointConfigBuilder, routeConfigBuilder);
        dsm.init(initState);
        InOrder inOrder = inOrder(responseObserver);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-stale"));
        DeltaDiscoveryResponse cdsReconnectResponse = nextResponse(inOrder, responseObserver);
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).as("routes may still use it").isEmpty();
        dsm.processUpdate(ackRequest(TypeUrl.CDS, cdsReconnectResponse.getNonce()));
        assertNoAdditionalResponsesSent(responseObserver, 1);

        dsm.processUpdate(reconnectSubscription(TypeUrl.RDS, "route-stale", "route-stale"));
        DeltaDiscoveryResponse rdsReconnectResponse = nextResponse(inOrder, responseObserver);
        assertThat(rdsReconnectResponse.getRemovedResourcesList()).containsExactly("route-stale");
        dsm.processUpdate(ackRequest(TypeUrl.RDS, rdsReconnectResponse.getNonce()));

        DeltaDiscoveryResponse cdsRemovalResponse = nextResponse(inOrder, responseObserver);
        assertThat(cdsRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsRemovalResponse.getRemovedResourcesList()).containsExactly("cluster-stale");
    }

    @Test
    public void testStaleClusterRemovalDoesNotWaitForTypesTheClientDoesNotUse(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        final DummyUpdate initState = new DummyUpdate();
        when(clusterConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = cdsEdsRds(responseObserver, clusterConfigBuilder, endpointConfigBuilder, routeConfigBuilder);
        dsm.init(initState, null, Set.of(TypeUrl.CDS, TypeUrl.EDS)); // static routes

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-stale"));
        assertThat(nextResponse(inOrder(responseObserver), responseObserver).getRemovedResourcesList()).containsExactly("cluster-stale");
    }

    @Test
    public void testStaleClusterRemovalDoesNotWaitForTypesWeDoNotServe(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder) {
        final DummyUpdate initState = new DummyUpdate();
        when(clusterConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);
        // As the factory builds it: every type has a service, with a NoOpConfigBuilder for those nobody configured.
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, new IncrementalDiscoveryService<>(TypeUrl.CDS, responseObserver, clusterConfigBuilder, nodeConfig, new WildcardSubManager(nodeConfig)),
                TypeUrl.RDS, new IncrementalDiscoveryService<>(TypeUrl.RDS, responseObserver, new NoOpConfigBuilder<RouteConfiguration, DummyUpdate, Object>(RouteConfiguration.class),
                    nodeConfig, new SubListSubManager(nodeConfig))),
            TypeUrl.ADD_ORDER,
            TypeUrl.REMOVE_ORDER,
            QueueingStateBacklog.<DummyUpdate>factory().build(),
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );
        dsm.init(initState);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-stale"));
        assertThat(nextResponse(inOrder(responseObserver), responseObserver).getRemovedResourcesList()).containsExactly("cluster-stale");
    }

    @Test
    public void testStaleRemovalsGoAfterTheTimeoutAtTheNextEvent(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        final DummyUpdate initState = new DummyUpdate();
        when(clusterConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = cdsEdsRds(responseObserver, clusterConfigBuilder, endpointConfigBuilder, routeConfigBuilder);
        AtomicLong now = new AtomicLong();
        dsm.setClock(now::get);
        dsm.init(initState);
        InOrder inOrder = inOrder(responseObserver);

        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", "cluster-stale"));
        DeltaDiscoveryResponse cdsReconnectResponse = nextResponse(inOrder, responseObserver);
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        now.addAndGet(DiscoveryServiceManager.WAIT_FOR_ACK_TIMEOUT_NANOS); // RDS never subscribes
        dsm.processUpdate(ackRequest(TypeUrl.CDS, cdsReconnectResponse.getNonce()));
        assertThat(nextResponse(inOrder, responseObserver).getRemovedResourcesList()).containsExactly("cluster-stale");
    }

    @Test
    public void testRoutesWaitForTheClustersAndEndpointsTheyMayUse(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {
        final DummyUpdate initState = new DummyUpdate();
        when(clusterConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(Cluster.newBuilder().setName("new").build()))));
        when(endpointConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(ClusterLoadAssignment.newBuilder().setClusterName("new").build()))));
        when(routeConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any())).thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(RouteConfiguration.newBuilder().setName("routes").build()))));
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> dsm = cdsEdsRds(responseObserver, clusterConfigBuilder, endpointConfigBuilder, routeConfigBuilder);
        dsm.init(initState);
        InOrder inOrder = inOrder(responseObserver);

        // Envoy's reconnect burst: everything at once.
        dsm.processUpdate(reconnectSubscription(TypeUrl.CDS, "*", null));
        final DeltaDiscoveryResponse cds = nextResponse(inOrder, responseObserver);
        dsm.processUpdate(reconnectSubscription(TypeUrl.RDS, "routes", "routes"));
        assertNoAdditionalResponsesSent(responseObserver, 1); // held: the clusters it may use aren't ACKed
        dsm.processUpdate(reconnectSubscription(TypeUrl.EDS, "new", null));
        DeltaDiscoveryResponse eds = nextResponse(inOrder, responseObserver);
        assertThat(eds.getTypeUrl()).as("EDS is never held: Envoy's CDS ACK waits for it").isEqualTo(TypeUrl.EDS.getTypeUrl());

        dsm.processUpdate(ackRequest(TypeUrl.CDS, cds.getNonce()));
        assertNoAdditionalResponsesSent(responseObserver, 2);
        dsm.processUpdate(ackRequest(TypeUrl.EDS, eds.getNonce()));
        DeltaDiscoveryResponse rds = nextResponse(inOrder, responseObserver);
        assertThat(rds.getTypeUrl()).isEqualTo(TypeUrl.RDS.getTypeUrl());
        assertThat(rds.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("routes");
    }

    @Test
    public void testReconnectNetworkUpdatesWaitForDeferredInitialRemovals(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final DummyUpdate nextState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        final RouteConfiguration routeWithoutDeletedCluster = RouteConfiguration.newBuilder()
            .setName("route-x")
            .build();
        final Cluster newCluster = Cluster.newBuilder()
            .setName("cluster-b")
            .build();

        when(clusterConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());
        when(routeConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(routeWithoutDeletedCluster))));
        when(clusterConfigBuilder.addOrder(eq(nextState), any(), any()))
            .thenReturn(new IncrementalConfigBuilder.Response<>(List.of(IncrementalConfigBuilder.NamedMessage.of(newCluster)), List.of()));

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> cds = new IncrementalDiscoveryService<>(
            TypeUrl.CDS,
            responseObserver,
            clusterConfigBuilder,
            nodeConfig,
            new WildcardSubManager(nodeConfig)
        );
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> rds = new IncrementalDiscoveryService<>(
            TypeUrl.RDS,
            responseObserver,
            routeConfigBuilder,
            nodeConfig,
            new SubListSubManager(nodeConfig)
        );
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> discoveryServiceManager = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds, TypeUrl.RDS, rds),
            List.of(TypeUrl.CDS, TypeUrl.RDS),
            List.of(TypeUrl.RDS, TypeUrl.CDS),
            QueueingStateBacklog.<DummyUpdate>factory().build(),
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );

        discoveryServiceManager.init(initState, TypeUrl.RDS);
        InOrder inOrder = inOrder(responseObserver);

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.CDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.CDS.getTypeUrl())
            .addResourceNamesSubscribe("*")
            .putInitialResourceVersions("cluster-a", "1")
            .build()));
        DeltaDiscoveryResponse cdsReconnectResponse = nextResponse(inOrder, responseObserver);
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        discoveryServiceManager.processUpdate(ackRequest(TypeUrl.CDS, cdsReconnectResponse.getNonce()));

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.RDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.RDS.getTypeUrl())
            .addResourceNamesSubscribe("route-x")
            .putInitialResourceVersions("route-x", "1")
            .build()));
        DeltaDiscoveryResponse rdsReconnectResponse = nextResponse(inOrder, responseObserver);
        assertThat(rdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.RDS.getTypeUrl());
        assertThat(rdsReconnectResponse.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("route-x");
        assertThat(rdsReconnectResponse.getRemovedResourcesList()).isEmpty();

        discoveryServiceManager.pushUpdates(nextState);
        verify(responseObserver, times(2)).onNext(any());

        discoveryServiceManager.processUpdate(ackRequest(TypeUrl.RDS, rdsReconnectResponse.getNonce()));

        DeltaDiscoveryResponse cdsDeferredRemovalResponse = nextResponse(inOrder, responseObserver);
        assertThat(cdsDeferredRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsDeferredRemovalResponse.getResourcesList()).isEmpty();
        assertThat(cdsDeferredRemovalResponse.getRemovedResourcesList()).containsExactly("cluster-a");

        discoveryServiceManager.processUpdate(ackRequest(TypeUrl.CDS, cdsDeferredRemovalResponse.getNonce()));

        DeltaDiscoveryResponse cdsNetworkUpdateResponse = nextResponse(inOrder, responseObserver);
        assertThat(cdsNetworkUpdateResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsNetworkUpdateResponse.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("cluster-b");
        assertThat(cdsNetworkUpdateResponse.getRemovedResourcesList()).isEmpty();
    }

    @Test
    public void testDeferredReconnectRemovalsUseRemoveOrderAndWaitForAck(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<Cluster, DummyUpdate, Object> clusterConfigBuilder,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> endpointConfigBuilder,
        @Mock IncrementalConfigBuilder<RouteConfiguration, DummyUpdate, Object> routeConfigBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);

        final RouteConfiguration currentRoute = RouteConfiguration.newBuilder()
            .setName("route-current")
            .build();

        when(routeConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(currentRoute))));
        when(clusterConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(IncrementalConfigBuilder.Resources.<Cluster>empty());
        when(endpointConfigBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(IncrementalConfigBuilder.Resources.<ClusterLoadAssignment>empty());

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> cds = new IncrementalDiscoveryService<>(
            TypeUrl.CDS,
            responseObserver,
            clusterConfigBuilder,
            nodeConfig,
            new WildcardSubManager(nodeConfig)
        );
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> eds = new IncrementalDiscoveryService<>(
            TypeUrl.EDS,
            responseObserver,
            endpointConfigBuilder,
            nodeConfig,
            new SubListSubManager(nodeConfig)
        );
        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> rds = new IncrementalDiscoveryService<>(
            TypeUrl.RDS,
            responseObserver,
            routeConfigBuilder,
            nodeConfig,
            new SubListSubManager(nodeConfig)
        );
        DiscoveryServiceManager<DeltaDiscoveryRequest, DummyUpdate> discoveryServiceManager = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds, TypeUrl.EDS, eds, TypeUrl.RDS, rds),
            List.of(TypeUrl.CDS, TypeUrl.EDS, TypeUrl.RDS),
            List.of(TypeUrl.RDS, TypeUrl.CDS, TypeUrl.EDS),
            QueueingStateBacklog.<DummyUpdate>factory().build(),
            DiscoveryServiceManagerMetrics.NOOP_METRICS
        );

        discoveryServiceManager.init(initState, TypeUrl.RDS);
        InOrder inOrder = inOrder(responseObserver);

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.CDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.CDS.getTypeUrl())
            .addResourceNamesSubscribe("*")
            .putInitialResourceVersions("cluster-stale", "1")
            .build()));
        DeltaDiscoveryResponse cdsReconnectResponse = nextResponse(inOrder, responseObserver);
        assertThat(cdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsReconnectResponse.getResourcesList()).isEmpty();
        assertThat(cdsReconnectResponse.getRemovedResourcesList()).isEmpty();
        discoveryServiceManager.processUpdate(ackRequest(TypeUrl.CDS, cdsReconnectResponse.getNonce()));

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.EDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.EDS.getTypeUrl())
            .addResourceNamesSubscribe("endpoint-stale")
            .putInitialResourceVersions("endpoint-stale", "1")
            .build()));
        DeltaDiscoveryResponse edsReconnectResponse = nextResponse(inOrder, responseObserver);
        assertThat(edsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(edsReconnectResponse.getResourcesList()).isEmpty();
        assertThat(edsReconnectResponse.getRemovedResourcesList()).isEmpty();
        discoveryServiceManager.processUpdate(ackRequest(TypeUrl.EDS, edsReconnectResponse.getNonce()));

        discoveryServiceManager.processUpdate(deltaRequest(TypeUrl.RDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.RDS.getTypeUrl())
            .addResourceNamesSubscribe("route-current")
            .addResourceNamesSubscribe("route-stale")
            .putInitialResourceVersions("route-stale", "1")
            .build()));
        DeltaDiscoveryResponse rdsReconnectResponse = nextResponse(inOrder, responseObserver);
        assertThat(rdsReconnectResponse.getTypeUrl()).isEqualTo(TypeUrl.RDS.getTypeUrl());
        assertThat(rdsReconnectResponse.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("route-current");
        // Nothing before RDS in remove order is served, so its stale route config goes straight away.
        assertThat(rdsReconnectResponse.getRemovedResourcesList()).containsExactly("route-stale");
        assertNoAdditionalResponsesSent(responseObserver, 3);

        discoveryServiceManager.processUpdate(ackRequest(TypeUrl.RDS, rdsReconnectResponse.getNonce()));

        DeltaDiscoveryResponse cdsRemovalResponse = nextResponse(inOrder, responseObserver);
        assertThat(cdsRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        assertThat(cdsRemovalResponse.getResourcesList()).isEmpty();
        assertThat(cdsRemovalResponse.getRemovedResourcesList()).containsExactly("cluster-stale");
        assertNoAdditionalResponsesSent(responseObserver, 4);

        discoveryServiceManager.processUpdate(nonAckRequest(TypeUrl.CDS));
        assertNoAdditionalResponsesSent(responseObserver, 4);

        discoveryServiceManager.processUpdate(ackRequest(TypeUrl.CDS, cdsRemovalResponse.getNonce()));

        DeltaDiscoveryResponse edsRemovalResponse = nextResponse(inOrder, responseObserver);
        assertThat(edsRemovalResponse.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(edsRemovalResponse.getResourcesList()).isEmpty();
        assertThat(edsRemovalResponse.getRemovedResourcesList()).containsExactly("endpoint-stale");
    }

    @Test
    public void testDoesNotDeferInitialStateRemovalForUnsubscribedSubListResource(@Mock StreamObserver<DeltaDiscoveryResponse> responseObserver,
        @Mock IncrementalConfigBuilder<ClusterLoadAssignment, DummyUpdate, Object> configBuilder) {

        final DummyUpdate initState = new DummyUpdate();
        final NodeConfig<Object> nodeConfig = new NodeConfig<>(XdsConfig.of(new Object()), false);
        final ClusterLoadAssignment currentEndpoint = ClusterLoadAssignment.newBuilder()
            .setClusterName("endpoint-current")
            .build();

        when(configBuilder.getResourcesRemoveOrder(eq(initState), any(), any()))
            .thenReturn(new IncrementalConfigBuilder.Resources<>(List.of(IncrementalConfigBuilder.NamedMessage.of(currentEndpoint))));

        DiscoveryService<DeltaDiscoveryRequest, DummyUpdate> ds = new IncrementalDiscoveryService<>(
            TypeUrl.EDS,
            responseObserver,
            configBuilder,
            nodeConfig,
            new SubListSubManager(nodeConfig)
        );

        ds.init(initState);
        ds.processUpdate(deltaRequest(TypeUrl.EDS, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(TypeUrl.EDS.getTypeUrl())
            .addResourceNamesSubscribe("endpoint-current")
            .putInitialResourceVersions("endpoint-unsubscribed", "1")
            .build()));

        verify(responseObserver).onNext(responseCaptor.capture());
        DeltaDiscoveryResponse resp = responseCaptor.getValue();
        assertThat(resp.getTypeUrl()).isEqualTo(TypeUrl.EDS.getTypeUrl());
        assertThat(resp.getResourcesList()).hasSize(1).first().extracting(Resource::getName).isEqualTo("endpoint-current");
        assertThat(resp.getRemovedResourcesList()).isEmpty();
        assertThat(ds.hasDeferredReconnectRemovals()).isFalse();
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> deltaRequest(TypeUrl typeUrl, DeltaDiscoveryRequest request) {
        return new CommonDiscoveryRequest<DeltaDiscoveryRequest>(null, typeUrl.getTypeUrl(), request, null);
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> ackRequest(TypeUrl typeUrl, String nonce) {
        return deltaRequest(typeUrl, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(typeUrl.getTypeUrl())
            .setResponseNonce(nonce)
            .build());
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> nonAckRequest(TypeUrl typeUrl) {
        return deltaRequest(typeUrl, DeltaDiscoveryRequest.newBuilder()
            .setTypeUrl(typeUrl.getTypeUrl())
            .build());
    }

    private void assertNoAdditionalResponsesSent(StreamObserver<DeltaDiscoveryResponse> responseObserver, int expectedResponsesSoFar) {
        verify(responseObserver, times(expectedResponsesSoFar)).onNext(any());
    }

    private DeltaDiscoveryResponse nextResponse(InOrder inOrder, StreamObserver<DeltaDiscoveryResponse> responseObserver) {
        inOrder.verify(responseObserver).onNext(responseCaptor.capture());
        return responseCaptor.getValue();
    }

}
