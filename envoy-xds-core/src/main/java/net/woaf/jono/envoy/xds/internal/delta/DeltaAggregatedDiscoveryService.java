package net.woaf.jono.envoy.xds.internal.delta;

import com.google.common.collect.ImmutableList;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.ClientConfigProvider;
import net.woaf.jono.envoy.xds.internal.CommonDiscoveryRequest;
import net.woaf.jono.envoy.xds.internal.CommonDiscoveryStreamObserver;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceManagerFactory;
import net.woaf.jono.envoy.xds.api.XdsEventListener;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;

public class DeltaAggregatedDiscoveryService<StateUpdT, DetailsT> {
    private final ClusterEventSource<StateUpdT> clusterManager;
    private final DiscoveryServiceManagerFactory<DeltaDiscoveryRequest, DeltaDiscoveryResponse, StateUpdT, DetailsT> discoveryServiceManagerFactory;
    private final ClientConfigProvider<DetailsT> clientConfigSource;
    private final Supplier<DiscoveryServiceManagerMetrics> metricsFactory;
    private final ImmutableList<XdsEventListener<DetailsT>> listeners;

    public DeltaAggregatedDiscoveryService(ClusterEventSource<StateUpdT> clusterManager,
            DiscoveryServiceManagerFactory<DeltaDiscoveryRequest, DeltaDiscoveryResponse, StateUpdT, DetailsT> discoveryServiceManagerFactory,
            ClientConfigProvider<DetailsT> clientConfigSource,
            ImmutableList<XdsEventListener<DetailsT>> listeners,
            Supplier<DiscoveryServiceManagerMetrics> metricsFactory) {
        this.clusterManager = clusterManager;
        this.discoveryServiceManagerFactory = discoveryServiceManagerFactory;
        this.clientConfigSource = clientConfigSource;
        this.metricsFactory = metricsFactory;
        this.listeners = listeners;
    }

    public StreamObserver<DeltaDiscoveryRequest> streamDeltaAggregatedResources(StreamObserver<DeltaDiscoveryResponse> responseObserver) {
        return new CommonDiscoveryStreamObserver<>(
            responseObserver,
            clusterManager,
            discoveryServiceManagerFactory,
            this::convertToCommonDiscoveryRequest,
            clientConfigSource,
            listeners,
            metricsFactory.get()
        );
    }

    private CommonDiscoveryRequest<DeltaDiscoveryRequest> convertToCommonDiscoveryRequest(DeltaDiscoveryRequest request) {
        return new CommonDiscoveryRequest<>(request.getNode(), request.getTypeUrl(), request,
            request.hasErrorDetail() ? request.getErrorDetail() : null);
    }
}
