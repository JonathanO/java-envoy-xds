package net.woaf.jono.envoy.xds.sotw;

import com.google.common.collect.ImmutableList;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.ClientConfigProvider;
import net.woaf.jono.envoy.xds.CommonDiscoveryRequest;
import net.woaf.jono.envoy.xds.CommonDiscoveryStreamObserver;
import net.woaf.jono.envoy.xds.DiscoveryServiceManagerFactory;
import net.woaf.jono.envoy.xds.api.XdsEventListener;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;

public class SotwAggregatedDiscoveryService<StateUpdT, DetailsT> {
    private final ClusterEventSource<StateUpdT> clusterManager;
    private final DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, StateUpdT, DetailsT> discoveryServiceManagerFactory;
    private final ClientConfigProvider<DetailsT> clientConfigSource;
    private final Supplier<DiscoveryServiceManagerMetrics> metricsFactory;
    private final ImmutableList<XdsEventListener<DetailsT>> listeners;

    public SotwAggregatedDiscoveryService(
        ClusterEventSource<StateUpdT> clusterManager,
        DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, StateUpdT, DetailsT> discoveryServiceManagerFactory,
        ClientConfigProvider<DetailsT> clientConfigSource,
        ImmutableList<XdsEventListener<DetailsT>> listeners,
        Supplier<DiscoveryServiceManagerMetrics> metricsFactory
    ) {
        this.clusterManager = clusterManager;
        this.discoveryServiceManagerFactory = discoveryServiceManagerFactory;
        this.clientConfigSource = clientConfigSource;
        this.metricsFactory = metricsFactory;
        this.listeners = listeners;
    }

    public StreamObserver<DiscoveryRequest> streamAggregatedResources(StreamObserver<DiscoveryResponse> responseObserver) {
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

    private CommonDiscoveryRequest<DiscoveryRequest> convertToCommonDiscoveryRequest(DiscoveryRequest request) {
        return CommonDiscoveryRequest.<DiscoveryRequest>builder()
                .typeUrl(request.getTypeUrl())
                .errorDetail(request.hasErrorDetail() ? request.getErrorDetail() : null)
                .node(request.getNode())
                .message(request)
                .build();
    }
}
