package net.woaf.jono.envoy.xds;

import com.google.common.collect.ImmutableList;
import net.woaf.jono.envoy.xds.api.ClientConfigProvider;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.api.StateBacklogFactory;
import net.woaf.jono.envoy.xds.api.XdsEventListener;
import net.woaf.jono.envoy.xds.delta.DeltaAggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.delta.IncrementalDiscoveryServiceFactory;
import net.woaf.jono.envoy.xds.sotw.SotwAggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.sotw.SotwDiscoveryServiceFactory;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import io.envoyproxy.envoy.service.discovery.v3.AggregatedDiscoveryServiceGrpc;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.function.Supplier;

public class AggregatedDiscoveryService<StateUpdT, DetailsT> extends AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceImplBase {

    private final SotwAggregatedDiscoveryService<StateUpdT, DetailsT> sotwAds;
    private final DeltaAggregatedDiscoveryService<StateUpdT, DetailsT> incrementalAds;

    public AggregatedDiscoveryService(SotwAggregatedDiscoveryService<StateUpdT, DetailsT> sotwAds, DeltaAggregatedDiscoveryService<StateUpdT, DetailsT> incrementalAds) {
        this.sotwAds = sotwAds;
        this.incrementalAds = incrementalAds;
    }

    public AggregatedDiscoveryService(
        ClusterEventSource<StateUpdT> clusterManager,
        ImmutableList<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders,
        ClientConfigProvider<DetailsT> clientConfigSource,
        ImmutableList<XdsEventListener<DetailsT>> listeners,
        StateBacklogFactory<StateUpdT> stateBacklogFactory,
        Supplier<DiscoveryServiceManagerMetrics> metricsFactory
    ) {
        this(
            sotwAggregatedDiscoveryService(clusterManager, configBuilders, clientConfigSource, listeners, stateBacklogFactory, metricsFactory),
            deltaAggregatedDiscoveryService(clusterManager, configBuilders, clientConfigSource, listeners, stateBacklogFactory, metricsFactory)
        );
    }

    @Override
    public StreamObserver<DiscoveryRequest> streamAggregatedResources(StreamObserver<DiscoveryResponse> responseObserver) {
        return sotwAds.streamAggregatedResources(responseObserver);
    }

    @Override
    public StreamObserver<DeltaDiscoveryRequest> deltaAggregatedResources(StreamObserver<DeltaDiscoveryResponse> responseObserver) {
        return incrementalAds.streamDeltaAggregatedResources(responseObserver);
    }

    public static <StateUpdT, DetailsT> SotwAggregatedDiscoveryService<StateUpdT, DetailsT> sotwAggregatedDiscoveryService(
        ClusterEventSource<StateUpdT> clusterManager,
        ImmutableList<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders,
        ClientConfigProvider<DetailsT> clientConfigSource,
        ImmutableList<XdsEventListener<DetailsT>> listeners,
        StateBacklogFactory<StateUpdT> stateBacklogFactory,
        Supplier<DiscoveryServiceManagerMetrics> metricsFactory
    ) {
        var discoveryServiceManagerFactory = new DiscoveryServiceManagerFactory<>(new SotwDiscoveryServiceFactory<>(configBuilders), stateBacklogFactory);
        return new SotwAggregatedDiscoveryService<>(clusterManager, discoveryServiceManagerFactory, clientConfigSource, listeners, metricsFactory);
    }

    public static <StateUpdT, DetailsT> DeltaAggregatedDiscoveryService<StateUpdT, DetailsT> deltaAggregatedDiscoveryService(
        ClusterEventSource<StateUpdT> clusterManager,
        List<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders,
        ClientConfigProvider<DetailsT> clientConfigSource,
        ImmutableList<XdsEventListener<DetailsT>> listeners,
        StateBacklogFactory<StateUpdT> stateBacklogFactory,
        Supplier<DiscoveryServiceManagerMetrics> metricsFactory
    ) {
        var discoveryServiceManagerFactory = new DiscoveryServiceManagerFactory<>(new IncrementalDiscoveryServiceFactory<>(configBuilders), stateBacklogFactory);
        return new DeltaAggregatedDiscoveryService<>(clusterManager, discoveryServiceManagerFactory, clientConfigSource, listeners, metricsFactory);
    }

}
