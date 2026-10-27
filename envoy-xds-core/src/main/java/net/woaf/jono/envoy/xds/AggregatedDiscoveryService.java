package net.woaf.jono.envoy.xds;

import net.woaf.jono.envoy.xds.internal.CommonDiscoveryRequest;
import net.woaf.jono.envoy.xds.internal.CommonDiscoveryStreamObserver;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceManagerFactory;
import com.google.common.collect.ImmutableList;
import net.woaf.jono.envoy.xds.api.ClientConfigProvider;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.api.StateBacklogFactory;
import net.woaf.jono.envoy.xds.api.XdsEventListener;
import net.woaf.jono.envoy.xds.internal.delta.IncrementalDiscoveryServiceFactory;
import net.woaf.jono.envoy.xds.internal.sotw.SotwDiscoveryServiceFactory;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import io.envoyproxy.envoy.service.discovery.v3.AggregatedDiscoveryServiceGrpc;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;

import java.util.function.Supplier;

/**
 * The ADS gRPC service: add it to your gRPC server, and Envoys can connect over SotW or delta xDS. Each connection
 * subscribes to the {@link ClusterEventSource}, and pushes each change to that Envoy in make-before-break order.
 *
 * @param <StateUpdT> the state updates your ClusterEventSource publishes
 * @param <DetailsT> client details, from your ClientConfigProvider
 */
public class AggregatedDiscoveryService<StateUpdT, DetailsT> extends AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceImplBase {

    private final ClusterEventSource<StateUpdT> clusterManager;
    private final DiscoveryServiceManagerFactory<DiscoveryRequest, DiscoveryResponse, StateUpdT, DetailsT> sotwManagerFactory;
    private final DiscoveryServiceManagerFactory<DeltaDiscoveryRequest, DeltaDiscoveryResponse, StateUpdT, DetailsT> deltaManagerFactory;
    private final ClientConfigProvider<DetailsT> clientConfigSource;
    private final ImmutableList<XdsEventListener<DetailsT>> listeners;
    private final Supplier<DiscoveryServiceManagerMetrics> metricsFactory;

    /**
     * Serves both protocols from one set of config builders.
     * @param clusterManager tells each connection about changes to the desired config
     * @param configBuilders one per resource type served: turn state updates into resources
     * @param clientConfigSource config for each Envoy, looked up from its node when it connects
     * @param listeners told when Envoys connect, disconnect and NACK
     * @param stateBacklogFactory how a connection holds the changes that arrive while it waits for an Envoy's ACKs,
     *     e.g. MergingStateBacklog
     * @param metricsFactory metrics for each connection, e.g. DiscoveryServiceManagerMetrics.METRICS_DISABLED
     */
    public AggregatedDiscoveryService(
        ClusterEventSource<StateUpdT> clusterManager,
        ImmutableList<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders,
        ClientConfigProvider<DetailsT> clientConfigSource,
        ImmutableList<XdsEventListener<DetailsT>> listeners,
        StateBacklogFactory<StateUpdT> stateBacklogFactory,
        Supplier<DiscoveryServiceManagerMetrics> metricsFactory
    ) {
        this.clusterManager = clusterManager;
        this.sotwManagerFactory = new DiscoveryServiceManagerFactory<>(new SotwDiscoveryServiceFactory<>(configBuilders), stateBacklogFactory);
        this.deltaManagerFactory = new DiscoveryServiceManagerFactory<>(new IncrementalDiscoveryServiceFactory<>(configBuilders), stateBacklogFactory);
        this.clientConfigSource = clientConfigSource;
        this.listeners = listeners;
        this.metricsFactory = metricsFactory;
    }

    @Override
    public StreamObserver<DiscoveryRequest> streamAggregatedResources(StreamObserver<DiscoveryResponse> responseObserver) {
        return new CommonDiscoveryStreamObserver<>(responseObserver, clusterManager, sotwManagerFactory,
            r -> new CommonDiscoveryRequest<>(r.getNode(), r.getTypeUrl(), r, r.hasErrorDetail() ? r.getErrorDetail() : null),
            clientConfigSource, listeners, metricsFactory.get());
    }

    @Override
    public StreamObserver<DeltaDiscoveryRequest> deltaAggregatedResources(StreamObserver<DeltaDiscoveryResponse> responseObserver) {
        return new CommonDiscoveryStreamObserver<>(responseObserver, clusterManager, deltaManagerFactory,
            r -> new CommonDiscoveryRequest<>(r.getNode(), r.getTypeUrl(), r, r.hasErrorDetail() ? r.getErrorDetail() : null),
            clientConfigSource, listeners, metricsFactory.get());
    }

}
