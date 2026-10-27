package net.woaf.jono.envoy.xds.internal.delta;

import com.google.common.collect.ImmutableList;
import net.woaf.jono.envoy.xds.internal.NodeConfig;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.internal.DiscoveryService;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceFactory;
import net.woaf.jono.envoy.xds.TypeUrl;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.grpc.stub.StreamObserver;

public class IncrementalDiscoveryServiceFactory<StateUpdT, DetailsT> extends DiscoveryServiceFactory<DeltaDiscoveryRequest, DeltaDiscoveryResponse, StateUpdT, DetailsT> {

    public IncrementalDiscoveryServiceFactory(ImmutableList<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders) {
        super(configBuilders);
    }

    private SubManager getSubmanagerForType(TypeUrl type, NodeConfig<DetailsT> nodeConfig) {
        if (type.isWildcard()) {
            return new WildcardSubManager(nodeConfig);
        } else {
            return new SubListSubManager(nodeConfig);
        }
    }

    @Override
    public DiscoveryService<DeltaDiscoveryRequest, StateUpdT> createDiscoveryService(StreamObserver<DeltaDiscoveryResponse> responseObserver, NodeConfig<DetailsT> nodeConfig, TypeUrl type) {
        return new IncrementalDiscoveryService<>(type, responseObserver, getConfigBuilderForType(type), nodeConfig, getSubmanagerForType(type, nodeConfig));
    }

}
