package net.woaf.jono.envoy.xds.internal.delta;

import com.google.common.collect.ImmutableList;
import net.woaf.jono.envoy.xds.XdsConfig;
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

    private SubManager getSubmanagerForType(TypeUrl type) {
        if (type.isWildcard()) {
            return new WildcardSubManager();
        } else {
            return new SubListSubManager();
        }
    }

    @Override
    public DiscoveryService<DeltaDiscoveryRequest, StateUpdT> createDiscoveryService(StreamObserver<DeltaDiscoveryResponse> responseObserver, XdsConfig<DetailsT> xdsConfig, TypeUrl type) {
        return new IncrementalDiscoveryService<>(type, responseObserver, getConfigBuilderForType(type), xdsConfig, getSubmanagerForType(type));
    }

}
