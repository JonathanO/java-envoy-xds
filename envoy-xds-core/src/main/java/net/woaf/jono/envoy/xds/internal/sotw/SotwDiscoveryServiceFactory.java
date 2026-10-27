package net.woaf.jono.envoy.xds.internal.sotw;

import com.google.common.collect.ImmutableList;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.internal.DiscoveryService;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceFactory;
import net.woaf.jono.envoy.xds.TypeUrl;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;


public class SotwDiscoveryServiceFactory<StateUpdT, DetailsT> extends DiscoveryServiceFactory<DiscoveryRequest, DiscoveryResponse, StateUpdT, DetailsT> {

    public SotwDiscoveryServiceFactory(ImmutableList<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders) {
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
    public DiscoveryService<DiscoveryRequest, StateUpdT> createDiscoveryService(StreamObserver<DiscoveryResponse> responseObserver, XdsConfig<DetailsT> xdsConfig, TypeUrl type) {
        return new SotwDiscoveryService<>(type, responseObserver, getSubmanagerForType(type), new VersionManager(), getConfigBuilderForType(type), xdsConfig);
    }

}
