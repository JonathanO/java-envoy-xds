package net.woaf.jono.envoy.xds.internal.sotw;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.internal.NoOpConfigBuilder;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.internal.DiscoveryService;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceFactory;
import net.woaf.jono.envoy.xds.TypeUrl;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;


public class SotwDiscoveryServiceFactory<StateUpdT, DetailsT> implements DiscoveryServiceFactory<DiscoveryRequest, DiscoveryResponse, StateUpdT, DetailsT> {

    private final ImmutableList<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders;

    public SotwDiscoveryServiceFactory(ImmutableList<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders) {
        this.configBuilders = configBuilders;
        for (IncrementalConfigBuilder<?, StateUpdT, DetailsT> icb : configBuilders) {
            if (!TypeUrl.getMessageClazzes().contains(icb.handlesType())) {
                throw new IllegalArgumentException(icb.handlesType() + " is not a known ADS message type. Valid types are: " + TypeUrl.getMessageClazzes());
            }
        }
    }

    private SubManager getSubmanagerForType(TypeUrl type) {
        if (type.isWildcard()) {
            return new WildcardSubManager();
        } else {
            return new SubListSubManager();
        }
    }

    @SuppressWarnings("unchecked")
    public <E extends Message> IncrementalConfigBuilder<E, StateUpdT, DetailsT> getConfigBuilderForType(TypeUrl type) {
        return (IncrementalConfigBuilder<E, StateUpdT, DetailsT>) configBuilders.stream().filter(b -> type.getMessageClazz().equals(b.handlesType())).findAny().orElseGet(() -> new NoOpConfigBuilder<>(type.getMessageClazz()));
    }

    @Override
    public DiscoveryService<DiscoveryRequest, StateUpdT> createDiscoveryService(StreamObserver<DiscoveryResponse> responseObserver, XdsConfig<DetailsT> xdsConfig, TypeUrl type) {
        return new SotwDiscoveryService<>(type, responseObserver, getSubmanagerForType(type), new VersionManager(), getConfigBuilderForType(type), xdsConfig);
    }

}
