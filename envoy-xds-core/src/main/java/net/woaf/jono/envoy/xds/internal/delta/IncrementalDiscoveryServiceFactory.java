package net.woaf.jono.envoy.xds.internal.delta;

import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.internal.NoOpConfigBuilder;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.internal.DiscoveryService;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceFactory;
import net.woaf.jono.envoy.xds.TypeUrl;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class IncrementalDiscoveryServiceFactory<StateUpdT, DetailsT> implements DiscoveryServiceFactory<DeltaDiscoveryRequest, DeltaDiscoveryResponse, StateUpdT, DetailsT> {

    private final List<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders;

    public IncrementalDiscoveryServiceFactory(List<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders) {
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
    public DiscoveryService<DeltaDiscoveryRequest, StateUpdT> createDiscoveryService(StreamObserver<DeltaDiscoveryResponse> responseObserver, XdsConfig<DetailsT> xdsConfig, TypeUrl type) {
        return new IncrementalDiscoveryService<>(type, responseObserver, getConfigBuilderForType(type), xdsConfig, getSubmanagerForType(type));
    }

}
