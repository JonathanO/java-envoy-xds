package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.TypeUrl;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class DiscoveryServiceFactory<RequestT extends Message, ResponseT extends Message, StateUpdT, DetailsT> {

    private final ImmutableList<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders;

    protected DiscoveryServiceFactory(ImmutableList<IncrementalConfigBuilder<?, StateUpdT, DetailsT>> configBuilders) {
        this.configBuilders = configBuilders;
        for (IncrementalConfigBuilder<?, StateUpdT, DetailsT> icb : configBuilders) {
            if (!TypeUrl.getMessageClazzes().contains(icb.handlesType())) {
                throw new IllegalArgumentException(icb.handlesType() + " is not a known ADS message type. Valid types are: " + TypeUrl.getMessageClazzes());
            }
        }
    }

    public abstract DiscoveryService<RequestT, StateUpdT> createDiscoveryService(StreamObserver<ResponseT> responseObserver, NodeConfig<DetailsT> nodeConfig, TypeUrl type);

    @SuppressWarnings("unchecked")
    protected <E extends Message> IncrementalConfigBuilder<E, StateUpdT, DetailsT> getConfigBuilderForType(TypeUrl type) {
        return (IncrementalConfigBuilder<E, StateUpdT, DetailsT>) configBuilders.stream().filter(b -> type.getMessageClazz().equals(b.handlesType())).findAny().orElseGet(() -> new NoOpConfigBuilder<>(type.getMessageClazz()));
    }

    public Map<TypeUrl, DiscoveryService<RequestT, StateUpdT>> createAll(StreamObserver<ResponseT> responseObserver, NodeConfig<DetailsT> nodeConfig) {
        return Arrays.stream(TypeUrl.values()).collect(Collectors.toMap(
                Function.identity(),
                t -> createDiscoveryService(responseObserver, nodeConfig, t)
            ));
    }

}
