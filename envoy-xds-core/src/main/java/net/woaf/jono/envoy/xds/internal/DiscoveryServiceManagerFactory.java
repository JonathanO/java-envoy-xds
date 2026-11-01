package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.TypeUrl;
import com.google.common.base.Ticker;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.api.StateBacklog;
import net.woaf.jono.envoy.xds.api.StateBacklogFactory;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public class DiscoveryServiceManagerFactory<RequestT extends Message, ResponseT extends Message, StateUpdT, DetailsT> {

    private final DiscoveryServiceFactory<RequestT, ResponseT, StateUpdT, DetailsT> discoveryServiceFactory;
    private final StateBacklogFactory<StateUpdT> waitingStateBacklogFactory;
    private final Ticker ticker;

    public DiscoveryServiceManagerFactory(DiscoveryServiceFactory<RequestT, ResponseT, StateUpdT, DetailsT> discoveryServiceFactory, StateBacklogFactory<StateUpdT> waitingStateBacklogFactory) {
        this(discoveryServiceFactory, waitingStateBacklogFactory, Ticker.systemTicker());
    }

    public DiscoveryServiceManagerFactory(DiscoveryServiceFactory<RequestT, ResponseT, StateUpdT, DetailsT> discoveryServiceFactory, StateBacklogFactory<StateUpdT> waitingStateBacklogFactory,
                                          Ticker ticker) {
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.waitingStateBacklogFactory = waitingStateBacklogFactory;
        this.ticker = ticker;
    }

    public DiscoveryServiceManager<RequestT, StateUpdT> build(StreamObserver<ResponseT> responseObserver, XdsConfig<DetailsT> xdsConfig, DiscoveryServiceManagerMetrics metrics) {
        return build(discoveryServiceFactory.createAll(responseObserver, xdsConfig), waitingStateBacklogFactory.build(), metrics);
    }

    public DiscoveryServiceManager<RequestT, StateUpdT> build(Map<TypeUrl, DiscoveryService<RequestT, StateUpdT>> discoveryServices, StateBacklog<StateUpdT> waitingStateBacklog, DiscoveryServiceManagerMetrics metrics) {
        DiscoveryServiceManager<RequestT, StateUpdT> manager = new DiscoveryServiceManager<>(discoveryServices, TypeUrl.ADD_ORDER, TypeUrl.REMOVE_ORDER, waitingStateBacklog, metrics);
        manager.setClock(ticker::read);
        return manager;
    }

}
