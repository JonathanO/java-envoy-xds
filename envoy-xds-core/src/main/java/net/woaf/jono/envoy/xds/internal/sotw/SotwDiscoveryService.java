package net.woaf.jono.envoy.xds.internal.sotw;

import org.jspecify.annotations.Nullable;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.internal.AbstractDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.NamedMessage;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;

class SotwDiscoveryService<E extends Message, StateUpdT, DetailsT> extends AbstractDiscoveryService<DiscoveryRequest, E, StateUpdT, DetailsT> {

    private static final Logger log = LoggerFactory.getLogger(SotwDiscoveryService.class);

    private final StreamObserver<DiscoveryResponse> responseObserver;

    private final SubManager subManager;
    private final VersionManager versionManager;

    private @Nullable String awaitingVersion = null;

    private final Map<String, NamedMessage<E>> resourceState = new LinkedHashMap<>();

    SotwDiscoveryService(TypeUrl myTypeUrl, StreamObserver<DiscoveryResponse> responseObserver, SubManager subManager, VersionManager versionManager, IncrementalConfigBuilder<E, StateUpdT, DetailsT> configBuilder, XdsConfig<DetailsT> xdsConfig) {
        super(myTypeUrl, configBuilder, xdsConfig);
        this.responseObserver = responseObserver;
        this.subManager = subManager;
        this.versionManager = versionManager;
    }

    private void applyCurrentState() {
        switch (getCurrentSubState()) {
            case PRE -> applyPre();
            case POST -> applyPost();
            default -> { } // COMPLETED: nothing left to apply
        }
    }

    private void processSubUpdate(Predicate<String> subChanged) {
        // Remove state for anything we're no longer subscribed to.
        resourceState.keySet().removeIf(n -> !subManager.isSubscribedTo(n));
        if (subChanged.equals(SubManager.NOTHING_ADDED) && hasAnswered()) {
            // Envoy drops what it unsubscribed from itself and doesn't wait for an answer, so don't make it ACK the rest
            // again, e.g. on every EDS unsubscribe after a cluster removal. Any change in progress is still sent in turn.
            return;
        }

        getResources(subChanged).resources().forEach(msg -> resourceState.put(msg.name(), msg));
        applyCurrentState();
        // Answer even if nothing changed: Envoy waits for a response to its first request of each type, e.g. CDS when
        // there are no clusters yet, and to tell it that names it asked for don't exist.
        pushResources(resourceState.values());
    }

    @Override
    protected void processRequest(DiscoveryRequest value) {
        log.debug("DiscoveryRequest: V={},R={},N={},T={}", value.getVersionInfo(), value.getResourceNamesList(), value.getResponseNonce(), value.getTypeUrl());
        if (!versionManager.processUpdate(value.getResponseNonce(), value.getVersionInfo())) {
            // Stale or otherwise invalid
            return;
        }
        if (value.hasErrorDetail()) {
            // Envoy rejected awaitingVersion, so it's answered. The DiscoveryServiceManager stops sending changes after a NACK.
            awaitingVersion = null;
        }
        subManager.processResourceListChange(value.getResourceNamesList()).ifPresent(this::processSubUpdate);
    }

    @Override
    public boolean awaitingAck() {
        if (awaitingVersion == null) {
            return false;
        }

        return !versionManager.hasAcceptedVersion(awaitingVersion);
    }

    @Override
    protected Predicate<String> subFilter() {
        return subManager::isSubscribedTo;
    }

    @Override
    protected boolean updateState(IncrementalConfigBuilder.Response<E> response) {
        if (response.isNoop()) {
            // Nothing to do :)
            return false;
        }
        response.addAndUpdates().forEach(msg ->
                resourceState.put(msg.name(), msg)
        );
        response.removes().forEach(name ->
                resourceState.remove(name)
        );
        return true;
    }

    @Override
    protected void pushNewState(IncrementalConfigBuilder.Response<E> response) {
        pushResources(resourceState.values());
    }

    private void pushResources(Collection<IncrementalConfigBuilder.NamedMessage<E>> resources) {
        String version = versionManager.getNext();
        DiscoveryResponse.Builder responseBuilder = DiscoveryResponse.newBuilder();
        log.debug("Sending {} of {}", version, getTypeUrl());
        for (IncrementalConfigBuilder.NamedMessage<? extends Message> namedMessage : resources) {
            responseBuilder.addResources(pack(namedMessage.message()));
        }
        responseBuilder.setTypeUrl(getTypeUrl().getTypeUrl());
        responseBuilder.setVersionInfo(version);
        responseBuilder.setNonce(versionManager.pushedVersion(version));

        DiscoveryResponse discoveryResponse = responseBuilder.build();
        if (log.isDebugEnabled()) {
            log.debug("DiscoveryResponse: V={},R={},N={},T={}", discoveryResponse.getVersionInfo(),
                resources.stream().map(IncrementalConfigBuilder.NamedMessage::name).toList(), discoveryResponse.getNonce(), discoveryResponse.getTypeUrl());
        }

        awaitingVersion = version;
        markAnswered();
        responseObserver.onNext(discoveryResponse);
    }

}
