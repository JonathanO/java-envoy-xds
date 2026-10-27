package net.woaf.jono.envoy.xds.internal.delta;

import org.jspecify.annotations.Nullable;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.internal.AbstractDiscoveryService;
import net.woaf.jono.envoy.xds.internal.NodeConfig;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import io.grpc.stub.StreamObserver;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IncrementalDiscoveryService<E extends Message, StateUpdT, DetailsT> extends AbstractDiscoveryService<DeltaDiscoveryRequest, E, StateUpdT, DetailsT> {

    private static final Logger log = LoggerFactory.getLogger(IncrementalDiscoveryService.class);

    private final StreamObserver<DeltaDiscoveryResponse> responseObserver;

    private @Nullable String lastNonce = null;

    private Long version = 0L;

    private final SubManager subManager;

    private final HashSet<String> deferredReconnectRemovals = new HashSet<>();

    private BooleanSupplier deferReconnectRemovals = () -> false;

    /** What we answered subscriptions with while the current change waits its turn, so the change needn't send it again. */
    private final Map<String, E> answeredDuringChange = new HashMap<>();

    IncrementalDiscoveryService(TypeUrl myTypeUrl, StreamObserver<DeltaDiscoveryResponse> responseObserver, IncrementalConfigBuilder<E, StateUpdT, DetailsT> configBuilder, NodeConfig<DetailsT> nodeConfig,
                                SubManager subManager) {
        super(myTypeUrl, configBuilder, nodeConfig);
        this.responseObserver = responseObserver;
        this.subManager = subManager;
    }

    @Override
    protected void processRequest(DeltaDiscoveryRequest value) {
        boolean isAck = !Strings.isNullOrEmpty(value.getResponseNonce());
        if (isAck) {
            // Envoy is answering something: an ACK, or a NACK if it has error detail.
            if (value.getResponseNonce().equals(lastNonce)) {
                log.debug("{} {} {}", getTypeUrl().name(), value.hasErrorDetail() ? "NACKed" : "ACKed", lastNonce);
                lastNonce = null; // answered. The DiscoveryServiceManager stops sending changes after a NACK.
            }
        }
        ImmutableSet<String> subscribeSet = ImmutableSet.copyOf(value.getResourceNamesSubscribeList());
        ImmutableSet<String> unsubscribeSet = ImmutableSet.copyOf(value.getResourceNamesUnsubscribeList());
        // Envoy puts any subscription changes it has queued into its ACKs (SubscriptionState::getNextRequestWithAck),
        // so a request can both answer a response and change subscriptions.
        subManager.processResourceListChange(subscribeSet, unsubscribeSet).ifPresent(newSubFilter ->
            processSubUpdate(subscribeSet, newSubFilter, ImmutableSet.copyOf(value.getInitialResourceVersionsMap().keySet())));
    }

    @Override
    public Predicate<String> subFilter() {
        return subManager::isSubscribedTo;
    }

    private void processSubUpdate(ImmutableSet<String> newSubs, Predicate<String> filter, ImmutableSet<String> initialState) {
        log.debug("{} subscription change - added: {}", getTypeUrl().name(), newSubs);

        // Envoy will want us to tell it about everything it's just asked for, assuming it successfully subscribed to it.
        // If the resource doesn't exist, however, we will need to tell it to delete it.
        // We check against the filter because some values in resource name lists have special meaning to sub management (e.g. wildcards),
        // they're not actually resource names.
        HashSet<String> removed = newSubs.stream()
            .filter(filter)
            .filter(resourceName -> !initialState.contains(resourceName))
            .collect(Collectors.toCollection(HashSet::new));
        // If envoy sent us an initial resource versions map it might also contain things that don't exist anymore.
        // Again, we will need to tell envoy to delete it, but only if envoy has subscribed for updates about it. We defer
        // these removals so DiscoveryServiceManager can send them in ADS remove order after dependent types have ACKed.
        // This is necessary because envoy doesn't explicitly subscribe to named resources (e.g. when it uses wildcards), so it can be
        // subscribed to things it has never explicitly asked us for.
        HashSet<String> reconnectRemovals = initialState.stream().filter(filter).collect(Collectors.toCollection(HashSet::new));

        // Now actually generate the resources for this sub update.
        IncrementalConfigBuilder.Resources<E> resources = getResources(filter);

        for (IncrementalConfigBuilder.NamedMessage<E> msg: resources.resources()) {
            // If we generated a resource we don't want to tell envoy to remove it.
            removed.remove(msg.name());
            reconnectRemovals.remove(msg.name());
            deferredReconnectRemovals.remove(msg.name());
        }

        if (getCurrentSubState() == SubState.PRE) {
            // Answered from the change we haven't pushed yet.
            resources.resources().forEach(msg -> answeredDuringChange.put(msg.name(), msg.message()));
        }
        if (deferReconnectRemovals.getAsBoolean()) {
            deferredReconnectRemovals.addAll(reconnectRemovals);
        } else {
            removed.addAll(reconnectRemovals);
        }
        pushResources(resources.resources(), removed);
    }

    @Override
    public boolean awaitingAck() {
        return lastNonce != null;
    }

    @Override
    public void setDeferReconnectRemovals(BooleanSupplier deferReconnectRemovals) {
        this.deferReconnectRemovals = deferReconnectRemovals;
    }

    @Override
    public void pushNewState(IncrementalConfigBuilder.Response<E> response) {
        List<IncrementalConfigBuilder.NamedMessage<E>> addAndUpdates = response.addAndUpdates().stream()
            .filter(msg -> !msg.message().equals(answeredDuringChange.get(msg.name())))
            .toList();
        answeredDuringChange.clear();
        if (addAndUpdates.isEmpty() && response.removes().isEmpty()) {
            return;
        }
        pushResources(addAndUpdates, response.removes());
    }

    @Override
    public void onNetworkUpdate(StateUpdT changes) {
        super.onNetworkUpdate(changes);
        answeredDuringChange.clear();
    }

    @Override
    public boolean hasDeferredReconnectRemovals() {
        return !deferredReconnectRemovals.isEmpty();
    }

    @Override
    public void sendDeferredReconnectRemovals() {
        if (deferredReconnectRemovals.isEmpty()) {
            return;
        }

        Predicate<String> currentlySubscribed = subFilter();
        HashSet<String> removals = deferredReconnectRemovals.stream()
            .filter(currentlySubscribed)
            .collect(Collectors.toCollection(HashSet::new));

        IncrementalConfigBuilder.Resources<E> currentResources = getResources(currentlySubscribed);
        for (IncrementalConfigBuilder.NamedMessage<E> msg: currentResources.resources()) {
            removals.remove(msg.name());
        }

        deferredReconnectRemovals.clear();
        if (!removals.isEmpty()) {
            pushResources(List.of(), removals);
        }
    }

    private void pushResources(Collection<IncrementalConfigBuilder.NamedMessage<E>> resources, Collection<String> removals) {
        Long myVersion = ++version;
        DeltaDiscoveryResponse.Builder responseBuilder = DeltaDiscoveryResponse.newBuilder();
        for (IncrementalConfigBuilder.NamedMessage<? extends Message> namedMessage : resources) {
            responseBuilder.addResources(Resource.newBuilder()
                    .setName(namedMessage.name())
                    .setVersion(myVersion.toString())
                    .setResource(pack(namedMessage.message()))
                    .build());
        }
        responseBuilder.addAllRemovedResources(removals);
        responseBuilder.setTypeUrl(getTypeUrl().getTypeUrl());
        lastNonce = UUID.randomUUID().toString();
        responseBuilder.setNonce(lastNonce);
        responseBuilder.setSystemVersionInfo(myVersion.toString());

        DeltaDiscoveryResponse discoveryResponse = responseBuilder.build();

        if (log.isDebugEnabled()) {
            log.debug("{} Pushing update {} change - added: {} removed: {}", getTypeUrl().name(), lastNonce, resources.stream().map(IncrementalConfigBuilder.NamedMessage::name).collect(Collectors.toList()), removals);
        }

        markAnswered();
        responseObserver.onNext(discoveryResponse);
    }

}
