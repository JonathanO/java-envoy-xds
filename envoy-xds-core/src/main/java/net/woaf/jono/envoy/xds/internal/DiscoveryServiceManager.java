package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.TypeUrl;
import org.jspecify.annotations.Nullable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.api.StateBacklog;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Co-ordinates ordering of messages between discovery services.
 * This class is not thread safe! External synchronization must be provided.
 *
 * @param <RequestT> The discovery Request type (DeltaDiscoveryRequest or DiscoveryRequest)
 */
public class DiscoveryServiceManager<RequestT extends Message, StateUpdT> {

    private static final Logger log = LoggerFactory.getLogger(DiscoveryServiceManager.class);

    private final List<DiscoveryService<RequestT, StateUpdT>> preOrder;
    private final List<DiscoveryService<RequestT, StateUpdT>> postOrder;

    private final Map<TypeUrl, DiscoveryService<RequestT, StateUpdT>> discoveryServices;

    /**
     * Iterator for obtaining the next discovery service we need to send messages from.
     */
    private @Nullable Iterator<DiscoveryService<RequestT, StateUpdT>> current = null;

    /**
     * The discovery service we're waiting on envoy to ack messages from to allow the current push to continue.
     * null if we are not waiting on anything.
     */
    private @Nullable DiscoveryService<RequestT, StateUpdT> waitingOn = null;

    /**
     * The set of discovery services, aside from waitingOn, that are awaiting ack messages from envoy.
     * While waitingOn tracks the current state of pushing updates, we also need to track which acks are outstanding
     * for messages sent in response to changes in Envoy subscriptions.
     */
    private final Set<DiscoveryService<RequestT, StateUpdT>> outstandingAcks = Sets.newIdentityHashSet();

    /**
     * Which order we're iterating through the discoveryServices in.
     * This is used to select which of preOrder or postOrder to use.
     */
    private DiscoveryService.SubState sendMode = DiscoveryService.SubState.COMPLETED;

    private final DiscoveryServiceManagerMetrics metrics;

    private final StateBacklog<StateUpdT> waitingStateBacklog;

    /**
     * The change set we're currently trying to send to envoy.
     * null if we're not currently waiting on acks from envoy for a change.
     */
    private @Nullable StateUpdT currentChange = null;

    private boolean closed = false;
    private boolean initialized = false;

    /**
     * Envoy rejected something we sent, so we've stopped sending changes on this stream. See freeze().
     */
    private boolean frozen = false;

    private @Nullable TypeUrl delayUpdatesUntilAckOf = null;

    /**
     * Types that refer to clusters. Envoy re-subscribes to everything at once when it reconnects, and may subscribe at
     * any point in a change, so their answers wait until the clusters and endpoints they may use have been sent and
     * ACKed. CDS and EDS answers never wait: Envoy holds its CDS ACK until new clusters have warmed, which takes the EDS
     * answer.
     */
    private static final Set<TypeUrl> USE_CLUSTERS = Set.of(TypeUrl.LDS, TypeUrl.RDS, TypeUrl.SRDS, TypeUrl.VHDS);

    /**
     * Longest we wait for an ACK of a type this client may never subscribe to: before removing stale resources after a
     * reconnect, and for delayUpdatesUntilAckOf. Checked when something happens.
     */
    public static final long WAIT_FOR_ACK_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final Deque<CommonDiscoveryRequest<RequestT>> heldRequests = new ArrayDeque<>();

    /**
     * Types this client gets from us, or null for all we serve. See XdsConfig.subscribesTo.
     */
    private @Nullable Set<TypeUrl> subscribesTo = null;

    private LongSupplier clock = System::nanoTime;

    private long startedNanos;

    /**
     * Stale removals waited too long for an ACK once, so they've waited too long from now on. See mayRemove().
     */
    private boolean gaveUpWaitingToRemove = false;

    /**
     * Checks that conditions hold, assuming init() was already called.
     */
    private void assertPostInitState() {
        Preconditions.checkState(!closed, "DiscoveryServiceManager is closed.");
        Preconditions.checkState(initialized, "DiscoveryServiceManager must be initialized before services can be initialized");
    }

    /**
     * See class description.
     * @param discoveryServices The discovery service implementations
     * @param preOrder Specifies the order in which discovery services should be processed for "add" order
     * @param postOrder Specifies the order in which discovery services should be processed for "remove" order
     * @param waitingStateBacklog holds the state updates waiting to be pushed: each one is put in, and taken out when its push can begin
     */
    public DiscoveryServiceManager(Map<TypeUrl, DiscoveryService<RequestT, StateUpdT>> discoveryServices, List<TypeUrl> preOrder, List<TypeUrl> postOrder, StateBacklog<StateUpdT> waitingStateBacklog,
                                   DiscoveryServiceManagerMetrics metrics) {
        this.discoveryServices = discoveryServices;
        this.waitingStateBacklog = waitingStateBacklog;
        this.preOrder = DiscoveryServiceOrderer.sort(preOrder, discoveryServices);
        this.postOrder = DiscoveryServiceOrderer.sort(postOrder, discoveryServices);
        this.metrics = metrics;
    }

    /**
     * Populate the initial state of the manager.
     * @param initialStateChange state that all changes provided to pushUpdates apply sequentially from
     * @param delayUpdatesUntilAckOf see XdsConfig, or null
     * @param subscribesTo see XdsConfig, or null for every type we serve
     */
    public void init(StateUpdT initialStateChange, @Nullable TypeUrl delayUpdatesUntilAckOf, @Nullable Set<TypeUrl> subscribesTo) {
        this.delayUpdatesUntilAckOf = delayUpdatesUntilAckOf;
        this.subscribesTo = subscribesTo;
        if (delayUpdatesUntilAckOf != null && subscribesTo != null && !subscribesTo.contains(delayUpdatesUntilAckOf)) {
            log.warn("delayUpdatesUntilAckOf {}, but this client doesn't subscribe to it, so not delaying updates", delayUpdatesUntilAckOf);
            this.delayUpdatesUntilAckOf = null;
        }
        this.startedNanos = clock.getAsLong();
        discoveryServices.forEach((t, s) -> {
            s.init(initialStateChange);
            s.setDeferReconnectRemovals(() -> !mayRemove(t));
        });
        initialized = true;
    }

    @VisibleForTesting
    public void setClock(LongSupplier clock) {
        this.clock = clock;
        this.startedNanos = clock.getAsLong();
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        metrics.close();
    }

    /**
     * Process a message from Envoy.
     * This is likely an ack for a message we've sent, or a change to the subscription set.
     * @param value This type is a generic wrapper around the two types of DiscoveryRequest message.
     */
    public void processUpdate(CommonDiscoveryRequest<RequestT> value) {
        assertPostInitState();

        DiscoveryService<RequestT, StateUpdT> discoveryService = serviceFor(value);
        if (mustWait(value, discoveryService)) {
            if (value.errorDetail() != null) {
                freeze(); // at once, though what it asks for waits its turn
            }
            heldRequests.add(value);
            return;
        }
        handle(value, discoveryService);
        while (!heldRequests.isEmpty() && !clustersPending()) {
            CommonDiscoveryRequest<RequestT> held = heldRequests.poll();
            handle(held, serviceFor(held));
        }
    }

    private DiscoveryService<RequestT, StateUpdT> serviceFor(CommonDiscoveryRequest<RequestT> value) {
        // The stream observer drops unknown types, and the factory builds a service for every type.
        TypeUrl typeUrl = Preconditions.checkNotNull(TypeUrl.of(value.typeUrl()), "Unknown type URL %s", value.typeUrl());
        return Preconditions.checkNotNull(discoveryServices.get(typeUrl), "No discovery service for %s", typeUrl);
    }

    /**
     * Whether to answer a request later, in add order: see USE_CLUSTERS. It never overtakes a held request of its type,
     * as each follows on from the last. Otherwise an ACK its service is waiting for never waits.
     */
    private boolean mustWait(CommonDiscoveryRequest<RequestT> value, DiscoveryService<RequestT, StateUpdT> discoveryService) {
        if (heldRequests.stream().anyMatch(held -> held.typeUrl().equals(value.typeUrl()))) {
            return true;
        }
        return USE_CLUSTERS.contains(discoveryService.getTypeUrl()) && !discoveryService.awaitingAck() && clustersPending();
    }

    private void handle(CommonDiscoveryRequest<RequestT> value, DiscoveryService<RequestT, StateUpdT> discoveryService) {
        discoveryService.processUpdate(value);
        if (value.errorDetail() != null) {
            freeze();
        }
        updateOutstandingAckState(discoveryService.getTypeUrl(), discoveryService);

        if (!frozen) {
            if (currentChange != null) {
                // Continue attempting to push the current change as this might have been an ack the current push was waiting for.
                continuePush();
            } else {
                pushAllowedWorkIfAny();
            }
        }
    }

    /**
     * Whether clusters or endpoints are still to be sent or ACKed: a change's, or an answer's. The endpoints' add round
     * comes after the clusters', so while either is to come, the endpoints' is.
     */
    private boolean clustersPending() {
        DiscoveryService<RequestT, StateUpdT> endpoints = discoveryServices.get(TypeUrl.EDS);
        return Stream.of(TypeUrl.CDS, TypeUrl.EDS).map(discoveryServices::get).anyMatch(s -> s != null && s.awaitingAck())
            || (endpoints != null && endpoints.addsToCome());
    }

    /**
     * Whether stale resources of this type, found when Envoy reconnected, may be removed now: every type before it in
     * remove order that we serve and this client gets has been answered and ACKed, or we've waited long enough.
     */
    private boolean mayRemove(TypeUrl typeUrl) {
        if (gaveUpWaitingToRemove) {
            return true;
        }
        for (DiscoveryService<RequestT, StateUpdT> earlier : postOrder) {
            if (earlier.getTypeUrl() == typeUrl) {
                break;
            }
            boolean used = earlier.isServed() && (subscribesTo == null || subscribesTo.contains(earlier.getTypeUrl()));
            if (used && (!earlier.hasAnswered() || earlier.awaitingAck())) {
                if (!waitedTooLong()) {
                    return false;
                }
                gaveUpWaitingToRemove = true;
                log.warn("No answer to or ACK of {} after {}s, so removing stale {} anyway: does this client subscribe to it? See"
                    + " subscribesTo in its XdsConfig", earlier.getTypeUrl(), TimeUnit.NANOSECONDS.toSeconds(WAIT_FOR_ACK_TIMEOUT_NANOS), typeUrl);
                break;
            }
        }
        return true;
    }

    private void updateOutstandingAckState(TypeUrl typeUrl, DiscoveryService<RequestT, StateUpdT> discoveryService) {
        if (discoveryService.awaitingAck()) {
            if (outstandingAcks.add(discoveryService)) {
                metrics.onAwaitingAck();
            }
        } else {
            if (outstandingAcks.remove(discoveryService)) {
                metrics.onMessageAcked();
                if (typeUrl.equals(delayUpdatesUntilAckOf)) {
                    delayUpdatesUntilAckOf = null;
                }
            }
        }
    }

    /**
     * Stop sending changes on this stream, for good. Envoy keeps serving its last good config until it reconnects.
     *
     * <p>A rejected change won't fix itself: resending it, or any later change that includes it, gets rejected again.
     * Envoy may also have applied part of it, so we no longer know what it has, and diffing from what we think it has
     * would make things worse. Dropping the stream instead would have Envoy reconnect straight away, be sent the same
     * config, and loop. Subscription requests are still answered, from our own state: the rejected change's add round,
     * as its removals were never sent and Envoy may still use what they'd remove. For SotW that includes what Envoy
     * rejected, as each answer is the whole set: resending it changes nothing Envoy has, where rolling it back would
     * undo any part of it Envoy took.
     */
    private void freeze() {
        if (frozen) {
            return;
        }
        frozen = true;
        if (currentChange != null) {
            currentChange = null;
            waitingOn = null;
            current = null;
            sendMode = DiscoveryService.SubState.COMPLETED;
            discoveryServices.values().forEach(DiscoveryService::abandonChange);
            metrics.onPushComplete();
        }
        while (waitingStateBacklog.take() != null) {
            // Drop it. Nothing more will be sent.
        }
    }

    private boolean isPushAllowed() {
        if (delayUpdatesUntilAckOf != null && waitedTooLong()) {
            log.warn("No ACK of {} after {}s, so sending updates anyway: does this client subscribe to it? See delayUpdatesUntilAckOf and"
                + " subscribesTo in its XdsConfig", delayUpdatesUntilAckOf, TimeUnit.NANOSECONDS.toSeconds(WAIT_FOR_ACK_TIMEOUT_NANOS));
            delayUpdatesUntilAckOf = null;
        }
        return outstandingAcks.isEmpty() && delayUpdatesUntilAckOf == null;
    }

    private boolean waitedTooLong() {
        return clock.getAsLong() - startedNanos >= WAIT_FOR_ACK_TIMEOUT_NANOS;
    }

    private void pushAllowedWorkIfAny() {
        // Nor while requests are held: a type that hasn't heard its first one would be skipped, though Envoy has some
        // of it. The last held request to be answered starts what waits.
        if (!outstandingAcks.isEmpty() || !heldRequests.isEmpty()) {
            return;
        }
        pushDeferredReconnectRemovalsIfAny(); // the ACK one waits for holds the change too: see isPushAllowed()
        if (isPushAllowed()) {
            pushWaitingChangeIfAny();
        }
    }

    private void beginPush(StateUpdT change) {
        metrics.onPushBegin();

        sendMode = DiscoveryService.SubState.PRE;
        current = preOrder.iterator();
        discoveryServices.forEach((t, s) -> s.onNetworkUpdate(change));
        continuePush(); // sends from the first service, as waitingOn is null
    }

    private void enqueueChange(StateUpdT diff) {
        waitingStateBacklog.put(diff);
    }

    /**
     * Receive a ServiceChanges that may need to be communicated to envoy.
     * If pushing of a previous ServiceChanges is still in progress, or any unacked messages are outstanding, communicating this change will be deferred until those in-progress changes are finished and all messages have been acked.
     * Waiting changes are kept in the StateBacklog, which may merge them (MergingStateBacklog) or not (QueueingStateBacklog).
     *
     * @param diff the change to be applied.
     */
    public void pushUpdates(StateUpdT diff) {
        assertPostInitState();

        if (frozen) {
            return;
        }
        enqueueChange(diff);
        if (currentChange == null) {
            // Nothing currently being applied: stale removals go first, then this change.
            pushAllowedWorkIfAny();
        }
    }

    private @Nullable DiscoveryService<RequestT, StateUpdT> nextOrNull() {
        if (current != null && current.hasNext()) {
            return current.next();
        }
        return null;
    }

    /**
     * Complete the current push, can only be called if there is a current change in progress.
     */
    private void finishPush() {
        sendMode = DiscoveryService.SubState.COMPLETED;
        // Record time since we were first told about this change.
        metrics.onPushComplete();

        // All done!
        currentChange = null;

        pushAllowedWorkIfAny();
    }

    /**
     * Start pushing any waiting changes to envoy.
     * Can only be called if there is no current change in progress.
     */
    private void pushWaitingChangeIfAny() {
        currentChange = waitingStateBacklog.take();
        if (currentChange != null) {
            beginPush(currentChange);
        }
    }

    private void pushDeferredReconnectRemovalsIfAny() {
        for (DiscoveryService<RequestT, StateUpdT> discoveryService: postOrder) {
            if (discoveryService.hasDeferredReconnectRemovals()) {
                if (!mayRemove(discoveryService.getTypeUrl())) {
                    return; // and not a later type's: removals go in remove order
                }
                discoveryService.sendDeferredReconnectRemovals();
                if (discoveryService.awaitingAck()) {
                    outstandingAcks.add(discoveryService); // was empty: see pushAllowedWorkIfAny()
                    metrics.onAwaitingAck();
                    return;
                }
            }
        }
    }

    /**
     * Attempt to continue the current push, if possible.
     * This can only be called if there is a change in progress.
     */
    private void continuePush() {
        if (waitingOn != null && waitingOn.awaitingAck()) {
            return;
        }
        DiscoveryService<RequestT, StateUpdT> nextService = nextOrNull();
        if (nextService == null) {
            if (sendMode.equals(DiscoveryService.SubState.PRE)) {
                sendMode = DiscoveryService.SubState.POST;
                current = postOrder.iterator();
                nextService = nextOrNull();
            }
        }
        waitingOn = nextService;

        if (nextService == null) {
            finishPush();
        } else {
            switch (sendMode) {
                case PRE -> nextService.sendNetworkUpdatePre();
                case POST -> nextService.sendNetworkUpdatePost();
                case COMPLETED -> { }
                default -> throw new IllegalStateException("Unhandled case should not happen.");
            }
            // Continue immediately, in case we are not waiting for acks.
            continuePush();
        }
    }


}
