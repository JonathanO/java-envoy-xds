package net.woaf.jono.envoy.xds;

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

/**
 * Co-ordinates ordering of messages between discovery services.
 * This class is not thread safe! External synchronization must be provided.
 *
 * @param <RequestT> The discovery Request type (DeltaDiscoveryRequest or DiscoveryRequest)
 */
public class DiscoveryServiceManager<RequestT extends Message, StateUpdT> {

    private final List<DiscoveryService<RequestT, StateUpdT>> preOrder;
    private final List<DiscoveryService<RequestT, StateUpdT>> postOrder;

    private final Map<TypeUrl, DiscoveryService<RequestT, StateUpdT>> discoveryServices;

    /**
     * Iterator for obtaining the next discovery service we need to send messages from.
     */
    private Iterator<DiscoveryService<RequestT, StateUpdT>> current = null;

    /**
     * The discovery service we're waiting on envoy to ack messages from to allow the current push to continue.
     * null if we are not waiting on anything.
     */
    private DiscoveryService<RequestT, StateUpdT> waitingOn = null;

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
    private StateUpdT currentChange = null;

    private boolean closed = false;
    private boolean initialized = false;

    /**
     * Envoy rejected something we sent, so we've stopped sending changes on this stream. See freeze().
     */
    private boolean frozen = false;

    private TypeUrl delayUpdatesUntilAckOf = null;

    /**
     * Types that refer to clusters. Envoy re-subscribes to everything at once when it reconnects, so their answers wait
     * until the clusters and endpoints they may use are ACKed. CDS and EDS answers never wait: Envoy holds its CDS ACK
     * until new clusters have warmed, which takes the EDS answer.
     */
    private static final Set<TypeUrl> USE_CLUSTERS = Set.of(TypeUrl.LDS, TypeUrl.RDS, TypeUrl.SRDS, TypeUrl.VHDS);

    /**
     * Longest we hold stale removals after a reconnect, waiting for types this client may never subscribe to. Holding
     * a removal only leaves an unused resource in Envoy, so this can be generous. Checked when something happens.
     */
    public static final long RECONNECT_REMOVAL_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final Deque<CommonDiscoveryRequest<RequestT>> heldRequests = new ArrayDeque<>();

    /**
     * Types this client gets from us, or null for all we serve. See XdsConfig.subscribesTo.
     */
    private Set<TypeUrl> subscribesTo = null;

    private LongSupplier clock = System::nanoTime;

    private long startedNanos;

    /**
     * Checks that conditions hold, assuming init() was already called.
     */
    private void assertPostInitState() {
        Preconditions.checkState(!closed, "DiscoveryServiceManager is closed.");
        Preconditions.checkState(initialized, "DiscoveryServiceManager must be initialized before services can be initialized");
        Preconditions.checkState((waitingOn == null) == (currentChange == null), "Waiting on an outstanding change, and on a service to ack, can't happen without each other.");
    }

    /**
     * See class description.
     * @param discoveryServices The discovery service implementations
     * @param preOrder Specifies the order in which discovery services should be processed for "add" order
     * @param postOrder Specifies the order in which discovery services should be processed for "remove" order
     * @param waitingStateBacklog Manager for the backlog of state updates, updates will be passed to the backlog if we are not able to immediately start sending them to this client because we are already part way through a previous update.
     */
    public DiscoveryServiceManager(Map<TypeUrl, DiscoveryService<RequestT, StateUpdT>> discoveryServices, List<TypeUrl> preOrder, List<TypeUrl> postOrder, StateBacklog<StateUpdT> waitingStateBacklog,
                                   DiscoveryServiceManagerMetrics metrics) {
        this.discoveryServices = discoveryServices;
        this.waitingStateBacklog = waitingStateBacklog;
        this.preOrder = DiscoveryServiceOrderer.sort(preOrder, discoveryServices);
        this.postOrder = DiscoveryServiceOrderer.sort(postOrder, discoveryServices);
        this.metrics = metrics;
    }


    public void init(StateUpdT initialStateChange, TypeUrl delayUpdatesUntilAckOf) {
        init(initialStateChange, delayUpdatesUntilAckOf, null);
    }

    /**
     * Populate the initial state of the manager.
     * All changes provided to pushUpdates are expected to apply sequentially from this initial state.
     */
    public void init(StateUpdT initialStateChange) {
        init(initialStateChange, null, null);
    }

    /**
     * Populate the initial state of the manager.
     * @param initialStateChange state that all changes provided to pushUpdates apply sequentially from
     * @param delayUpdatesUntilAckOf see XdsConfig, or null
     * @param subscribesTo see XdsConfig, or null for every type we serve
     */
    public void init(StateUpdT initialStateChange, TypeUrl delayUpdatesUntilAckOf, Set<TypeUrl> subscribesTo) {
        this.delayUpdatesUntilAckOf = delayUpdatesUntilAckOf;
        this.subscribesTo = subscribesTo;
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

        TypeUrl typeUrl = TypeUrl.of(value.getTypeUrl());
        DiscoveryService<RequestT, StateUpdT> discoveryService = discoveryServices.get(typeUrl);
        if (USE_CLUSTERS.contains(typeUrl) && value.getErrorDetail() == null && !discoveryService.awaitingAck() && clustersAwaitingAck()) {
            // Answer later, in add order: see USE_CLUSTERS. Never hold an ACK this service is waiting for.
            heldRequests.add(value);
            return;
        }
        discoveryService.processUpdate(value);
        if (value.getErrorDetail() != null) {
            freeze();
        }
        updateOutstandingAckState(typeUrl, discoveryService);

        if (!frozen) {
            if (currentChange != null) {
                // Continue attempting to push the current change as this might have been an ack the current push was waiting for.
                continuePush();
            } else {
                pushAllowedWorkIfAny();
            }
        }
        while (!heldRequests.isEmpty() && !clustersAwaitingAck()) {
            processUpdate(heldRequests.poll());
        }
    }

    private boolean clustersAwaitingAck() {
        return Stream.of(TypeUrl.CDS, TypeUrl.EDS).map(discoveryServices::get).anyMatch(s -> s != null && s.awaitingAck());
    }

    /**
     * Whether stale resources of this type, found when Envoy reconnected, may be removed now: every type before it in
     * remove order that we serve and this client gets has been answered and ACKed, or we've waited long enough.
     */
    private boolean mayRemove(TypeUrl typeUrl) {
        if (clock.getAsLong() - startedNanos >= RECONNECT_REMOVAL_TIMEOUT_NANOS) {
            return true;
        }
        for (DiscoveryService<RequestT, StateUpdT> earlier : postOrder) {
            if (earlier == null) {
                continue;
            }
            if (earlier.getTypeUrl() == typeUrl) {
                return true;
            }
            boolean used = earlier.isServed() && (subscribesTo == null || subscribesTo.contains(earlier.getTypeUrl()));
            if (used && (!earlier.hasAnswered() || earlier.awaitingAck())) {
                return false;
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
     * config, and loop. Subscription requests are still answered, from our own state.
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
            metrics.onPushComplete();
        }
        while (waitingStateBacklog.take() != null) {
            // Drop it. Nothing more will be sent.
        }
    }

    boolean isFrozen() {
        return frozen;
    }

    private boolean isPushAllowed() {
        return outstandingAcks.isEmpty() && delayUpdatesUntilAckOf == null;
    }

    private void pushAllowedWorkIfAny() {
        if (!outstandingAcks.isEmpty()) {
            return;
        }
        if (pushDeferredReconnectRemovalsIfAny()) {
            return;
        }
        if (isPushAllowed()) {
            pushWaitingChangeIfAny();
        }
    }

    private void beginPush() {
        Preconditions.checkState(currentChange != null, "beginPush() can only be called with a change lined up to be applied.");
        Preconditions.checkState(sendMode == DiscoveryService.SubState.COMPLETED, "beginPush() should be started from COMPLETED state");

        metrics.onPushBegin();

        sendMode = DiscoveryService.SubState.PRE;
        current = preOrder.iterator();
        discoveryServices.forEach((t, s) -> s.onNetworkUpdate(currentChange));
        DiscoveryService<RequestT, StateUpdT> service = nextOrNull();
        waitingOn = service;
        if (service != null) {
            service.sendNetworkUpdatePre();
        }
        // Continue pushing changes as sendNetworkUpdatePre() could be a NOOP, so there
        // wouldn't be an ack to trigger further pushes.
        continuePush();
    }

    private void enqueueChange(StateUpdT diff) {
        waitingStateBacklog.put(diff);
    }

    /**
     * Receive a ServiceChanges that may need to be communicated to envoy.
     * If pushing of a previous ServiceChanges is still in progress, or any unacked messages are outstanding, communicating this change will be deferred until those in-progress changes are finished and all messages have been acked.
     * If there's already a deferred change waiting, this change will be merged with the deferred one, and applied together eventually.
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

    private DiscoveryService<RequestT, StateUpdT> nextOrNull() {
        if (current.hasNext()) {
            return current.next();
        }
        return null;
    }

    /**
     * Complete the current push, can only be called if there is a current change in progress.
     */
    private void finishPush() {
        Preconditions.checkState(currentChange != null, "Cannot finish a push when there isn't one in progress");
        Preconditions.checkState(waitingOn == null, "Cannot finish a push when waiting on an ack for a previous pushed message");
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
    private boolean pushWaitingChangeIfAny() {
        Preconditions.checkState(currentChange == null, "Should not start pushing waiting change if we already have a current change to push.");
        currentChange = waitingStateBacklog.take();
        if (currentChange != null) {
            beginPush();
            return true;
        }
        return false;
    }

    private boolean pushDeferredReconnectRemovalsIfAny() {
        Preconditions.checkState(currentChange == null, "Should not send deferred reconnect removals if we already have a current change to push.");
        for (DiscoveryService<RequestT, StateUpdT> discoveryService: postOrder) {
            if (discoveryService == null) {
                continue;
            }
            if (discoveryService.hasDeferredReconnectRemovals()) {
                if (!mayRemove(discoveryService.getTypeUrl())) {
                    return false; // and not a later type's: removals go in remove order
                }
                discoveryService.sendDeferredReconnectRemovals();
                if (discoveryService.awaitingAck()) {
                    if (outstandingAcks.add(discoveryService)) {
                        metrics.onAwaitingAck();
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Attempt to continue the current push, if possible.
     * This can only be called if there is a change in progress.
     */
    private void continuePush() {
        assertPostInitState();
        Preconditions.checkState(currentChange != null, "Cannot continue pushing a change if we have not begun to push one.");

        if (waitingOn == null || waitingOn.awaitingAck()) {
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
