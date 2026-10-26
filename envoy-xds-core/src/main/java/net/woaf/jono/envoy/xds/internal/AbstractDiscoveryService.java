package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.TypeUrl;
import org.jspecify.annotations.Nullable;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Predicate;

public abstract class AbstractDiscoveryService<T extends Message, E extends Message, StateUpdT, DetailsT> implements DiscoveryService<T, StateUpdT> {

    private static final Logger log = LoggerFactory.getLogger(AbstractDiscoveryService.class);

    // SotW re-sends every unchanged resource on each push, and builders that share messages (e.g. snapshots) send one
    // instance to every client, so serialise each instance once. Weak keys compare by identity and drop the entry once
    // nothing holds the message.
    private static final LoadingCache<Message, Any> PACKED = CacheBuilder.newBuilder().weakKeys().build(CacheLoader.from(Any::pack));

    private final TypeUrl myTypeUrl;

    private final IncrementalConfigBuilder<E, StateUpdT, DetailsT> configBuilder;

    private final NodeConfig<DetailsT> nodeConfig;

    private @Nullable StateUpdT currentState = null;

    private SubState currentSubState = SubState.COMPLETED;

    private boolean active = false;

    private boolean answered = false;

    public AbstractDiscoveryService(TypeUrl myTypeUrl, IncrementalConfigBuilder<E, StateUpdT, DetailsT> configBuilder, NodeConfig<DetailsT> nodeConfig) {
        this.myTypeUrl = myTypeUrl;
        this.configBuilder = configBuilder;
        this.nodeConfig = nodeConfig;
    }

    protected abstract void processRequest(T value);

    @Override
    public void processUpdate(CommonDiscoveryRequest<T> value) {
        Preconditions.checkState(currentState != null, "Discovery must be initialized");
        TypeUrl typeUrl = TypeUrl.of(value.typeUrl());
        if (!myTypeUrl.equals(typeUrl)) {
            throw new IllegalArgumentException(myTypeUrl.name() + " discovery service got called for request of type " + value.typeUrl());
        }
        active = true;
        processRequest(value.message());
    }

    /**
     * Record that a response has gone to Envoy.
     */
    protected void markAnswered() {
        answered = true;
    }

    @Override
    public boolean isServed() {
        return !(configBuilder instanceof NoOpConfigBuilder);
    }

    @Override
    public boolean hasAnswered() {
        return answered;
    }

    protected SubState getCurrentSubState() {
        return currentSubState;
    }

    protected void checkSubNames(IncrementalConfigBuilder.Resources<E> resources, Predicate<String> resourceInSubListChange) {
        if (resources.resources().stream()
                .anyMatch(m -> !resourceInSubListChange.test(m.name()))) {
            log.warn("Config builder for {} returned unasked for resources!", myTypeUrl.getTypeUrl());
        }
    }

    protected void checkSubNames(IncrementalConfigBuilder.Response<E> response, Predicate<String> resourceInSubListChange) {
        if (
                response.removes().stream()
                        .anyMatch(m -> !resourceInSubListChange.test(m))
                || response.addAndUpdates().stream()
                        .anyMatch(m -> !resourceInSubListChange.test(m.name()))
        ) {
            log.warn("Config builder for {} returned unasked for messages!", myTypeUrl.getTypeUrl());
        }
    }

    private StateUpdT state() {
        return Preconditions.checkNotNull(currentState, "Discovery must be initialized");
    }

    protected IncrementalConfigBuilder.Resources<E> getResources(Predicate<String> resourceInSubListChange) {
        final IncrementalConfigBuilder.Resources<E> resources;
        if (SubState.PRE.equals(currentSubState)) {
            resources = configBuilder.getResourcesAddOrder(state(), resourceInSubListChange, nodeConfig.xdsConfig().clientDetails());
        } else {
            resources = configBuilder.getResourcesRemoveOrder(state(), resourceInSubListChange, nodeConfig.xdsConfig().clientDetails());
        }
        checkSubNames(resources, resourceInSubListChange);
        return resources;
    }

    @Override
    public abstract boolean awaitingAck();

    @Override
    public void init(StateUpdT state) {
        Preconditions.checkState(currentState == null, "Discovery service already initialized");
        currentState = state;
    }

    @Override
    public void onNetworkUpdate(StateUpdT changes) {
        Preconditions.checkState(currentState != null, "Discovery must be initialized");
        if (changes.equals(currentState)) {
            return;
        }
        if (!SubState.COMPLETED.equals(currentSubState)) {
            throw new IllegalStateException("Network map update committed while still in dirty state!");
        }
        currentState = changes;
        currentSubState = SubState.PRE;
    }

    protected abstract Predicate<String> subFilter();

    protected boolean updateState(IncrementalConfigBuilder.Response<E> response) {
        return true;
    }

    @CanIgnoreReturnValue // SotW keeps the result through updateState
    protected Optional<IncrementalConfigBuilder.Response<E>> applyPre() {
        if (!SubState.PRE.equals(currentSubState)) {
            return Optional.empty();
        }
        currentSubState = SubState.POST;
        if (!active) {
            // We've never been asked for this resource. Since subFilter should return false for everything it ought to be safe to call configBuilder, but don't because:
            // a) It might do costly work we know we don't need;
            // b) It might return stuff. Envoy gets really angry if ADS returns a message type that it never requested.
            return Optional.empty();
        }
        IncrementalConfigBuilder.Response<E> resources = configBuilder.addOrder(state(), subFilter(), nodeConfig.xdsConfig().clientDetails());
        checkSubNames(resources, subFilter());
        if (updateState(resources)) {
            return Optional.of(resources);
        }
        return Optional.empty();
    }

    protected abstract void pushNewState(IncrementalConfigBuilder.Response<E> response);

    @Override
    public void sendNetworkUpdatePre() {
        Preconditions.checkState(currentState != null, "Discovery must be initialized");
        applyPre().ifPresent(this::pushNewState);
    }

    @CanIgnoreReturnValue // SotW keeps the result through updateState
    protected Optional<IncrementalConfigBuilder.Response<E>> applyPost() {
        if (!SubState.POST.equals(currentSubState)) {
            return Optional.empty();
        }
        currentSubState = SubState.COMPLETED;
        if (!active) {
            // We've never been asked for this resource. Since subFilter should return false for everything it ought to be safe to call configBuilder, but don't because:
            // a) It might do costly work we know we don't need;
            // b) It might return stuff. Envoy gets really angry if ADS returns a message type that it never requested.
            return Optional.empty();
        }
        IncrementalConfigBuilder.Response<E> resources = configBuilder.removeOrder(state(), subFilter(), nodeConfig.xdsConfig().clientDetails());
        checkSubNames(resources, subFilter());
        if (updateState(resources)) {
            return Optional.of(resources);
        }
        return Optional.empty();
    }

    @Override
    public void sendNetworkUpdatePost() {
        Preconditions.checkState(currentState != null, "Discovery must be initialized");
        applyPost().ifPresent(this::pushNewState);
    }

    @Override
    public void abandonChange() {
        currentSubState = SubState.COMPLETED;
    }

    protected Any pack(Message o) {
        return PACKED.getUnchecked(o);
    }

    @Override
    public TypeUrl getTypeUrl() {
        return myTypeUrl;
    }
}
