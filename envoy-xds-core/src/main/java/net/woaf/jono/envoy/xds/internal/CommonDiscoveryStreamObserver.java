package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.TypeUrl;
import org.jspecify.annotations.Nullable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.xds.api.ClientConfigProvider;
import net.woaf.jono.envoy.xds.api.ClientHandle;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import net.woaf.jono.envoy.xds.api.XdsEventListener;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.Closeable;
import java.util.function.Function;

public class CommonDiscoveryStreamObserver<T extends Message, R extends Message, StateUpdT, DetailsT>
        implements StreamObserver<T>, ClusterManagerEventListener<StateUpdT>, ClientHandle {

    private static final Logger log = LoggerFactory.getLogger(CommonDiscoveryStreamObserver.class);

    /**
     * The MDC key that says which Envoy a log is about. Each entry point puts the node ID there, so everything logged
     * under it, by the manager and services too, carries it, then puts back what the thread had.
     */
    private static final String NODE_MDC_KEY = "node";

    @GuardedBy("this")
    private boolean isDead = false;

    private final StreamObserver<R> responseObserver;
    private final ClusterEventSource<StateUpdT> clusterManager;
    @GuardedBy("this")
    private @Nullable XdsConfig<DetailsT> xdsConfig = null;

    private final DiscoveryServiceManagerFactory<T, R, StateUpdT, DetailsT> discoveryServiceManagerFactory;
    @GuardedBy("this")
    private @Nullable DiscoveryServiceManager<T, StateUpdT> discoveryServiceManager = null;

    private final Function<T, CommonDiscoveryRequest<T>> commonDiscoveryRequestConverter;

    private final DiscoveryServiceManagerMetrics metrics;

    private final ImmutableList<XdsEventListener<DetailsT>> listeners;

    private final ClientConfigProvider<DetailsT> configProvider;

    @GuardedBy("this")
    private @Nullable String nodeId = null;
    @GuardedBy("this")
    private @Nullable String clusterId = null;

    @GuardedBy("this")
    private @Nullable Node node = null;

    public CommonDiscoveryStreamObserver(
        StreamObserver<R> responseObserver,
        ClusterEventSource<StateUpdT> clusterManager,
        DiscoveryServiceManagerFactory<T, R, StateUpdT, DetailsT> discoveryServiceManagerFactory,
        Function<T, CommonDiscoveryRequest<T>> commonDiscoveryRequestConverter,
        ClientConfigProvider<DetailsT> configProvider,
        ImmutableList<XdsEventListener<DetailsT>> listeners,
        DiscoveryServiceManagerMetrics metrics) {
        this.responseObserver = responseObserver;
        this.clusterManager = clusterManager;
        this.discoveryServiceManagerFactory = discoveryServiceManagerFactory;
        this.commonDiscoveryRequestConverter = commonDiscoveryRequestConverter;
        this.metrics = metrics;
        this.listeners = listeners;
        this.configProvider = configProvider;
    }

    @GuardedBy("this")
    private void extractNodeData(Node node) {
        nodeId = node.getId();
        clusterId = node.getCluster();
        MDC.put(NODE_MDC_KEY, nodeId); // first, so the lookup's and listeners' logs carry it too

        XdsConfig<DetailsT> config = configProvider.lookup(node);

        this.node = node;
        xdsConfig = config;
        notifyClientConnected(node, config);
    }

    /**
     * {@return what the MDC had for the node, to give restoreMdc when done}
     * Puts this stream's node in it, once we know it.
     */
    @GuardedBy("this")
    private @Nullable String putNodeInMdc() {
        String outer = MDC.get(NODE_MDC_KEY);
        if (nodeId != null) {
            MDC.put(NODE_MDC_KEY, nodeId);
        }
        return outer;
    }

    /** Puts back what the thread had, so a pooled thread doesn't log as this Envoy for the next one. */
    private static void restoreMdc(@Nullable String outer) {
        if (outer == null) {
            MDC.remove(NODE_MDC_KEY);
        } else {
            MDC.put(NODE_MDC_KEY, outer);
        }
    }

    @Override
    public synchronized void onNext(T concreteValue) {
        String outerNode = putNodeInMdc();
        try {
            if (isDead) {
                // e.g. sent before Envoy saw us close the stream to resync it
                log.debug("Ignoring a request from {} on a closed stream", nodeId);
                return;
            }
            CommonDiscoveryRequest<T> value = commonDiscoveryRequestConverter.apply(concreteValue);

            if (node == null) {
                log.debug("New envoy connected: {}", value.node().getId());
                extractNodeData(value.node());
            }
            final Node node = Preconditions.checkNotNull(this.node);
            final XdsConfig<DetailsT> xdsConfig = Preconditions.checkNotNull(this.xdsConfig);

            TypeUrl typeUrl = TypeUrl.of(value.typeUrl());
            if (typeUrl == null) {
                // Leave it unanswered, like a type nobody serves: closing the stream would take every other type down
                // with it, and Envoy would reconnect and ask again.
                log.warn("Client {} in cluster {} asked for unknown type URL {}, ignoring", nodeId, clusterId, value.typeUrl());
                return;
            }
            log.debug("DiscoveryRequest T={}", value.typeUrl());
            if (value.errorDetail() != null) {
                log.warn("Client {} rejected {}, so we'll send it no more changes on this stream: {}", nodeId, value.typeUrl(), value.errorDetail());
                notifyClientNack(node, typeUrl, value.errorDetail());
            }
            if (discoveryServiceManager == null) {
                discoveryServiceManager = discoveryServiceManagerFactory.build(responseObserver, xdsConfig, metrics);
                discoveryServiceManager.init(clusterManager.subscribe(this, xdsConfig.clientDetails()), xdsConfig.delayUpdatesUntilAckOf(), xdsConfig.subscribesTo());
            }

            discoveryServiceManager.processUpdate(value);
        } catch (Throwable t) {
            this.onError(t); // Despite docs on interface, upstream is not calling onError :-(
            throw t;
        } finally {
            restoreMdc(outerNode);
        }
    }

    private static class RunWithExceptions implements Closeable {

        private @Nullable Throwable caught = null;

        private final String doingWhat;

        RunWithExceptions(String doingWhat) {
            this.doingWhat = doingWhat;
        }

        private void exec(Runnable thing) {
            try {
                thing.run();
            } catch (Error e) {
                if (caught == null) {
                    caught = e;
                } else if (caught instanceof Error) {
                    // If we'd already caught an Error, then throw that one and suppress this one.
                    caught.addSuppressed(e);
                } else {
                    // We want to make sure Error gets thrown rather than whatever Exceptions we'd captured.
                    e.addSuppressed(caught);
                    caught = e;
                }
            } catch (Throwable e) {
                // The first Throwable we caught will be rethrown, all others will be suppressed unless they are Error (see catch block above.)
                if (caught == null) {
                    caught = e;
                } else {
                    caught.addSuppressed(e);
                }
            }
        }

        @Override
        public void close() {
            if (caught != null) {
                // Directly rethrow if we can, but we'll have to wrap checked Exceptions in a RuntimeException.
                if (caught instanceof Error rethrow) {
                    throw rethrow;
                }
                if (caught instanceof RuntimeException rethrow) {
                    throw rethrow;
                }
                throw new IllegalStateException("Caught exception while " + doingWhat, caught);
            }
        }
    }

    @GuardedBy("this")
    private void notifyClientDisconnected(RunWithExceptions runner) {
        Node node = this.node;
        if (node == null) {
            return;
        }
        for (XdsEventListener<DetailsT> listener: listeners) {
            runner.exec(() -> listener.onClientDisconnected(this, node));
        }
    }

    @GuardedBy("this")
    private void notifyClientConnected(Node node, XdsConfig<DetailsT> config) {
        try (RunWithExceptions runner = new RunWithExceptions("notifying XdsEventListeners of client connection")) {
            for (XdsEventListener<DetailsT> listener: listeners) {
                runner.exec(() -> listener.onNewClient(this, node, config));
            }
        }
    }

    @GuardedBy("this")
    private void notifyClientNack(Node node, TypeUrl typeUrl, com.google.rpc.Status errorDetail) {
        try (RunWithExceptions runner = new RunWithExceptions("notifying XdsEventListeners of a NACK")) {
            for (XdsEventListener<DetailsT> listener: listeners) {
                runner.exec(() -> listener.onClientNack(this, node, typeUrl, errorDetail));
            }
        }
    }

    @Override
    public synchronized void resync() {
        String outerNode = putNodeInMdc();
        try {
            if (isDead) {
                return;
            }
            log.info("Closing stream to {} so it resyncs", nodeId);
            try (RunWithExceptions runner = new RunWithExceptions("closing stream to resync")) {
                cleanupOnDisconnect(runner);
                runner.exec(responseObserver::onCompleted);
            }
        } finally {
            restoreMdc(outerNode);
        }
    }

    @GuardedBy("this")
    private void cleanupOnDisconnect(RunWithExceptions runner) {
        isDead = true;
        DiscoveryServiceManager<T, StateUpdT> dsm = discoveryServiceManager;
        if (dsm != null) {
            runner.exec(() -> clusterManager.unsubscribe(this));
            runner.exec(dsm::close);
            discoveryServiceManager = null;
        }
        notifyClientDisconnected(runner);
    }

    @Override
    public synchronized void onError(Throwable t) {
        String outerNode = putNodeInMdc();
        try {
            if (isDead) {
                log.warn("onError called on already dead CDSO. Maybe upstream fixed the bug where onError wasn't getting called?");
                return;
            }
            if (!(t instanceof StatusRuntimeException r) || !Status.Code.CANCELLED.equals(r.getStatus().getCode())) {
                log.warn("Error streaming to an ADS client", t);
            }
            closeWithError(t);
        } finally {
            restoreMdc(outerNode);
        }
    }

    @GuardedBy("this")
    private void closeWithError(Throwable t) {
        try (RunWithExceptions runner = new RunWithExceptions("handling stream error")) {
            cleanupOnDisconnect(runner);
            runner.exec(() -> responseObserver.onError(t));
        }
    }

    @Override
    public synchronized void onCompleted() {
        String outerNode = putNodeInMdc();
        try {
            if (isDead) {
                log.warn("onCompleted called on already dead CDSO.");
                return;
            }
            log.debug("Completed: " + this.node);

            try (RunWithExceptions runner = new RunWithExceptions("handling stream completed")) {
                cleanupOnDisconnect(runner);
                runner.exec(responseObserver::onCompleted);
            }
        } finally {
            restoreMdc(outerNode);
        }
    }

    @Override
    public synchronized void onNetworkChange(StateUpdT diff) {
        String outerNode = putNodeInMdc();
        try {
            if (isDead) {
                return;
            }
            // Set before we subscribe, and only cleared once dead.
            Preconditions.checkNotNull(discoveryServiceManager).pushUpdates(diff);
        } catch (Throwable t) {
            // Logged and handled here, so the source (e.g. SerialEventListener) doesn't log it again.
            log.error("Failed to push an update to {}, so closing its stream", nodeId, t);
            if (!isDead) {
                closeWithError(t);
            }
            if (t instanceof Error e) {
                throw e;
            }
        } finally {
            restoreMdc(outerNode);
        }
    }
}
