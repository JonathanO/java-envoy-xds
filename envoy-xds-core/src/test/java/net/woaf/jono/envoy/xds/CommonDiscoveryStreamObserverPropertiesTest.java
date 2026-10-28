package net.woaf.jono.envoy.xds;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.rpc.Status;
import dev.hegel.HegelTest;
import dev.hegel.Invariant;
import dev.hegel.Rule;
import dev.hegel.Stateful;
import dev.hegel.TestCase;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.ScopedRouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.Secret;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import io.envoyproxy.envoy.service.runtime.v3.Runtime;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import net.woaf.jono.envoy.xds.api.ClientHandle;
import net.woaf.jono.envoy.xds.api.ClusterEventSource;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;
import net.woaf.jono.envoy.xds.api.XdsEventListener;
import net.woaf.jono.envoy.xds.api.utils.QueueingStateBacklog;
import org.jspecify.annotations.Nullable;

import static dev.hegel.Generators.booleans;
import static dev.hegel.Generators.integers;
import static dev.hegel.Generators.sampledFrom;
import static dev.hegel.Generators.sets;
import static dev.hegel.Generators.text;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whatever a client sends, in whatever order, and whatever the source does meanwhile, a stream answers only what was
 * asked, fails only for protocol violations, and always cleans up when it ends. Runs the real stack, both protocols.
 */
public class CommonDiscoveryStreamObserverPropertiesTest {

    private static final List<String> NAMES = List.of("a", "b");
    private static final List<String> TYPE_URLS = Stream.concat(Arrays.stream(TypeUrl.values()).map(TypeUrl::getTypeUrl),
        Stream.of("type.googleapis.com/envoy.config.core.v3.TypedExtensionConfig", "")).toList();

    @HegelTest(testCases = 1000)
    void streamsStayConsistent(TestCase tc) {
        Stateful.run(new Machine(tc.draw(booleans(), "delta")), tc);
    }

    private record State(int version) {
    }

    /** Serves names a and b of one type, changing with the state's version. */
    private record Builder<M extends Message>(Class<M> type, Function<String, M> build) implements IncrementalConfigBuilder<M, State, Object> {

        private List<NamedMessage<M>> matching(Predicate<String> names) {
            return NAMES.stream().filter(names).map(n -> new NamedMessage<>(n, build.apply(n))).toList();
        }

        @Override
        public Response<M> addOrder(State diff, Predicate<String> names, Object clientDetails) {
            return new Response<>(matching(names), List.of());
        }

        @Override
        public Response<M> removeOrder(State diff, Predicate<String> names, Object clientDetails) {
            return Response.empty();
        }

        @Override
        public Resources<M> getResourcesAddOrder(State state, Predicate<String> names, Object clientDetails) {
            return new Resources<>(matching(names));
        }

        @Override
        public Resources<M> getResourcesRemoveOrder(State state, Predicate<String> names, Object clientDetails) {
            return new Resources<>(matching(names));
        }

        @Override
        public Class<M> handlesType() {
            return type;
        }
    }

    private static final class Source implements ClusterEventSource<State> {
        private final List<ClusterManagerEventListener<State>> subscribers = new ArrayList<>();
        private State current = new State(0);

        @Override
        public State subscribe(ClusterManagerEventListener<State> listener) {
            subscribers.add(listener);
            return current;
        }

        @Override
        public void unsubscribe(ClusterManagerEventListener<State> listener) {
            subscribers.remove(listener);
        }

        void change() {
            current = new State(current.version() + 1);
            List.copyOf(subscribers).forEach(s -> s.onNetworkChange(current));
        }
    }

    /** Records what the server says to the client: a response observer for either protocol. */
    private static final class Client implements StreamObserver<Message> {
        private final List<Message> responses = new ArrayList<>();
        private final Set<String> asked = new HashSet<>();
        private boolean ended = false;

        @Override
        public void onNext(Message value) {
            assertThat(ended).as("a response after the stream ended").isFalse();
            responses.add(value);
        }

        @Override
        public void onError(Throwable t) {
            ended = true;
        }

        @Override
        public void onCompleted() {
            ended = true;
        }
    }

    private static final class Listeners implements XdsEventListener<Object> {
        private int connected = 0;
        private int disconnected = 0;
        private int nacks = 0;
        private @Nullable ClientHandle handle = null;

        @Override
        public void onNewClient(ClientHandle clientHandle, Node node, XdsConfig<Object> config) {
            connected++;
            handle = clientHandle;
        }

        @Override
        public void onClientDisconnected(ClientHandle clientHandle, Node node) {
            disconnected++;
        }

        @Override
        public void onClientNack(ClientHandle clientHandle, Node node, TypeUrl typeUrl, Status errorDetail) {
            nacks++;
        }
    }

    // Not private: Hegel calls its rules and invariants reflectively, which Error Prone can't see.
    static final class Machine {
        private final boolean delta;
        private final Source source = new Source();
        private final Client client = new Client();
        private final Listeners listeners = new Listeners();
        private final StreamObserver<Message> requests;
        private final List<Throwable> escaped = new ArrayList<>();
        private boolean clientEnded = false;
        private int nacksSent = 0;

        @SuppressWarnings("unchecked") // each protocol's observers take that protocol's messages
        Machine(boolean delta) {
            this.delta = delta;
            ImmutableList<IncrementalConfigBuilder<?, State, Object>> builders = ImmutableList.of(
                new Builder<>(Cluster.class, n -> Cluster.newBuilder().setName(n).setAltStatName("v" + source.current.version()).build()),
                new Builder<>(ClusterLoadAssignment.class, n -> ClusterLoadAssignment.newBuilder().setClusterName(n).build()),
                new Builder<>(Listener.class, n -> Listener.newBuilder().setName(n).build()),
                new Builder<>(RouteConfiguration.class, n -> RouteConfiguration.newBuilder().setName(n).build()),
                new Builder<>(ScopedRouteConfiguration.class, n -> ScopedRouteConfiguration.newBuilder().setName(n).build()),
                new Builder<>(VirtualHost.class, n -> VirtualHost.newBuilder().setName(n).build()),
                new Builder<>(Secret.class, n -> Secret.newBuilder().setName(n).build()),
                new Builder<>(Runtime.class, n -> Runtime.newBuilder().setName(n).build()));
            AggregatedDiscoveryService<State, Object> ads = new AggregatedDiscoveryService<>(source, builders, node -> XdsConfig.of(new Object()),
                ImmutableList.of(listeners), QueueingStateBacklog.factory(), DiscoveryServiceManagerMetrics.METRICS_DISABLED);
            StreamObserver<?> raw = delta
                ? ads.deltaAggregatedResources((StreamObserver<DeltaDiscoveryResponse>) (StreamObserver<?>) client)
                : ads.streamAggregatedResources((StreamObserver<DiscoveryResponse>) (StreamObserver<?>) client);
            requests = (StreamObserver<Message>) raw;
        }

        private boolean live() {
            return !client.ended && !clientEnded;
        }

        private void call(Runnable r) {
            try {
                r.run();
            } catch (RuntimeException e) {
                escaped.add(e);
            }
        }

        @Rule(weight = 6)
        void request(TestCase tc) {
            tc.assume(live());
            String typeUrl = tc.draw(sampledFrom(TYPE_URLS), "type");
            client.asked.add(typeUrl);
            Message request = delta ? deltaRequest(tc, typeUrl) : sotwRequest(tc, typeUrl);
            boolean nack = request instanceof DeltaDiscoveryRequest d ? d.hasErrorDetail() : ((DiscoveryRequest) request).hasErrorDetail();
            if (nack && TypeUrl.of(typeUrl) != null) {
                nacksSent++;
            }
            call(() -> requests.onNext(request));
        }

        @Rule(weight = 2)
        void sourceChanges(TestCase tc) {
            call(source::change);
        }

        @Rule
        void resync(TestCase tc) {
            tc.assume(live() && listeners.handle != null);
            call(() -> listeners.handle.resync());
        }

        @Rule
        void clientFails(TestCase tc) {
            tc.assume(live());
            clientEnded = true;
            call(() -> requests.onError(new RuntimeException("client went away")));
        }

        @Rule
        void clientCompletes(TestCase tc) {
            tc.assume(live());
            clientEnded = true;
            call(requests::onCompleted);
        }

        private Node node(TestCase tc) {
            return Node.newBuilder().setId(tc.draw(text().maxSize(3), "node id")).build();
        }

        private String nonce(TestCase tc) {
            List<String> sent = client.responses.stream().map(r -> r instanceof DeltaDiscoveryResponse d ? d.getNonce() : ((DiscoveryResponse) r).getNonce()).toList();
            if (!sent.isEmpty() && tc.draw(booleans(), "echo a nonce")) {
                return tc.draw(sampledFrom(sent), "nonce");
            }
            return tc.draw(sampledFrom("", "bogus"), "nonce");
        }

        private @Nullable Status errorDetail(TestCase tc) {
            return tc.draw(integers().min(0).max(9), "nack") == 0 ? Status.newBuilder().setCode(3).setMessage("rejected").build() : null;
        }

        private Message sotwRequest(TestCase tc, String typeUrl) {
            DiscoveryRequest.Builder b = DiscoveryRequest.newBuilder().setNode(node(tc)).setTypeUrl(typeUrl)
                .setResponseNonce(nonce(tc))
                .setVersionInfo(tc.draw(sampledFrom("", "1", "2", "not-a-number"), "version"))
                .addAllResourceNames(tc.draw(sets(sampledFrom("a", "b", "*")), "names"));
            Status error = errorDetail(tc);
            if (error != null) {
                b.setErrorDetail(error);
            }
            return b.build();
        }

        private Message deltaRequest(TestCase tc, String typeUrl) {
            DeltaDiscoveryRequest.Builder b = DeltaDiscoveryRequest.newBuilder().setNode(node(tc)).setTypeUrl(typeUrl)
                .setResponseNonce(nonce(tc))
                .addAllResourceNamesSubscribe(tc.draw(sets(sampledFrom("a", "b", "*")), "subscribe"))
                .addAllResourceNamesUnsubscribe(tc.draw(sets(sampledFrom("a", "b", "*")), "unsubscribe"));
            for (String name : tc.draw(sets(sampledFrom("a", "b")), "already has")) {
                b.putInitialResourceVersions(name, "v");
            }
            Status error = errorDetail(tc);
            if (error != null) {
                b.setErrorDetail(error);
            }
            return b.build();
        }

        @Invariant(alwaysRun = true)
        void endsCleanly(TestCase tc) {
            if (!live()) {
                assertThat(source.subscribers).as("still subscribed after the stream ended").isEmpty();
                assertThat(listeners.disconnected).as("disconnects").isEqualTo(listeners.connected);
            }
            assertThat(listeners.connected).as("connects").isLessThanOrEqualTo(1);
        }

        @Invariant(alwaysRun = true)
        void listenersHearOfEveryNack(TestCase tc) {
            assertThat(listeners.nacks).as("NACKs passed to onClientNack").isEqualTo(nacksSent);
        }

        @Invariant(alwaysRun = true)
        void answersOnlyWhatWasAsked(TestCase tc) {
            for (Message response : client.responses) {
                String typeUrl = response instanceof DeltaDiscoveryResponse d ? d.getTypeUrl() : ((DiscoveryResponse) response).getTypeUrl();
                List<Any> resources = response instanceof DeltaDiscoveryResponse d
                    ? d.getResourcesList().stream().map(Resource::getResource).toList()
                    : ((DiscoveryResponse) response).getResourcesList();
                assertThat(client.asked).as("answered a type never asked for").contains(typeUrl);
                assertThat(TypeUrl.of(typeUrl)).as("answered an unknown type").isNotNull();
                assertThat(resources).allSatisfy(a -> assertThat(a.getTypeUrl()).isEqualTo(typeUrl));
            }
        }

        @Invariant(alwaysRun = true)
        void failsOnlyForProtocolViolations(TestCase tc) {
            assertThat(escaped).allSatisfy(e -> assertThat(e).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("subscribe and unsubscribe from the same resource"));
            if (!escaped.isEmpty()) {
                assertThat(client.ended).as("an exception escaped but the stream is still open").isTrue();
            }
        }
    }
}
