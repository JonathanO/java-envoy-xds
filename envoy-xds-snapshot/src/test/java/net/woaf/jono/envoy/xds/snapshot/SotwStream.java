package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;
import org.jspecify.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/** A SotW ADS stream from one Envoy, played by the test, to a source of one group. Updates run when the test says. */
final class SotwStream {

    private final Queue<Runnable> tasks = new ArrayDeque<>();

    final SnapshotSource<Object, String, String> source = TestResources.oneGroup(tasks::add);

    final List<DiscoveryResponse> responses = new ArrayList<>();

    private final StreamObserver<DiscoveryRequest> requests;

    @SafeVarargs
    SotwStream(Class<? extends Message>... types) {
        AggregatedDiscoveryService<SnapshotUpdate<Object>, Object> ads = new AggregatedDiscoveryService<>(source,
            SnapshotConfigBuilder.<Object>forTypes(types), node -> XdsConfig.of(new Object()),
            ImmutableList.of(), MergingStateBacklog.factory(), DiscoveryServiceManagerMetrics.METRICS_DISABLED);
        requests = ads.streamAggregatedResources(new StreamObserver<>() {
            @Override
            public void onNext(DiscoveryResponse value) {
                responses.add(value);
            }

            @Override
            public void onError(Throwable t) {
                throw new AssertionError("stream failed", t);
            }

            @Override
            public void onCompleted() {
            }
        });
    }

    /** Subscribe to names (none for a wildcard), and return the answer. */
    DiscoveryResponse request(TypeUrl type, @Nullable DiscoveryResponse previous, String... names) {
        DiscoveryRequest.Builder request = DiscoveryRequest.newBuilder().setTypeUrl(type.getTypeUrl()).addAllResourceNames(List.of(names));
        if (previous != null) {
            request.setVersionInfo(previous.getVersionInfo()).setResponseNonce(previous.getNonce());
        }
        int before = responses.size();
        send(request);
        assertThat(responses).as("an answer to %s", type).hasSizeGreaterThan(before);
        return last();
    }

    /** SotW requests list every name subscribed to, ACKs included. */
    void ack(DiscoveryResponse response, String... names) {
        send(DiscoveryRequest.newBuilder().setTypeUrl(response.getTypeUrl()).setVersionInfo(response.getVersionInfo())
            .setResponseNonce(response.getNonce()).addAllResourceNames(List.of(names)));
    }

    void send(DiscoveryRequest.Builder request) {
        requests.onNext(request.setNode(Node.newBuilder().setId("envoy")).build());
        runTasks();
    }

    /** Deliver the source's updates to the stream. */
    void runTasks() {
        for (Runnable task = tasks.poll(); task != null; task = tasks.poll()) {
            task.run();
        }
    }

    DiscoveryResponse last() {
        return responses.get(responses.size() - 1);
    }

    DiscoveryResponse previous(TypeUrl type) {
        return previous(type, 0);
    }

    // The last response of a type, or the one back before it.
    DiscoveryResponse previous(TypeUrl type, int back) {
        List<DiscoveryResponse> ofType = responses.stream().filter(r -> r.getTypeUrl().equals(type.getTypeUrl())).toList();
        return ofType.get(ofType.size() - 1 - back);
    }
}
