package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.rpc.Status;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
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
import org.junit.jupiter.api.Test;

import static net.woaf.jono.envoy.xds.snapshot.TestResources.cla;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cluster;
import static org.assertj.core.api.Assertions.assertThat;

public class SotwNackTest {

    private final Queue<Runnable> tasks = new ArrayDeque<>();

    private final SnapshotSource<Object> source = new SnapshotSource<>(null, tasks::add);

    private final List<DiscoveryResponse> responses = new ArrayList<>();

    private final StreamObserver<DiscoveryRequest> requests = connect();

    @Test
    public void subscribingAfterANackSendsNoMoreOfTheRejectedChange() {
        source.update(e -> e.put(cluster("a")).put(cla("a", "10.0.0.1")));
        DiscoveryResponse cds = request(TypeUrl.CDS, null);
        ack(cds);
        ack(request(TypeUrl.EDS, null, "a"), "a");

        // Clusters go first, so the new endpoints for a wait for Envoy to ACK them. It NACKs instead.
        source.update(e -> e.put(cluster("b")).put(cla("a", "10.0.0.2")).put(cla("b", "10.0.0.3")));
        runTasks();
        DiscoveryResponse rejected = last();
        assertThat(rejected.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        send(DiscoveryRequest.newBuilder().setTypeUrl(TypeUrl.CDS.getTypeUrl()).setVersionInfo(cds.getVersionInfo())
            .setResponseNonce(rejected.getNonce()).setErrorDetail(Status.newBuilder().setMessage("bad cluster")));

        // Envoy accepted cluster b anyway, so it asks for b's endpoints.
        DiscoveryResponse eds = request(TypeUrl.EDS, previous(TypeUrl.EDS), "a", "b");

        assertThat(eds.getResourcesList()).extracting(any -> any.unpack(ClusterLoadAssignment.class))
            .as("a keeps the endpoints Envoy has; b, newly asked for, is answered")
            .containsExactlyInAnyOrder(cla("a", "10.0.0.1"), cla("b", "10.0.0.3"));
    }

    private StreamObserver<DiscoveryRequest> connect() {
        AggregatedDiscoveryService<SnapshotUpdate<Object>, Object> ads = new AggregatedDiscoveryService<>(source,
            SnapshotConfigBuilder.<Object>forTypes(Cluster.class, ClusterLoadAssignment.class), node -> XdsConfig.of(new Object()),
            ImmutableList.of(), MergingStateBacklog.factory(), DiscoveryServiceManagerMetrics.METRICS_DISABLED);
        return ads.streamAggregatedResources(new StreamObserver<>() {
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
    private DiscoveryResponse request(TypeUrl type, @Nullable DiscoveryResponse previous, String... names) {
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
    private void ack(DiscoveryResponse response, String... names) {
        send(DiscoveryRequest.newBuilder().setTypeUrl(response.getTypeUrl()).setVersionInfo(response.getVersionInfo())
            .setResponseNonce(response.getNonce()).addAllResourceNames(List.of(names)));
    }

    private void send(DiscoveryRequest.Builder request) {
        requests.onNext(request.setNode(Node.newBuilder().setId("envoy")).build());
        runTasks();
    }

    private void runTasks() {
        for (Runnable task = tasks.poll(); task != null; task = tasks.poll()) {
            task.run();
        }
    }

    private DiscoveryResponse last() {
        return responses.get(responses.size() - 1);
    }

    private DiscoveryResponse previous(TypeUrl type) {
        return responses.stream().filter(r -> r.getTypeUrl().equals(type.getTypeUrl())).reduce((a, b) -> b).orElseThrow();
    }
}
