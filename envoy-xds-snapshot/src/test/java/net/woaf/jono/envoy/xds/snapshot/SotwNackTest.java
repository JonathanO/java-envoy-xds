package net.woaf.jono.envoy.xds.snapshot;

import com.google.rpc.Status;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import java.util.List;
import net.woaf.jono.envoy.xds.TypeUrl;
import org.junit.jupiter.api.Test;

import static net.woaf.jono.envoy.xds.snapshot.TestResources.all;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cla;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cluster;
import static org.assertj.core.api.Assertions.assertThat;

public class SotwNackTest {

    private final SotwStream stream = new SotwStream(Cluster.class, ClusterLoadAssignment.class);

    private final SnapshotSource<Object, String, String> source = stream.source;

    private final List<DiscoveryResponse> responses = stream.responses;

    @Test
    public void subscribingAfterANackSendsNoMoreOfTheRejectedChange() {
        source.update(all(e -> e.put(cluster("a")).put(cla("a", "10.0.0.1"))));
        DiscoveryResponse cds = stream.request(TypeUrl.CDS, null);
        stream.ack(cds);
        stream.ack(stream.request(TypeUrl.EDS, null, "a"), "a");

        // Clusters go first, so the new endpoints for a wait for Envoy to ACK them. It NACKs instead.
        source.update(all(e -> e.put(cluster("b")).put(cla("a", "10.0.0.2")).put(cla("b", "10.0.0.3"))));
        stream.runTasks();
        DiscoveryResponse rejected = stream.last();
        assertThat(rejected.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        stream.send(DiscoveryRequest.newBuilder().setTypeUrl(TypeUrl.CDS.getTypeUrl()).setVersionInfo(cds.getVersionInfo())
            .setResponseNonce(rejected.getNonce()).setErrorDetail(Status.newBuilder().setMessage("bad cluster")));

        // Envoy accepted cluster b anyway, so it asks for b's endpoints.
        DiscoveryResponse eds = stream.request(TypeUrl.EDS, stream.previous(TypeUrl.EDS), "a", "b");

        assertThat(eds.getResourcesList()).extracting(any -> any.unpack(ClusterLoadAssignment.class))
            .as("a keeps the endpoints Envoy has; b, newly asked for, is answered")
            .containsExactlyInAnyOrder(cla("a", "10.0.0.1"), cla("b", "10.0.0.3"));
    }

    @Test
    public void subscribingAfterANackOfTheSameTypeRepeatsWhatWasRejected() {
        source.update(all(e -> e.put(cluster("a")).put(cla("a", "10.0.0.1"))));
        stream.ack(stream.request(TypeUrl.CDS, null));
        stream.ack(stream.request(TypeUrl.EDS, null, "a"), "a");

        source.update(all(e -> e.put(cluster("b")).put(cla("a", "10.0.0.2")).put(cla("b", "10.0.0.3"))));
        stream.runTasks();
        stream.ack(stream.last());
        DiscoveryResponse rejected = stream.last();
        assertThat(rejected.getResourcesList()).extracting(any -> any.unpack(ClusterLoadAssignment.class)).containsExactly(cla("a", "10.0.0.2"));

        // Envoy NACKs the endpoints, and in the same request asks for b's, as it took cluster b.
        int before = responses.size();
        stream.send(DiscoveryRequest.newBuilder().setTypeUrl(TypeUrl.EDS.getTypeUrl()).setVersionInfo(stream.previous(TypeUrl.EDS, 1).getVersionInfo())
            .setResponseNonce(rejected.getNonce()).addAllResourceNames(List.of("a", "b")).setErrorDetail(Status.newBuilder().setMessage("bad endpoints")));

        // Envoy may have taken part of what it rejected, so the only answer that leaves it as it is, is the same one.
        // Rolling a back would also roll back anything else in that answer that Envoy took.
        assertThat(responses).hasSize(before + 1);
        assertThat(stream.last().getResourcesList()).extracting(any -> any.unpack(ClusterLoadAssignment.class))
            .as("what Envoy rejected again, as SotW sends the whole set, and b, newly asked for")
            .containsExactlyInAnyOrder(cla("a", "10.0.0.2"), cla("b", "10.0.0.3"));
    }
}
