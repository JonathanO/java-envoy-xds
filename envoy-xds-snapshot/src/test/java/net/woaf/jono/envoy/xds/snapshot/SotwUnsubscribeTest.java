package net.woaf.jono.envoy.xds.snapshot;

import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import net.woaf.jono.envoy.xds.TypeUrl;
import org.junit.jupiter.api.Test;

import static net.woaf.jono.envoy.xds.snapshot.TestResources.all;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cla;
import static net.woaf.jono.envoy.xds.snapshot.TestResources.cluster;
import static org.assertj.core.api.Assertions.assertThat;

public class SotwUnsubscribeTest {

    private final SotwStream stream = new SotwStream(Cluster.class, ClusterLoadAssignment.class);

    @Test
    public void unsubscribingIsNotAnswered() {
        stream.source.update(all(e -> e.put(cluster("a")).put(cluster("b")).put(cla("a", "10.0.0.1")).put(cla("b", "10.0.0.2"))));
        stream.ack(stream.request(TypeUrl.CDS, null));
        DiscoveryResponse eds = stream.request(TypeUrl.EDS, null, "a", "b");

        // Envoy drops b's endpoints, as when it removes cluster b, in the ACK.
        int before = stream.responses.size();
        stream.ack(eds, "a");
        assertThat(stream.responses).as("Envoy doesn't wait for an answer to an unsubscribe").hasSize(before);

        stream.source.update(all(e -> e.put(cla("a", "10.0.0.3"))));
        stream.runTasks();
        assertThat(stream.last().getResourcesList()).extracting(any -> any.unpack(ClusterLoadAssignment.class))
            .as("b is gone from the set").containsExactly(cla("a", "10.0.0.3"));
    }

    @Test
    public void unsubscribingDuringAChangeStillGetsTheChangeInTurn() {
        stream.source.update(all(e -> e.put(cluster("a")).put(cluster("b")).put(cla("a", "10.0.0.1")).put(cla("b", "10.0.0.2"))));
        stream.ack(stream.request(TypeUrl.CDS, null));
        DiscoveryResponse eds = stream.request(TypeUrl.EDS, null, "a", "b");
        stream.ack(eds, "a", "b");

        // Endpoints wait for the new cluster to be ACKed. Envoy unsubscribes from b's meanwhile.
        stream.source.update(all(e -> e.put(cluster("c")).put(cla("a", "10.0.0.3"))));
        stream.runTasks();
        DiscoveryResponse cds = stream.last();
        assertThat(cds.getTypeUrl()).isEqualTo(TypeUrl.CDS.getTypeUrl());
        stream.ack(eds, "a");
        assertThat(stream.last()).as("no answer to the unsubscribe").isEqualTo(cds);

        stream.ack(cds);
        assertThat(stream.last().getResourcesList()).extracting(any -> any.unpack(ClusterLoadAssignment.class))
            .as("the change, once its clusters are ACKed, without b").containsExactly(cla("a", "10.0.0.3"));
    }
}
