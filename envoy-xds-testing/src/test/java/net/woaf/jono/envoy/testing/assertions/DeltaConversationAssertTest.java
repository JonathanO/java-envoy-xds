package net.woaf.jono.envoy.testing.assertions;

import static net.woaf.jono.envoy.testing.assertions.TestResources.cluster;
import static net.woaf.jono.envoy.testing.assertions.TestResources.endpoints;
import static net.woaf.jono.envoy.testing.assertions.TestResources.listenerUsing;
import static net.woaf.jono.envoy.testing.assertions.TestResources.routeTo;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.rpc.Status;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import java.util.List;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.junit.jupiter.api.Test;

public class DeltaConversationAssertTest {

    private int nonce = 0;

    private static DeltaDiscoveryRequest subscribe(String typeUrl, String... names) {
        return DeltaDiscoveryRequest.newBuilder().setTypeUrl(typeUrl).addAllResourceNamesSubscribe(List.of(names)).build();
    }

    private static DeltaDiscoveryRequest unsubscribe(String typeUrl, String... names) {
        return DeltaDiscoveryRequest.newBuilder().setTypeUrl(typeUrl).addAllResourceNamesUnsubscribe(List.of(names)).build();
    }

    private static DeltaDiscoveryRequest ack(String typeUrl, String responseNonce) {
        return DeltaDiscoveryRequest.newBuilder().setTypeUrl(typeUrl).setResponseNonce(responseNonce).build();
    }

    private static DeltaDiscoveryRequest nack(String typeUrl, String responseNonce) {
        return ack(typeUrl, responseNonce).toBuilder().setErrorDetail(Status.newBuilder().setMessage("bad cluster v2")).build();
    }

    private String send(Conversation c, String typeUrl, List<String> removed, Message... resources) {
        String n = Integer.toString(++nonce);
        DeltaDiscoveryResponse.Builder r = DeltaDiscoveryResponse.newBuilder().setTypeUrl(typeUrl).setNonce(n).addAllRemovedResources(removed);
        for (Message m : resources) {
            r.addResources(Resource.newBuilder().setName(XdsTypes.nameOf(m)).setResource(Any.pack(m)));
        }
        c.onResponse(r.build());
        return n;
    }

    private String send(Conversation c, String typeUrl, Message... resources) {
        return send(c, typeUrl, List.of(), resources);
    }

    private void exchange(Conversation c, String typeUrl, Message... resources) {
        c.onRequest(ack(typeUrl, send(c, typeUrl, resources)));
    }

    /** Envoy synced with a listener using routes to cluster v1. Nonces 1 to 4. */
    private Conversation synced() {
        Conversation c = new Conversation();
        c.onRequest(subscribe(XdsTypes.CDS));
        exchange(c, XdsTypes.CDS, cluster("v1"));
        c.onRequest(subscribe(XdsTypes.EDS, "v1"));
        exchange(c, XdsTypes.EDS, endpoints("v1"));
        c.onRequest(subscribe(XdsTypes.LDS));
        exchange(c, XdsTypes.LDS, listenerUsing("routes"));
        c.onRequest(subscribe(XdsTypes.RDS, "routes"));
        exchange(c, XdsTypes.RDS, routeTo("v1"));
        return c;
    }

    /** Then moved to cluster v2, making it before breaking v1. Nonces 5 to 8. */
    private void switchToV2(Conversation c) {
        exchange(c, XdsTypes.CDS, cluster("v2"));
        c.onRequest(subscribe(XdsTypes.EDS, "v2"));
        exchange(c, XdsTypes.EDS, endpoints("v2"));
        exchange(c, XdsTypes.RDS, routeTo("v2"));
        c.onRequest(ack(XdsTypes.CDS, send(c, XdsTypes.CDS, List.of("v1"))));
        c.onRequest(unsubscribe(XdsTypes.EDS, "v1"));
    }

    /** Envoy has clusters, and nothing else yet. */
    private Conversation clustersOnly() {
        Conversation c = new Conversation();
        c.onRequest(subscribe(XdsTypes.CDS));
        exchange(c, XdsTypes.CDS, cluster("v1"));
        return c;
    }

    private static DeltaDiscoveryResponse response(String typeUrl, String nonce, Message... resources) {
        DeltaDiscoveryResponse.Builder r = DeltaDiscoveryResponse.newBuilder().setTypeUrl(typeUrl).setNonce(nonce);
        for (Message m : resources) {
            r.addResources(Resource.newBuilder().setName(XdsTypes.nameOf(m)).setResource(Any.pack(m)));
        }
        return r.build();
    }

    @Test
    public void doesNotTakeAnAckOfAnotherTypesNonce() {
        // Envoy keeps nonces per type, so EDS's ACK of its nonce 2 isn't CDS's.
        Conversation c = new Conversation();
        c.onRequest(subscribe(XdsTypes.CDS));
        c.onResponse(response(XdsTypes.CDS, "1", cluster("a")));
        c.onRequest(ack(XdsTypes.CDS, "1"));
        c.onRequest(subscribe(XdsTypes.EDS, "a"));
        c.onResponse(response(XdsTypes.EDS, "1", endpoints("a")));
        c.onRequest(ack(XdsTypes.EDS, "1"));
        c.onRequest(subscribe(XdsTypes.RDS, "routes"));
        c.onResponse(response(XdsTypes.EDS, "2", endpoints("a")));
        c.onResponse(response(XdsTypes.CDS, "2", Cluster.newBuilder().setName("b").setType(Cluster.DiscoveryType.STATIC).build()));
        c.onRequest(ack(XdsTypes.EDS, "2"));
        c.onResponse(response(XdsTypes.RDS, "1", routeTo("b")));
        c.onRequest(ack(XdsTypes.CDS, "2"));
        c.onRequest(ack(XdsTypes.RDS, "1"));
        assertThatCode(() -> DeltaConversationAssert.assertThat(c).allWereAcked().nothingSentUnasked()).doesNotThrowAnyException();
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).wasMakeBeforeBreak(0))
            .hasMessageContaining("Route <routes> sent using cluster <b> before Envoy ACKed the cluster");
    }

    @Test
    public void passesACleanSwitchBetweenClusters() {
        Conversation c = synced();
        switchToV2(c);
        assertThatCode(() -> DeltaConversationAssert.assertThat(c).allWereAcked().nothingSentUnasked().sentNoEmptyResponses(0)
            .wasMakeBeforeBreak(0).unsubscribedFrom(XdsTypes.EDS, "v1").answeredEveryType()
            .hadConversationsWith(XdsTypes.CDS, XdsTypes.EDS, XdsTypes.LDS, XdsTypes.RDS)).doesNotThrowAnyException();
    }

    @Test
    public void catchesAResponseEnvoyNeverAcked() {
        Conversation c = synced();
        send(c, XdsTypes.CDS, cluster("v2"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).allWereAcked()).hasMessageContaining("Found unacked nonces <{" + XdsTypes.CDS + "=[5]}>");
    }

    @Test
    public void catchesANack() {
        Conversation c = synced();
        c.onRequest(nack(XdsTypes.CDS, send(c, XdsTypes.CDS, cluster("v2"))));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).allWereAcked()).hasMessageContaining("Envoy sent error <bad cluster v2>");
    }

    @Test
    public void catchesANonceSentTwice() {
        Conversation c = synced();
        DeltaDiscoveryResponse removal = DeltaDiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.CDS).setNonce("5").addRemovedResources("v1").build();
        c.onResponse(removal);
        c.onResponse(removal); // resent before Envoy ACKed it
        c.onRequest(ack(XdsTypes.CDS, "5"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).allWereAcked()).hasMessageContaining("Sent nonce <5> for <" + XdsTypes.CDS + "> multiple times");
    }

    @Test
    public void catchesANonceReusedAfterItsAck() {
        Conversation c = clustersOnly(); // nonce 1, ACKed
        c.onResponse(DeltaDiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.CDS).setNonce("1").addRemovedResources("v1").build());
        c.onRequest(ack(XdsTypes.CDS, "1"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).allWereAcked()).hasMessageContaining("Sent nonce <1> for <" + XdsTypes.CDS + "> multiple times");
    }

    @Test
    public void catchesATypeEnvoyNeverAskedFor() {
        Conversation c = clustersOnly();
        send(c, XdsTypes.EDS, endpoints("v1")); // before Envoy asked for them
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).nothingSentUnasked())
            .hasMessageContaining("Sent message for type <" + XdsTypes.EDS + "> which envoy has never asked for");
    }

    @Test
    public void catchesAResourceEnvoyNeverAskedFor() {
        Conversation c = synced();
        send(c, XdsTypes.EDS, endpoints("v2"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).nothingSentUnasked()).hasMessageContaining("containing resource(s) <[v2]>");
    }

    @Test
    public void catchesAResourceEnvoyUnsubscribedFrom() {
        Conversation c = synced();
        switchToV2(c);
        send(c, XdsTypes.EDS, endpoints("v1"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).nothingSentUnasked()).hasMessageContaining("containing resource(s) <[v1]>");
    }

    @Test
    public void pre119ReconnectAllowanceOnlyCoversWildcardTypes() {
        Conversation c = new Conversation();
        c.onRequest(subscribe(XdsTypes.CDS, "v1")); // Envoy before 1.19 reconnecting lists the clusters it had
        exchange(c, XdsTypes.CDS, cluster("v2"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).nothingSentUnasked()).hasMessageContaining("containing resource(s) <[v2]>");
        assertThatCode(() -> DeltaConversationAssert.assertThat(c).nothingSentUnaskedPre119Reconnect()).doesNotThrowAnyException();

        c.onRequest(subscribe(XdsTypes.EDS, "v1"));
        send(c, XdsTypes.EDS, endpoints("v2"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).nothingSentUnaskedPre119Reconnect())
            .hasMessageContaining("containing resource(s) <[v2]>");
    }

    @Test
    public void catchesAResourceEnvoyNeverUnsubscribedFrom() {
        Conversation c = synced();
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).unsubscribedFrom(XdsTypes.EDS, "v1"))
            .hasMessageContaining("Envoy never unsubscribed type <" + XdsTypes.EDS + "> from resource <v1>");
    }

    @Test
    public void catchesAnEmptyUpdate() {
        Conversation c = synced();
        exchange(c, XdsTypes.EDS);
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).sentNoEmptyResponses(0))
            .hasMessageContaining("Sent empty response <5> for type <" + XdsTypes.EDS + ">");
    }

    @Test
    public void catchesARouteSentBeforeEnvoyAckedItsCluster() {
        Conversation c = synced();
        send(c, XdsTypes.CDS, cluster("v2"));
        send(c, XdsTypes.RDS, routeTo("v2"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).wasMakeBeforeBreak(0))
            .hasMessageContaining("Route <routes> sent using cluster <v2> before Envoy ACKed the cluster");
    }

    @Test
    public void catchesARouteSentBeforeEnvoyAckedItsEndpoints() {
        Conversation c = synced();
        exchange(c, XdsTypes.CDS, cluster("v2"));
        c.onRequest(subscribe(XdsTypes.EDS, "v2"));
        send(c, XdsTypes.RDS, routeTo("v2"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).wasMakeBeforeBreak(0))
            .hasMessageContaining("Route <routes> sent using cluster <v2> before Envoy ACKed its endpoints");
    }

    @Test
    public void findsEndpointsUnderTheClustersEdsServiceName() {
        Conversation c = new Conversation();
        c.onRequest(subscribe(XdsTypes.CDS));
        exchange(c, XdsTypes.CDS, cluster("v1", "v1-endpoints"));
        c.onRequest(subscribe(XdsTypes.EDS, "v1-endpoints"));
        exchange(c, XdsTypes.EDS, endpoints("v1-endpoints"));
        c.onRequest(subscribe(XdsTypes.RDS, "routes"));
        exchange(c, XdsTypes.RDS, routeTo("v1"));
        assertThatCode(() -> DeltaConversationAssert.assertThat(c).wasMakeBeforeBreak(0)).doesNotThrowAnyException();
    }

    @Test
    public void catchesARouteToAClusterEnvoyRejected() {
        Conversation c = synced();
        c.onRequest(nack(XdsTypes.CDS, send(c, XdsTypes.CDS, cluster("v2"))));
        send(c, XdsTypes.RDS, routeTo("v2"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).wasMakeBeforeBreak(0))
            .hasMessageContaining("Route <routes> sent using cluster <v2> before Envoy ACKed the cluster");
    }

    @Test
    public void catchesARouteConfigRemovedWhileAListenerUsesIt() {
        Conversation c = synced();
        send(c, XdsTypes.RDS, List.of("routes"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).wasMakeBeforeBreak(0))
            .hasMessageContaining("Route config <routes> removed while Envoy's ACKed listener <listener> still uses it");
    }

    @Test
    public void catchesATypeLeftUnanswered() {
        Conversation c = clustersOnly();
        c.onRequest(subscribe(XdsTypes.LDS));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).answeredEveryType())
            .hasMessageContaining("Envoy got no response at all for <[" + XdsTypes.LDS + "]>");
    }

    @Test
    public void catchesATypeEnvoyNeverRequested() {
        Conversation c = clustersOnly();
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(c).hadConversationsWith(XdsTypes.CDS, XdsTypes.LDS))
            .hasMessageContaining("Did not have a conversation with <[" + XdsTypes.LDS + "]>");
    }
}
