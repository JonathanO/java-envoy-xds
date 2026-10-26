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
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import java.util.List;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.junit.jupiter.api.Test;

public class SotwConversationAssertTest {

    private int nonce = 0;

    private static DiscoveryRequest request(String typeUrl, String responseNonce, String... names) {
        return DiscoveryRequest.newBuilder().setTypeUrl(typeUrl).setResponseNonce(responseNonce).addAllResourceNames(List.of(names)).build();
    }

    private static DiscoveryRequest nack(String typeUrl, String responseNonce) {
        return request(typeUrl, responseNonce).toBuilder().setErrorDetail(Status.newBuilder().setMessage("bad cluster v2")).build();
    }

    private String send(Conversation c, String typeUrl, Message... resources) {
        String n = Integer.toString(++nonce);
        DiscoveryResponse.Builder r = DiscoveryResponse.newBuilder().setTypeUrl(typeUrl).setNonce(n);
        for (Message m : resources) {
            r.addResources(Any.pack(m));
        }
        c.onResponse(r.build());
        return n;
    }

    // Envoy's ACK re-lists what it subscribes to: for a non-wildcard type, what was sent.
    private void exchange(Conversation c, String typeUrl, Message... resources) {
        String n = send(c, typeUrl, resources);
        String[] names = XdsTypes.isWildcard(typeUrl) ? new String[0] : List.of(resources).stream().map(XdsTypes::nameOf).toArray(String[]::new);
        c.onRequest(request(typeUrl, n, names));
    }

    /** Envoy synced with a listener using routes to cluster v1. Nonces 1 to 4. */
    private Conversation synced() {
        Conversation c = new Conversation();
        c.onRequest(request(XdsTypes.CDS, ""));
        exchange(c, XdsTypes.CDS, cluster("v1"));
        c.onRequest(request(XdsTypes.EDS, "", "v1"));
        exchange(c, XdsTypes.EDS, endpoints("v1"));
        c.onRequest(request(XdsTypes.LDS, ""));
        exchange(c, XdsTypes.LDS, listenerUsing("routes"));
        c.onRequest(request(XdsTypes.RDS, "", "routes"));
        exchange(c, XdsTypes.RDS, routeTo("v1"));
        return c;
    }

    /** Then moved to cluster v2, making it before breaking v1. Nonces 5 to 8. */
    private void switchToV2(Conversation c) {
        exchange(c, XdsTypes.CDS, cluster("v1"), cluster("v2"));
        c.onRequest(request(XdsTypes.EDS, "2", "v1", "v2"));
        exchange(c, XdsTypes.EDS, endpoints("v1"), endpoints("v2"));
        exchange(c, XdsTypes.RDS, routeTo("v2"));
        exchange(c, XdsTypes.CDS, cluster("v2"));
        c.onRequest(request(XdsTypes.EDS, "6", "v2"));
    }

    /** Envoy has clusters, and nothing else yet. */
    private Conversation clustersOnly() {
        Conversation c = new Conversation();
        c.onRequest(request(XdsTypes.CDS, ""));
        exchange(c, XdsTypes.CDS, cluster("v1"));
        return c;
    }

    @Test
    public void passesACleanSwitchBetweenClusters() {
        Conversation c = synced();
        switchToV2(c);
        assertThatCode(() -> SotwConversationAssert.assertThat(c).allWereAcked().nothingSentUnasked().sentNoEmptyResponses(0)
            .wasMakeBeforeBreak(0).unsubscribedFrom(XdsTypes.EDS, "v1").answeredEveryType()
            .hadConversationsWith(XdsTypes.CDS, XdsTypes.EDS, XdsTypes.LDS, XdsTypes.RDS)).doesNotThrowAnyException();
    }

    @Test
    public void catchesANonceEchoedFromThePreviousStream() {
        Conversation conversation = new Conversation();
        conversation.onRequest(DiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.CDS).setResponseNonce("1").build()); // Envoy reconnecting
        conversation.onResponse(DiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.CDS).setNonce("1").build());
        conversation.onRequest(DiscoveryRequest.newBuilder().setTypeUrl(XdsTypes.CDS).setResponseNonce("1").build());

        assertThatThrownBy(() -> SotwConversationAssert.assertThat(conversation).allWereAcked())
            .hasMessageContaining("which Envoy had echoed from its previous stream");
    }

    @Test
    public void catchesAResponseEnvoyNeverAcked() {
        Conversation c = synced();
        send(c, XdsTypes.CDS, cluster("v1"), cluster("v2"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).allWereAcked())
            .hasMessageContaining("Found unacked nonces <{" + XdsTypes.CDS + "=[5]}>");
    }

    @Test
    public void catchesANack() {
        Conversation c = synced();
        c.onRequest(nack(XdsTypes.CDS, send(c, XdsTypes.CDS, cluster("v1"), cluster("v2"))));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).allWereAcked()).hasMessageContaining("Envoy sent error <bad cluster v2>");
    }

    @Test
    public void catchesANonceSentTwice() {
        Conversation c = synced();
        c.onResponse(DiscoveryResponse.newBuilder().setTypeUrl(XdsTypes.CDS).setNonce("1").addResources(Any.pack(cluster("v2"))).build());
        c.onRequest(request(XdsTypes.CDS, "1"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).allWereAcked())
            .hasMessageContaining("Sent nonce <1> for <" + XdsTypes.CDS + "> multiple times");
    }

    @Test
    public void catchesATypeEnvoyNeverAskedFor() {
        Conversation c = clustersOnly();
        send(c, XdsTypes.EDS, endpoints("v1")); // before Envoy asked for them
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).nothingSentUnasked())
            .hasMessageContaining("Sent message for type <" + XdsTypes.EDS + "> which envoy has never asked for");
    }

    @Test
    public void catchesAResourceEnvoyNeverAskedFor() {
        Conversation c = synced();
        send(c, XdsTypes.EDS, endpoints("v1"), endpoints("v2"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).nothingSentUnasked()).hasMessageContaining("containing resource(s) <[v2]>");
    }

    @Test
    public void catchesAResourceEnvoyStoppedAskingFor() {
        Conversation c = synced();
        switchToV2(c);
        send(c, XdsTypes.EDS, endpoints("v1"), endpoints("v2"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).nothingSentUnasked()).hasMessageContaining("containing resource(s) <[v1]>");
    }

    @Test
    public void pre119ReconnectAllowanceOnlyCoversWildcardTypes() {
        Conversation c = new Conversation();
        c.onRequest(request(XdsTypes.CDS, "", "v1")); // Envoy before 1.19 reconnecting lists the clusters it had
        exchange(c, XdsTypes.CDS, cluster("v1"), cluster("v2"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).nothingSentUnasked()).hasMessageContaining("containing resource(s) <[v2]>");
        assertThatCode(() -> SotwConversationAssert.assertThat(c).nothingSentUnaskedPre119Reconnect()).doesNotThrowAnyException();

        c.onRequest(request(XdsTypes.EDS, "", "v1"));
        send(c, XdsTypes.EDS, endpoints("v1"), endpoints("v2"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).nothingSentUnaskedPre119Reconnect())
            .hasMessageContaining("containing resource(s) <[v2]>");
    }

    @Test
    public void catchesAResourceEnvoyNeverDropped() {
        Conversation c = synced();
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).unsubscribedFrom(XdsTypes.EDS, "v1"))
            .hasMessageContaining("Envoy never unsubscribed type <" + XdsTypes.EDS + "> from resource <v1>");
    }

    @Test
    public void catchesAnEmptyUpdate() {
        Conversation c = synced();
        c.onRequest(request(XdsTypes.RDS, send(c, XdsTypes.RDS), "routes"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).sentNoEmptyResponses(0))
            .hasMessageContaining("Sent empty response <5> for type <" + XdsTypes.RDS + ">");
    }

    @Test
    public void catchesARouteSentBeforeEnvoyAckedItsCluster() {
        Conversation c = synced();
        send(c, XdsTypes.CDS, cluster("v1"), cluster("v2"));
        send(c, XdsTypes.RDS, routeTo("v2"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).wasMakeBeforeBreak(0))
            .hasMessageContaining("Route <routes> sent using cluster <v2> before Envoy ACKed the cluster");
    }

    @Test
    public void catchesARouteSentBeforeEnvoyAckedItsEndpoints() {
        Conversation c = synced();
        exchange(c, XdsTypes.CDS, cluster("v1"), cluster("v2"));
        c.onRequest(request(XdsTypes.EDS, "2", "v1", "v2"));
        send(c, XdsTypes.RDS, routeTo("v2"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).wasMakeBeforeBreak(0))
            .hasMessageContaining("Route <routes> sent using cluster <v2> before Envoy ACKed its endpoints");
    }

    @Test
    public void findsEndpointsUnderTheClustersEdsServiceName() {
        Conversation c = new Conversation();
        c.onRequest(request(XdsTypes.CDS, ""));
        exchange(c, XdsTypes.CDS, cluster("v1", "v1-endpoints"));
        c.onRequest(request(XdsTypes.EDS, "", "v1-endpoints"));
        exchange(c, XdsTypes.EDS, endpoints("v1-endpoints"));
        c.onRequest(request(XdsTypes.RDS, "", "routes"));
        exchange(c, XdsTypes.RDS, routeTo("v1"));
        assertThatCode(() -> SotwConversationAssert.assertThat(c).wasMakeBeforeBreak(0)).doesNotThrowAnyException();
    }

    @Test
    public void catchesARouteToAClusterEnvoyRejected() {
        Conversation c = synced();
        c.onRequest(nack(XdsTypes.CDS, send(c, XdsTypes.CDS, cluster("v1"), cluster("v2"))));
        send(c, XdsTypes.RDS, routeTo("v2"));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).wasMakeBeforeBreak(0))
            .hasMessageContaining("Route <routes> sent using cluster <v2> before Envoy ACKed the cluster");
    }

    @Test
    public void catchesAClusterLeftOutWhileARouteUsesIt() {
        Conversation c = synced();
        send(c, XdsTypes.CDS, cluster("v2")); // the full set, so v1 is removed
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).wasMakeBeforeBreak(0))
            .hasMessageContaining("Cluster <v1> removed while Envoy's ACKed route <routes> still uses it");
    }

    @Test
    public void catchesATypeLeftUnanswered() {
        Conversation c = clustersOnly();
        c.onRequest(request(XdsTypes.LDS, ""));
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).answeredEveryType())
            .hasMessageContaining("Envoy got no response at all for <[" + XdsTypes.LDS + "]>");
    }

    @Test
    public void catchesATypeEnvoyNeverRequested() {
        Conversation c = clustersOnly();
        assertThatThrownBy(() -> SotwConversationAssert.assertThat(c).hadConversationsWith(XdsTypes.CDS, XdsTypes.LDS))
            .hasMessageContaining("Did not have a conversation with <[" + XdsTypes.LDS + "]>");
    }
}
