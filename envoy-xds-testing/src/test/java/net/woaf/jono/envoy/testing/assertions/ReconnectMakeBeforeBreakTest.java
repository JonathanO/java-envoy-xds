package net.woaf.jono.envoy.testing.assertions;

import static net.woaf.jono.envoy.testing.assertions.TestResources.cluster;
import static net.woaf.jono.envoy.testing.assertions.TestResources.endpoints;
import static net.woaf.jono.envoy.testing.assertions.TestResources.routeTo;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import java.util.List;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.junit.jupiter.api.Test;

public class ReconnectMakeBeforeBreakTest {

    private int nonce = 0;

    private void exchange(Conversation c, String typeUrl, List<String> removed, Message... resources) {
        String n = Integer.toString(++nonce);
        DeltaDiscoveryResponse.Builder r = DeltaDiscoveryResponse.newBuilder().setTypeUrl(typeUrl).setNonce(n).addAllRemovedResources(removed);
        for (Message m : resources) {
            r.addResources(Resource.newBuilder().setName(XdsTypes.nameOf(m)).setResource(Any.pack(m)));
        }
        c.onResponse(r.build());
        c.onRequest(DeltaDiscoveryRequest.newBuilder().setTypeUrl(typeUrl).setResponseNonce(n).build());
    }

    /** Envoy left with a route to v1 on the first stream. */
    private Conversation before() {
        Conversation before = new Conversation();
        exchange(before, XdsTypes.CDS, List.of(), cluster("v1"));
        exchange(before, XdsTypes.EDS, List.of(), endpoints("v1"));
        exchange(before, XdsTypes.RDS, List.of(), routeTo("v1"));
        return before;
    }

    @Test
    public void startsFromWhatEnvoyAckedBeforeTheReconnect() {
        Conversation after = new Conversation();
        exchange(after, XdsTypes.CDS, List.of(), cluster("v1"), cluster("v2"));
        exchange(after, XdsTypes.EDS, List.of(), endpoints("v1"), endpoints("v2"));
        exchange(after, XdsTypes.RDS, List.of(), routeTo("v2"));
        exchange(after, XdsTypes.CDS, List.of("v1"));
        assertThatCode(() -> DeltaConversationAssert.assertThat(after).wasMakeBeforeBreak(0, before())).doesNotThrowAnyException();
    }

    @Test
    public void catchesARemovalOfAClusterEnvoysOldRouteStillUses() {
        Conversation after = new Conversation();
        exchange(after, XdsTypes.CDS, List.of("v1"), cluster("v2"));
        assertThatThrownBy(() -> DeltaConversationAssert.assertThat(after).wasMakeBeforeBreak(0, before()))
            .hasMessageContaining("Cluster <v1> removed while Envoy's ACKed route <routes> still uses it");
        assertThatCode(() -> DeltaConversationAssert.assertThat(after).wasMakeBeforeBreak(0)).as("without the conversation before, nothing to compare with")
            .doesNotThrowAnyException();
    }
}
