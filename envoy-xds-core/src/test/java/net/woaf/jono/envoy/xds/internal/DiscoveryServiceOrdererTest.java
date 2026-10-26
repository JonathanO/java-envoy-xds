package net.woaf.jono.envoy.xds.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Message;
import java.util.Map;
import net.woaf.jono.envoy.xds.TypeUrl;
import org.junit.jupiter.api.Test;

class DiscoveryServiceOrdererTest {

    @Test
    @SuppressWarnings("unchecked")
    void skipsTypesWithNoService() {
        DiscoveryService<Message, Object> cds = mock(DiscoveryService.class);
        DiscoveryService<Message, Object> rds = mock(DiscoveryService.class);

        // SDS, the first type in add order, has no service; neither has EDS, between the two that do.
        assertThat(DiscoveryServiceOrderer.sort(TypeUrl.ADD_ORDER, Map.of(TypeUrl.CDS, cds, TypeUrl.RDS, rds))).containsExactly(cds, rds);
    }
}
