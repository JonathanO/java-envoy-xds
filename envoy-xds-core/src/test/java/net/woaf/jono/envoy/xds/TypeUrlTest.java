package net.woaf.jono.envoy.xds;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class TypeUrlTest {

    @Test
    void wildcardTypesAreTheOnesEnvoySubscribesToWithAWildcard() {
        // The xDS docs name only LDS and CDS, but Envoy's SRDS subscription also starts with no names:
        // source/common/router/scoped_rds.h, subscription_->start({}).
        assertThat(Arrays.stream(TypeUrl.values()).filter(TypeUrl::isWildcard))
            .containsExactlyInAnyOrder(TypeUrl.LDS, TypeUrl.CDS, TypeUrl.SRDS);
    }
}
