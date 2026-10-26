package net.woaf.jono.envoy.xds;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TypeUrlTest {

    /** The types a type's resources can name. */
    private static final Map<TypeUrl, Set<TypeUrl>> REFERENCES = Map.of(
        TypeUrl.LDS, Set.of(TypeUrl.RDS, TypeUrl.SRDS, TypeUrl.SDS),
        TypeUrl.SRDS, Set.of(TypeUrl.RDS),
        TypeUrl.RDS, Set.of(TypeUrl.CDS, TypeUrl.VHDS),
        TypeUrl.VHDS, Set.of(TypeUrl.CDS),
        TypeUrl.CDS, Set.of(TypeUrl.EDS, TypeUrl.SDS));

    @Test
    void removalsGoBeforeWhatTheyName() {
        REFERENCES.forEach((from, named) -> named.forEach(to ->
            assertThat(TypeUrl.REMOVE_ORDER.indexOf(from)).as("%s removed before %s", from, to).isLessThan(TypeUrl.REMOVE_ORDER.indexOf(to))));
    }

    @Test
    void wildcardTypesAreTheOnesEnvoySubscribesToWithAWildcard() {
        // The xDS docs name only LDS and CDS, but Envoy's SRDS subscription also starts with no names:
        // source/common/router/scoped_rds.h, subscription_->start({}).
        assertThat(Arrays.stream(TypeUrl.values()).filter(TypeUrl::isWildcard))
            .containsExactlyInAnyOrder(TypeUrl.LDS, TypeUrl.CDS, TypeUrl.SRDS);
    }
}
