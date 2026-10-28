package net.woaf.jono.envoy.xds;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

public class XdsConfigTest {

    @Test
    public void withersSetTheirOwnSetting() {
        Object details = new Object();

        XdsConfig<Object> config = XdsConfig.of(details).withDelayUpdatesUntilAckOf(TypeUrl.RDS).withSubscribesTo(Set.of(TypeUrl.CDS, TypeUrl.RDS));

        assertThat(config).isEqualTo(new XdsConfig<>(details, TypeUrl.RDS, Set.of(TypeUrl.CDS, TypeUrl.RDS)));
    }
}
