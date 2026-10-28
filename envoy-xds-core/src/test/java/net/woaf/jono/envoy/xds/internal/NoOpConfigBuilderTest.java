package net.woaf.jono.envoy.xds.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import org.junit.jupiter.api.Test;

public class NoOpConfigBuilderTest {

    private final NoOpConfigBuilder<RouteConfiguration, Object, Object> builder = new NoOpConfigBuilder<>(RouteConfiguration.class);

    @Test
    public void neverHasAnyResources() {
        Object state = new Object();
        assertThat(builder.addOrder(state, name -> true, state).isNoop()).isTrue();
        assertThat(builder.removeOrder(state, name -> true, state).isNoop()).isTrue();
        assertThat(builder.getResourcesAddOrder(state, name -> true, state).resources()).isEmpty();
        assertThat(builder.getResourcesRemoveOrder(state, name -> true, state).resources()).isEmpty();
    }

    @Test
    public void handlesTheTypeItStandsInFor() {
        assertThat(builder.handlesType()).isEqualTo(RouteConfiguration.class);
    }
}
