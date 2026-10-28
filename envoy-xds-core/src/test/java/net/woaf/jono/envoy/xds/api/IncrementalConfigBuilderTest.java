package net.woaf.jono.envoy.xds.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.envoyproxy.envoy.config.listener.v3.Listener;
import java.util.List;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.NamedMessage;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.Resources;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder.Response;
import org.junit.jupiter.api.Test;

public class IncrementalConfigBuilderTest {

    private static final Listener LISTENER = Listener.newBuilder().setName("ingress").build();

    @Test
    public void listenerIsNamedByItsName() {
        assertThat(NamedMessage.of(LISTENER)).isEqualTo(new NamedMessage<>("ingress", LISTENER));
    }

    @Test
    public void responseIsNoopOnlyWhenItChangesNothing() {
        assertThat(Response.<Listener>empty().isNoop()).isTrue();
        assertThat(new Response<>(List.of(NamedMessage.of(LISTENER)), List.of()).isNoop()).as("adds").isFalse();
        assertThat(new Response<Listener>(List.of(), List.of("ingress")).isNoop()).as("removes").isFalse();
    }

    @Test
    public void emptyResourcesHaveNone() {
        assertThat(Resources.<Listener>empty().resources()).isEmpty();
    }
}
