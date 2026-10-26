package net.woaf.jono.envoy.xds.internal.delta;

import net.woaf.jono.envoy.xds.XdsConfig;
import com.google.common.collect.ImmutableSet;
import net.woaf.jono.envoy.xds.internal.NodeConfig;
import net.woaf.jono.envoy.xds.internal.delta.WildcardSubManager;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("Duplicates")
public class WildcardSubManagerSpec {

    @Test
    public void testSubscribeEmptyFirstMessageIsWildcard() {
        WildcardSubManager subManager = new WildcardSubManager(new NodeConfig<>(XdsConfig.of(new Object()), false));

        Predicate<String> wouldSendUpdates = subManager.processResourceListChange(ImmutableSet.of(), ImmutableSet.of()).orElseThrow();

        assertThat(wouldSendUpdates).accepts("foo", "bar");
        assertThat(subManager.isSubscribedTo("foo")).isTrue();
        assertThat(subManager.isSubscribedTo("bar")).isTrue();
    }

    @Test
    public void testSubscribeStarIsWildcard() {
        WildcardSubManager subManager = new WildcardSubManager(new NodeConfig<>(XdsConfig.of(new Object()), false));

        Predicate<String> wouldSendUpdates = subManager.processResourceListChange(ImmutableSet.of("*"), ImmutableSet.of()).orElseThrow();

        assertThat(wouldSendUpdates).accepts("foo", "bar").rejects("*"); // The wildcard itself is not a real resource name
        assertThat(subManager.isSubscribedTo("foo")).isTrue();
        assertThat(subManager.isSubscribedTo("bar")).isTrue();
        assertThat(subManager.isSubscribedTo("*")).isFalse();
    }

    @Test
    public void testBugClientSendsResourceListOnReconnectToWildcard() {
        // Prior to 1.19 a reconnecting envoy would send a resource list on connection to a wildcard DS if it already had state for that DS (e.g. if it was reconnecting to the control plane.)
        // While the spec says this should be treated as a normal SubList style subscribe, Envoy actually wanted a wildcard subscription, so we have a bug flag to allow this.
        WildcardSubManager subManager = new WildcardSubManager(new NodeConfig<>(XdsConfig.of(new Object()), true));

        Predicate<String> wouldSendUpdates = subManager.processResourceListChange(ImmutableSet.of("foo", "bar"), ImmutableSet.of()).orElseThrow();
        assertThat(wouldSendUpdates).accepts("foo", "bar", "baz");

        // Nothing changes, so no filter returned.
        assertThat(subManager.processResourceListChange(ImmutableSet.of(), ImmutableSet.of("foo"))).isNotPresent();

        assertThat(wouldSendUpdates).accepts("foo", "bar", "baz");
    }

    @Test
    public void testSubUnsubSubWildcard() {
        WildcardSubManager subManager = new WildcardSubManager(new NodeConfig<>(XdsConfig.of(new Object()), false));

        Predicate<String> wouldSendUpdates = subManager.processResourceListChange(ImmutableSet.of("*", "foo"), ImmutableSet.of()).orElseThrow();
        assertThat(wouldSendUpdates).accepts("foo", "bar", "baz", "cat");
        assertThat(subManager.isSubscribedTo("foo")).isTrue();
        assertThat(subManager.isSubscribedTo("bar")).isTrue();
        assertThat(subManager.isSubscribedTo("baz")).isTrue();
        assertThat(subManager.isSubscribedTo("cat")).isTrue();

        // An explicit subscribe while in wildcard mode is answered for just that name.
        wouldSendUpdates = subManager.processResourceListChange(ImmutableSet.of("bar"), ImmutableSet.of()).orElseThrow();
        assertThat(wouldSendUpdates).accepts("bar").rejects("foo", "baz", "cat");
        assertThat(subManager.isSubscribedTo("foo")).isTrue();
        assertThat(subManager.isSubscribedTo("bar")).isTrue();
        assertThat(subManager.isSubscribedTo("baz")).isTrue();
        assertThat(subManager.isSubscribedTo("baz")).isTrue();
        assertThat(subManager.isSubscribedTo("cat")).isTrue();

        wouldSendUpdates = subManager.processResourceListChange(ImmutableSet.of("baz"), ImmutableSet.of("*")).orElseThrow();
        assertThat(wouldSendUpdates).accepts("baz").rejects("foo", "bar", "cat"); // Only baz actually got changed by this update, it was already subscribed to foo and bar.
        assertThat(subManager.isSubscribedTo("foo")).isTrue();
        assertThat(subManager.isSubscribedTo("bar")).isTrue();
        assertThat(subManager.isSubscribedTo("baz")).isTrue();
        assertThat(subManager.isSubscribedTo("cat")).isFalse();

        wouldSendUpdates = subManager.processResourceListChange(ImmutableSet.of("*"), ImmutableSet.of()).orElseThrow();
        assertThat(wouldSendUpdates).accepts("cat").rejects("foo", "bar", "baz"); // We were already subscribed to foo, bar, baz, so it's not a change for them.
        assertThat(subManager.isSubscribedTo("foo")).isTrue();
        assertThat(subManager.isSubscribedTo("bar")).isTrue();
        assertThat(subManager.isSubscribedTo("baz")).isTrue();
        assertThat(subManager.isSubscribedTo("cat")).isTrue();
    }

}
