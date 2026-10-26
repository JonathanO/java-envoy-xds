package net.woaf.jono.envoy.xds.internal.sotw;

import com.google.common.collect.ImmutableMap;
import net.woaf.jono.envoy.xds.internal.sotw.WildcardSubManager;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("Duplicates")
public class WildcardSubManagerSpec {

    @Test
    public void testNotSubscribed() {
        WildcardSubManager subManager = new WildcardSubManager();

        Map<String, Object> subs = ImmutableMap.of(
                "foo", new Object(),
                "bar", new Object()
        );

        assertThat(subscribed(subManager, subs)).isEmpty();
    }

    @Test
    public void testSubscribeToWildcard() {
        WildcardSubManager subManager = new WildcardSubManager();
        assertThat(subManager.processResourceListChange(Collections.emptyList())).isPresent();
        assertThat(subManager.isSubscribed()).isTrue();

        Map<String, Object> subs = ImmutableMap.of(
                "foo", new Object(),
                "bar", new Object()
        );

        assertThat(subscribed(subManager, subs)).isEqualTo(subs.keySet());

        assertThat(subManager.processResourceListChange(Collections.emptyList())).isNotPresent();
        assertThat(subscribed(subManager, subs)).isEqualTo(subs.keySet());

    }

    @Test
    public void testSubscribeToStarIsWildcard() {
        WildcardSubManager subManager = new WildcardSubManager();
        assertThat(subManager.processResourceListChange(List.of("*"))).isPresent();
        assertThat(subManager.isSubscribedTo("foo")).isTrue();
    }

    @Test
    public void testNamesWithoutStarAreASublist() {
        WildcardSubManager subManager = new WildcardSubManager();
        assertThat(subManager.processResourceListChange(List.of("foo")).orElseThrow()).accepts("foo").rejects("bar");
        assertThat(subManager.isSubscribedTo("foo")).isTrue();
        assertThat(subManager.isSubscribedTo("bar")).isFalse();
    }

    @Test
    public void testExplicitNamesInWildcardModeAreAnswered() {
        WildcardSubManager subManager = new WildcardSubManager();
        assertThat(subManager.processResourceListChange(List.of("*"))).isPresent();
        assertThat(subManager.processResourceListChange(List.of("*", "foo")).orElseThrow()).accepts("foo");
        assertThat(subManager.processResourceListChange(List.of("*", "foo"))).isNotPresent();
    }

    @Test
    public void testLeavingWildcardKeepsExplicitNames() {
        WildcardSubManager subManager = new WildcardSubManager();
        assertThat(subManager.processResourceListChange(List.of("*", "foo"))).isPresent();
        assertThat(subManager.processResourceListChange(List.of("foo"))).isPresent();
        assertThat(subManager.isSubscribedTo("foo")).isTrue();
        assertThat(subManager.isSubscribedTo("bar")).isFalse();
    }

    @Test
    public void testEmptyListAfterNamesUnsubscribesFromEverything() {
        WildcardSubManager subManager = new WildcardSubManager();
        assertThat(subManager.processResourceListChange(List.of("*"))).isPresent();
        assertThat(subManager.processResourceListChange(Collections.emptyList())).isPresent();
        assertThat(subManager.isSubscribedTo("foo")).isFalse();
    }

    private static Set<String> subscribed(SubManager subManager, Map<String, ?> resources) {
        return resources.keySet().stream().filter(subManager::isSubscribedTo).collect(Collectors.toSet());
    }
}
