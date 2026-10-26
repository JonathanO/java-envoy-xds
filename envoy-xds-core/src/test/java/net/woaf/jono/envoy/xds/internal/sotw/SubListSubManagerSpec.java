package net.woaf.jono.envoy.xds.internal.sotw;

import com.google.common.collect.ImmutableMap;
import net.woaf.jono.envoy.xds.internal.sotw.SubListSubManager;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("Duplicates")
public class SubListSubManagerSpec {

    @Test
    public void testSubscribeToEmpty() {
        SubListSubManager subManager = new SubListSubManager();
        assertThat(subManager.processResourceListChange(Collections.emptyList())).isNotPresent();
        assertThat(subManager.getSubs()).isEmpty();

        Map<String, Object> subs = ImmutableMap.of(
                "foo", new Object(),
                "bar", new Object()
        );

        assertThat(subscribed(subManager, subs)).isEmpty();
    }

    @Test
    public void testSubscribeReplace() {
        SubListSubManager subManager = new SubListSubManager();
        assertThat(subManager.processResourceListChange(Collections.singletonList("foo"))).isPresent();
        assertThat(subManager.getSubs()).contains("foo");

        Map<String, Object> subs = ImmutableMap.of(
                "foo", new Object(),
                "bar", new Object()
        );

        assertThat(subscribed(subManager, subs)).contains("foo");

        assertThat(subManager.processResourceListChange(Collections.singletonList("bar"))).isPresent();
        assertThat(subManager.getSubs()).contains("bar");
        assertThat(subscribed(subManager, subs)).contains("bar");

    }

    private static Set<String> subscribed(SubManager subManager, Map<String, ?> resources) {
        return resources.keySet().stream().filter(subManager::isSubscribedTo).collect(Collectors.toSet());
    }
}
