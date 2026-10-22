package net.woaf.jono.envoy.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import net.woaf.jono.envoy.testing.utils.EnvoyContainer;

/**
 * Checks on Envoy itself, rather than on what was said to it.
 */
public final class EnvoyChecks {

    private EnvoyChecks() {
    }

    /**
     * Envoy is connected to its control plane, rejected no updates, has nothing warming and timed out no initial fetches
     * (Envoy 1.24 on; older ones have no stat for it). Stats can trail an ACK a little, so this retries for a few seconds.
     * @param envoy the running Envoy
     */
    public static void assertHealthy(EnvoyContainer envoy) {
        assertHealthy(envoy, Map.of());
    }

    /**
     * As {@link #assertHealthy(EnvoyContainer)}, but only counting rejections and timeouts since baseline was taken, e.g.
     * after a deliberately rejected update.
     * @param envoy the running Envoy
     * @param baseline earlier {@link net.woaf.jono.envoy.testing.configdump.EnvoyAdminClient#stats()}
     */
    public static void assertHealthy(EnvoyContainer envoy, Map<String, Long> baseline) {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, Long> stats = envoy.admin().stats();
            assertThat(stats)
                .containsEntry("control_plane.connected_state", 1L)
                .containsEntry("cluster_manager.warming_clusters", 0L)
                .containsEntry("listener_manager.total_listeners_warming", 0L);
            assertThat(stats.entrySet().stream()
                .filter(e -> (e.getKey().endsWith(".update_rejected") || e.getKey().endsWith(".init_fetch_timeout")) && !e.getValue().equals(baseline.getOrDefault(e.getKey(), 0L)))
                .toList()).as("rejected updates and initial fetch timeouts").isEmpty();
        });
    }

    /**
     * Envoy logged no warnings or errors from fromLine on.
     * @param envoy the Envoy
     * @param fromLine e.g. {@link EnvoyContainer#logSize()} once Envoy has synced, to skip startup noise
     */
    public static void assertNoWarningsSince(EnvoyContainer envoy, int fromLine) {
        assertThat(envoy.logSince(fromLine)).as("Envoy warnings and errors")
            .filteredOn(l -> l.matches(".*\\]\\[(warning|error|critical)\\]\\[.*")).isEmpty();
    }
}
