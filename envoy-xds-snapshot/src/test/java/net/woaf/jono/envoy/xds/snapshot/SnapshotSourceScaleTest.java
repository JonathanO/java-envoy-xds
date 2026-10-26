package net.woaf.jono.envoy.xds.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import org.junit.jupiter.api.Test;

/** Thousands of clients share a few threads, rather than one each. */
public class SnapshotSourceScaleTest {

    private static final class Client implements ClusterManagerEventListener<SnapshotUpdate<Object>> {
        private final Set<Thread> threads;
        private volatile Snapshot seen = Snapshot.empty();

        Client(Set<Thread> threads) {
            this.threads = threads;
        }

        @Override
        public void onNetworkChange(SnapshotUpdate<Object> update) {
            threads.add(Thread.currentThread());
            seen = update.getCurrent();
        }
    }

    @Test
    public void fiveThousandClientsShareTheExecutor() throws InterruptedException {
        SnapshotSource<Object> source = new SnapshotSource<>();
        Set<Thread> threads = ConcurrentHashMap.newKeySet();
        List<Client> clients = IntStream.range(0, 5000).mapToObj(i -> new Client(threads)).toList();
        clients.forEach(source::subscribe);

        for (int i = 0; i < 10; i++) {
            String name = "cluster-" + i;
            source.update(e -> e.put(TestResources.cluster(name)));
        }

        Snapshot current = source.current();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!clients.stream().allMatch(c -> c.seen == current) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(clients).allMatch(c -> c.seen == current, "caught up");
        assertThat(threads).hasSizeLessThanOrEqualTo(Runtime.getRuntime().availableProcessors());
    }
}
