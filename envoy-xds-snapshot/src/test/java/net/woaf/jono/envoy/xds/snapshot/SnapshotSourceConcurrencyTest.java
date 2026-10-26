package net.woaf.jono.envoy.xds.snapshot;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import dev.hegel.ConcurrentPool;
import dev.hegel.HegelTest;
import dev.hegel.Invariant;
import dev.hegel.Rule;
import dev.hegel.Stateful;
import dev.hegel.TestCase;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;

import static dev.hegel.Generators.sampledFrom;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Clients connect, disconnect and publish on several threads at once. Every update a client gets must follow on from
 * what it already has, and once things settle every connected client has the current snapshot.
 */
public class SnapshotSourceConcurrencyTest {

    @HegelTest
    void clientsSeeEveryChangeInOrder(TestCase tc) {
        Stateful.run(new Machine(tc), tc, Stateful.options().maxConcurrency(4));
    }

    /** Subscribes and unsubscribes holding its own lock, as CommonDiscoveryStreamObserver does. */
    private static final class Client implements ClusterManagerEventListener<SnapshotUpdate<Object>> {

        @GuardedBy("this")
        private boolean connected = false;

        @GuardedBy("this")
        private ViewState seen = ViewState.EMPTY;

        @GuardedBy("this")
        private String problem = "";

        synchronized void connect(SnapshotSource<Object, String, String> source) {
            seen = source.subscribe(this, "client").current();
            connected = true;
        }

        synchronized void disconnect(SnapshotSource<Object, String, String> source) {
            source.unsubscribe(this);
            connected = false;
        }

        @Override
        public synchronized void onNetworkChange(SnapshotUpdate<Object> update) {
            if (!connected) {
                return; // Updates may still drain after unsubscribe.
            }
            if (update.previous() != seen && problem.isEmpty()) {
                problem = "got an update from a state it didn't hold";
            }
            seen = update.current();
        }

        synchronized boolean caughtUpWith(Snapshot current) {
            return !connected || seen.group("all") == current;
        }

        synchronized String problem() {
            return problem;
        }
    }

    // Not private: Hegel calls its rules and invariants reflectively, which Error Prone can't see.
    static final class Machine {
        private final SnapshotSource<Object, String, String> source = TestResources.oneGroup();
        private final Queue<Client> everyClient = new ConcurrentLinkedQueue<>();
        private final ConcurrentPool<Client> connected;
        private final AtomicInteger changes = new AtomicInteger();

        Machine(TestCase tc) {
            connected = new ConcurrentPool<>(tc);
        }

        @Rule(group = "ops")
        void connect(TestCase tc) {
            Client client = new Client();
            client.connect(source);
            everyClient.add(client);
            connected.add(tc, client);
        }

        @Rule(group = "ops")
        void disconnect(TestCase tc) {
            tc.draw(connected.consuming()).disconnect(source);
        }

        @Rule(group = "ops", weight = 3)
        void publish(TestCase tc) {
            String name = tc.draw(sampledFrom("a", "b", "c"), "cluster");
            Cluster cluster = TestResources.cluster(name).toBuilder().setAltStatName("change-" + changes.incrementAndGet()).build();
            source.update(TestResources.all(e -> e.put(cluster)));
        }

        @Invariant(alwaysRun = true)
        void everyClientCatchesUpInOrder(TestCase tc) throws InterruptedException {
            Snapshot current = source.group("all");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            for (Client client: everyClient) {
                while (!client.caughtUpWith(current) && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
                assertThat(client.problem()).isEmpty();
                assertThat(client.caughtUpWith(current)).as("caught up").isTrue();
            }
        }
    }
}
