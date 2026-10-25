package net.woaf.jono.envoy.xds.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.StringValue;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.grpc.stub.StreamObserver;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

import static net.woaf.jono.envoy.xds.snapshot.TestResources.cluster;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Races on SnapshotSource, explored interleaving by interleaving with Fray rather than by chance.
 */
@ExtendWith(FrayTestExtension.class)
public class SnapshotSourceFrayTest {

    /** Subscribes holding its own lock, as CommonDiscoveryStreamObserver does, and checks each update follows on. */
    private static final class Client implements ClusterManagerEventListener<SnapshotUpdate<Object>> {
        private final CountDownLatch closed = new CountDownLatch(1);

        @GuardedBy("this")
        private @Nullable Snapshot seen = null;

        @GuardedBy("this")
        private String problem = "";

        synchronized void connect(SnapshotSource<Object> source) {
            seen = source.subscribe(this).getCurrent();
        }

        @Override
        public synchronized void onNetworkChange(SnapshotUpdate<Object> update) {
            if (update.getPrevious() != seen && problem.isEmpty()) {
                problem = "got an update from " + update.getPrevious().id() + " while holding " + (seen == null ? "nothing" : seen.id());
            }
            seen = update.getCurrent();
        }

        @Override
        public void close() {
            closed.countDown();
        }

        synchronized @Nullable Snapshot seen() {
            return seen;
        }

        synchronized String problem() {
            return problem;
        }
    }

    /** Our own pool, so each interleaving can shut it down: Fray needs every thread to end. */
    private static ExecutorService pool() {
        return Executors.newFixedThreadPool(2);
    }

    /** Waits for every queued delivery, then lets the pool's threads end. */
    private static void drain(ExecutorService pool) throws InterruptedException {
        pool.shutdown();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
    }

    @FrayTest(iterations = 100)
    void subscribingWhilePublishingMissesNothing() throws InterruptedException {
        ExecutorService pool = pool();
        SnapshotSource<Object> source = new SnapshotSource<>(null, pool);
        Client client = new Client();

        // A small message, so each interleaving doesn't load Envoy's API.
        Thread connect = new Thread(() -> client.connect(source));
        Thread publish = new Thread(() -> {
            source.update(e -> e.put("a", StringValue.of("1")));
            source.update(e -> e.put("b", StringValue.of("2")));
        });
        connect.start();
        publish.start();
        connect.join();
        publish.join();
        // Unsubscribing drains what's queued, then closes the client.
        source.unsubscribe(client);
        client.closed.await();
        drain(pool);

        assertThat(client.problem()).isEmpty();
        assertThat(client.seen()).isSameAs(source.current());
    }

    /** Checks what gRPC requires of a server: calls never overlap, nothing follows the end, and it ends once. */
    private static final class Responses implements StreamObserver<DeltaDiscoveryResponse> {
        private final CountDownLatch ended = new CountDownLatch(1);
        private final AtomicBoolean inCall = new AtomicBoolean(false);
        private final AtomicInteger ends = new AtomicInteger();
        private final Queue<String> problems = new ConcurrentLinkedQueue<>();

        private void call(String what, Runnable body) {
            if (!inCall.compareAndSet(false, true)) {
                problems.add(what + " while another call was in progress");
            }
            if (ends.get() > 0) {
                problems.add(what + " after the stream ended");
            }
            body.run();
            inCall.set(false);
        }

        @Override
        public void onNext(DeltaDiscoveryResponse value) {
            call("onNext", () -> { });
        }

        @Override
        public void onError(Throwable t) {
            call("onError", () -> problems.add("failed: " + t));
            ends.incrementAndGet();
            ended.countDown();
        }

        @Override
        public void onCompleted() {
            call("onCompleted", () -> { });
            ends.incrementAndGet();
            ended.countDown();
        }
    }

    // Taking synchronized off CommonDiscoveryStreamObserver.onNetworkChange first fails around interleaving 60.
    @FrayTest(iterations = 150)
    void streamEndingWhileChangesArriveEndsCleanly() throws InterruptedException {
        ExecutorService pool = pool();
        SnapshotSource<Object> source = new SnapshotSource<>(null, pool);
        AggregatedDiscoveryService<SnapshotUpdate<Object>, Object> ads = new AggregatedDiscoveryService<>(source,
            SnapshotConfigBuilder.<Object>forTypes(Cluster.class), node -> XdsConfig.of(new Object()), ImmutableList.of(),
            MergingStateBacklog.factory(), DiscoveryServiceManagerMetrics.METRICS_DISABLED);
        Responses responses = new Responses();
        StreamObserver<DeltaDiscoveryRequest> requests = ads.deltaAggregatedResources(responses);

        // gRPC delivers one stream's requests on one thread at a time; changes arrive on the source's pool.
        Thread client = new Thread(() -> {
            requests.onNext(DeltaDiscoveryRequest.newBuilder().setTypeUrl(TypeUrl.CDS.getTypeUrl()).build());
            requests.onNext(DeltaDiscoveryRequest.newBuilder().setTypeUrl(TypeUrl.CDS.getTypeUrl()).addResourceNamesSubscribe("a").build());
            requests.onCompleted();
        });
        Thread publish = new Thread(() -> {
            source.update(e -> e.put(cluster("a")));
            source.update(e -> e.put(cluster("b")));
            source.update(e -> e.remove(Cluster.class, "a"));
        });
        client.start();
        publish.start();
        client.join();
        publish.join();
        responses.ended.await();
        // Let any change still queued arrive: nothing may follow the end.
        source.update(e -> e.put(cluster("c")));
        drain(pool);

        assertThat(responses.problems).isEmpty();
        assertThat(responses.ends.get()).isEqualTo(1);
    }
}
