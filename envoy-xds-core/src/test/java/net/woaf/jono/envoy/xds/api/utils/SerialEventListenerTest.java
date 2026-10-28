package net.woaf.jono.envoy.xds.api.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import org.junit.jupiter.api.Test;

public class SerialEventListenerTest {

    /** Records what it gets, and complains if two deliveries overlap. */
    private static class Recorder implements ClusterManagerEventListener<Integer> {
        private final List<Object> calls = new ArrayList<>();
        private int running = 0;
        private final int failOn;

        Recorder(int failOn) {
            this.failOn = failOn;
        }

        @Override
        public void onNetworkChange(Integer diff) {
            boolean alone;
            synchronized (this) {
                alone = ++running == 1;
            }
            try {
                record(alone ? diff : "overlap at " + diff);
                if (diff == failOn) {
                    throw new IllegalStateException("boom");
                }
            } finally {
                synchronized (this) {
                    running--;
                }
            }
        }

        @Override
        public void close() {
            record("closed");
        }

        private synchronized void record(Object call) {
            calls.add(call);
        }

        synchronized List<Object> calls() {
            return List.copyOf(calls);
        }
    }

    private static List<Object> deliver(int updates, int failOn) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Recorder recorder = new Recorder(failOn);
        SerialEventListener<Integer> listener = new SerialEventListener<>(pool, recorder);
        IntStream.range(0, updates).forEach(listener::onNetworkChange);
        listener.close();
        listener.onNetworkChange(updates); // after close: dropped
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        return recorder.calls();
    }

    @Test
    public void deliversEveryUpdateInOrderThenCloses() throws InterruptedException {
        List<Object> expected = new ArrayList<>(IntStream.range(0, 1000).boxed().toList());
        expected.add("closed");
        assertThat(deliver(1000, -1)).isEqualTo(expected);
    }

    @Test
    public void listenerThatThrowsGetsNoMore() throws InterruptedException {
        assertThat(deliver(10, 2)).containsExactly(0, 1, 2, "closed");
    }

    @Test
    public void deliversOnTheSharedExecutorByDefault() throws InterruptedException {
        CountDownLatch closed = new CountDownLatch(1);
        Recorder recorder = new Recorder(-1) {
            @Override
            public void close() {
                super.close();
                closed.countDown();
            }
        };
        SerialEventListener<Integer> listener = new SerialEventListener<>(recorder);
        listener.onNetworkChange(1);
        listener.close();
        assertThat(closed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(recorder.calls()).containsExactly(1, "closed");
    }
}
