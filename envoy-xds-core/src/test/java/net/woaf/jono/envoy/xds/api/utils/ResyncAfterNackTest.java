package net.woaf.jono.envoy.xds.api.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.rpc.Status;
import io.envoyproxy.envoy.config.core.v3.Node;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.api.ClientHandle;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class ResyncAfterNackTest {

    private static final Node NODE = Node.newBuilder().setId("envoy-1").build();

    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
    private final ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    private final ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);

    @Test
    public void backsOffWhileTheSameNodeKeepsNacking() {
        doReturn(future).when(scheduler).schedule(any(Runnable.class), anyLong(), any());
        ResyncAfterNack<Object> resync = new ResyncAfterNack<>(scheduler, Duration.ofMinutes(1), Duration.ofMinutes(3));

        for (int i = 0; i < 4; i++) {
            ClientHandle stream = mock(ClientHandle.class); // each resync is a new stream
            resync.onClientNack(stream, NODE, TypeUrl.CDS, Status.getDefaultInstance());
            resync.onClientNack(stream, NODE, TypeUrl.EDS, Status.getDefaultInstance()); // same stream: no second resync
            verify(scheduler, times(i + 1)).schedule(task.capture(), anyLong(), eq(TimeUnit.MILLISECONDS));
            task.getValue().run();
            verify(stream).resync();
        }

        verify(scheduler, times(4)).schedule(any(Runnable.class), delays.capture(), eq(TimeUnit.MILLISECONDS));
        assertThat(delays.getAllValues()).containsExactly(60_000L, 120_000L, 180_000L, 180_000L);
    }

    @Test
    public void startsAgainAfterAQuietSpell() {
        doReturn(future).when(scheduler).schedule(any(Runnable.class), anyLong(), any());
        AtomicReference<Instant> now = new AtomicReference<>(Instant.EPOCH);
        ResyncAfterNack<Object> resync = new ResyncAfterNack<>(scheduler, Duration.ofMillis(100), Duration.ofMillis(200), now::get);
        List<ClientHandle> streams = List.of(mock(ClientHandle.class), mock(ClientHandle.class), mock(ClientHandle.class));

        resync.onClientNack(streams.get(0), NODE, TypeUrl.CDS, Status.getDefaultInstance());
        resync.onClientNack(streams.get(1), NODE, TypeUrl.CDS, Status.getDefaultInstance());
        now.set(Instant.EPOCH.plusMillis(401)); // over 2 * maxDelay
        resync.onClientNack(streams.get(2), NODE, TypeUrl.CDS, Status.getDefaultInstance());

        verify(scheduler, times(3)).schedule(any(Runnable.class), delays.capture(), eq(TimeUnit.MILLISECONDS));
        assertThat(delays.getAllValues()).isEqualTo(List.of(100L, 200L, 100L));
    }

    @Test
    public void forgetsNodesThatWentQuiet() {
        doReturn(future).when(scheduler).schedule(any(Runnable.class), anyLong(), any());
        AtomicReference<Instant> now = new AtomicReference<>(Instant.EPOCH);
        ResyncAfterNack<Object> resync = new ResyncAfterNack<>(scheduler, Duration.ofMillis(100), Duration.ofMillis(200), now::get);

        for (int i = 0; i < 100; i++) {
            resync.onClientNack(mock(ClientHandle.class), Node.newBuilder().setId("pod-" + i).build(), TypeUrl.CDS, Status.getDefaultInstance());
        }
        now.set(Instant.EPOCH.plusMillis(401));
        resync.onClientNack(mock(ClientHandle.class), NODE, TypeUrl.CDS, Status.getDefaultInstance());

        assertThat(resync.trackedNodes()).isEqualTo(1);
    }

    @Test
    public void disconnectCancelsThePendingResync() {
        doReturn(future).when(scheduler).schedule(any(Runnable.class), anyLong(), any());
        ResyncAfterNack<Object> resync = new ResyncAfterNack<>(scheduler, Duration.ofMinutes(1), Duration.ofMinutes(30));
        ClientHandle stream = mock(ClientHandle.class);

        resync.onClientNack(stream, NODE, TypeUrl.CDS, Status.getDefaultInstance());
        resync.onClientDisconnected(stream, NODE);

        verify(future).cancel(false);
    }
}
