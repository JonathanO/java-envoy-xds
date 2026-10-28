package net.woaf.jono.envoy.xds.internal;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.woaf.jono.envoy.xds.internal.CommonDiscoveryRequest;
import net.woaf.jono.envoy.xds.internal.DiscoveryService;
import net.woaf.jono.envoy.xds.internal.DiscoveryServiceManager;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.api.StateBacklog;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
public class DiscoveryServiceManagerTest {

    public static class DummyUpdate {

    }

    public static class QueueBacklog implements StateBacklog<DummyUpdate> {

        private final Queue<DummyUpdate> queue = new ArrayDeque<>();

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public void put(DummyUpdate update) {
            queue.add(update);
        }

        @Override
        public DummyUpdate take() {
            return queue.poll();
        }
    }

    @Test
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT", justification = "Mockito verification")
    public void testSingleServicePush(@Mock DiscoveryService<Message, DummyUpdate> mockDiscoveryService) {

        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(Map.of(TypeUrl.EDS, mockDiscoveryService), List.of(TypeUrl.EDS), List.of(TypeUrl.EDS), new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS);

        var initUpdate = new DummyUpdate();

        dsm.init(initUpdate, null, null);

        InOrder inOrder = Mockito.inOrder(mockDiscoveryService);
        inOrder.verify(mockDiscoveryService).init(initUpdate);
        inOrder.verify(mockDiscoveryService).setDeferReconnectRemovals(any());

        var update = new DummyUpdate();

        dsm.pushUpdates(update);
        inOrder.verify(mockDiscoveryService).hasDeferredReconnectRemovals(); // stale removals from a reconnect go first
        inOrder.verify(mockDiscoveryService).onNetworkUpdate(update);
        inOrder.verify(mockDiscoveryService).sendNetworkUpdatePre();
        inOrder.verify(mockDiscoveryService).awaitingAck();
        inOrder.verify(mockDiscoveryService).sendNetworkUpdatePost();
        inOrder.verify(mockDiscoveryService).awaitingAck();
        inOrder.verify(mockDiscoveryService).hasDeferredReconnectRemovals();
        verifyNoMoreInteractions(mockDiscoveryService);
    }


    @Test
    public void testSingleServicePushBlocksUntilResponse() {
        DiscoveryService<Message, DummyUpdate> eds = spy(new StateAwareFakeDiscoveryService(TypeUrl.EDS));

        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(Map.of(TypeUrl.EDS, eds), List.of(TypeUrl.EDS), List.of(TypeUrl.EDS), new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS);

        dsm.init(new DummyUpdate(), null, null);
        final var update = new DummyUpdate();
        final var update2 = new DummyUpdate();
        final var update3 = new DummyUpdate();

        dsm.pushUpdates(update);

        InOrder inOrder = Mockito.inOrder(eds);
        inOrder.verify(eds).onNetworkUpdate(update);
        inOrder.verify(eds).sendNetworkUpdatePre();
        inOrder.verify(eds, never()).onNetworkUpdate(update2);

        dsm.pushUpdates(update2); // queued behind update
        dsm.pushUpdates(update3);
        ack(dsm, eds);

        inOrder.verify(eds).processUpdate(request(TypeUrl.EDS));
        inOrder.verify(eds).sendNetworkUpdatePost();

        ack(dsm, eds); // update is done, so update2 starts
        inOrder.verify(eds).onNetworkUpdate(update2);
        inOrder.verify(eds).sendNetworkUpdatePre();

        ack(dsm, eds);
        inOrder.verify(eds).sendNetworkUpdatePost();

        ack(dsm, eds);
        inOrder.verify(eds).onNetworkUpdate(update3);
        inOrder.verify(eds).sendNetworkUpdatePre();

        ack(dsm, eds);
        inOrder.verify(eds).sendNetworkUpdatePost();
    }

    @Test
    public void testSingleUpdateMultipleDiscoveryServices() {
        final DiscoveryService<Message, DummyUpdate> eds = spy(new StateAwareFakeDiscoveryService(TypeUrl.EDS));
        final DiscoveryService<Message, DummyUpdate> cds = spy(new StateAwareFakeDiscoveryService(TypeUrl.CDS));

        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.EDS, eds, TypeUrl.CDS, cds),
            List.of(TypeUrl.EDS, TypeUrl.CDS), List.of(TypeUrl.CDS, TypeUrl.EDS),
            new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS
        );
        dsm.init(new DummyUpdate(), null, null);
        final var update = new DummyUpdate();

        dsm.pushUpdates(update);

        verify(cds).onNetworkUpdate(update);
        verify(eds).onNetworkUpdate(update);

        InOrder inOrder = Mockito.inOrder(eds, cds);
        inOrder.verify(eds).sendNetworkUpdatePre();
        inOrder.verify(cds, never()).sendNetworkUpdatePre(); // Shouldn't continue until acked.

        ack(dsm, eds);

        inOrder.verify(eds).processUpdate(request(TypeUrl.EDS));
        inOrder.verify(cds).sendNetworkUpdatePre();
        inOrder.verify(cds, never()).sendNetworkUpdatePost(); // Shouldn't continue until acked.

        ack(dsm, cds);

        inOrder.verify(cds).processUpdate(request(TypeUrl.CDS));
        inOrder.verify(cds).sendNetworkUpdatePost();
        inOrder.verify(eds, never()).sendNetworkUpdatePost(); // Shouldn't continue until acked.

        ack(dsm, cds);

        inOrder.verify(cds).processUpdate(request(TypeUrl.CDS));
        inOrder.verify(eds).sendNetworkUpdatePost();

        ack(dsm, eds);
        inOrder.verify(eds).processUpdate(request(TypeUrl.EDS));
    }

    @Test
    public void testUpdateDuringInitDoesNotPush() {
        // To reduce the risk of running into bugs in envoy's cluster initialisation code we don't actively push updates on any discovery service while an existing response
        // for any of them remains unacked.
        // This doesn't prevent us answering interleaved requests from envoy, but it does block pushing network updates.
        // It also bounds what we queue on the stream: we never write while Envoy is behind.

        // The discovery service type isn't really relevant here, this should work with any types. I chose CDS and EDS simply because they really do interleave this way.
        final DiscoveryService<Message, DummyUpdate> cds = spy(new StateAwareFakeDiscoveryService(TypeUrl.CDS));
        final DiscoveryService<Message, DummyUpdate> eds = spy(new StateAwareFakeDiscoveryService(TypeUrl.EDS));

        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds, TypeUrl.EDS, eds),
            List.of(TypeUrl.CDS, TypeUrl.EDS), List.of(TypeUrl.EDS, TypeUrl.CDS),
            new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS
        );

        dsm.init(new DummyUpdate(), null, null);

        final var update = new DummyUpdate();

        subscribe(dsm, cds);
        subscribe(dsm, eds);
        ack(dsm, eds);
        dsm.pushUpdates(update); // Should do nothing until CDS is acked.

        verify(eds).init(any());
        verify(cds).init(any());

        InOrder inOrder = Mockito.inOrder(cds, eds);
        inOrder.verify(cds).processUpdate(request(TypeUrl.CDS));
        inOrder.verify(eds).processUpdate(request(TypeUrl.EDS));
        inOrder.verify(eds).processUpdate(request(TypeUrl.EDS));
        inOrder.verify(cds, never()).sendNetworkUpdatePre();
        inOrder.verify(eds, never()).sendNetworkUpdatePre();
        ack(dsm, cds);
        inOrder.verify(cds).processUpdate(request(TypeUrl.CDS));
        inOrder.verify(cds).sendNetworkUpdatePre();
    }

    @Test
    public void testUpdatesDelayedUntilConfiguredAck() {
        // We optionally allow the DSM to block updates until the first ACK of a given DS. This can be useful to work around bugs in envoy's cluster initialisation.
        // For example to work around https://github.com/envoyproxy/envoy/issues/16035 in older envoy versions, our service mesh blocks updates until RDS is ACKed
        // since envoy won't ACK that until after clusters have been fully initialized.

        final DiscoveryService<Message, DummyUpdate> cds = spy(new StateAwareFakeDiscoveryService(TypeUrl.CDS));
        final DiscoveryService<Message, DummyUpdate> rds = spy(new StateAwareFakeDiscoveryService(TypeUrl.RDS));

        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds, TypeUrl.RDS, rds),
            List.of(TypeUrl.CDS, TypeUrl.RDS), List.of(TypeUrl.RDS, TypeUrl.CDS),
            new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS
        );

        dsm.init(new DummyUpdate(), TypeUrl.RDS, null); // Note configuring delayUpdatesUntilAckOf

        final var update = new DummyUpdate();

        final InOrder inOrder = Mockito.inOrder(cds, rds);
        subscribe(dsm, cds);
        ack(dsm, cds);

        verify(cds).init(any());
        verify(rds).init(any());

        inOrder.verify(cds).processUpdate(request(TypeUrl.CDS));
        inOrder.verify(cds).processUpdate(request(TypeUrl.CDS));

        dsm.pushUpdates(update); // Should do nothing until RDS is acked.

        inOrder.verify(cds, never()).sendNetworkUpdatePre();
        inOrder.verify(rds, never()).sendNetworkUpdatePre();
        subscribe(dsm, rds);
        inOrder.verify(rds).processUpdate(request(TypeUrl.RDS));
        inOrder.verify(cds, never()).sendNetworkUpdatePre();
        inOrder.verify(rds, never()).sendNetworkUpdatePre();
        ack(dsm, rds);
        inOrder.verify(rds).processUpdate(request(TypeUrl.RDS));
        inOrder.verify(cds).sendNetworkUpdatePre();
        ack(dsm, cds);
        inOrder.verify(rds).sendNetworkUpdatePre();
        ack(dsm, rds);
        inOrder.verify(rds).sendNetworkUpdatePost();
        ack(dsm, rds);
        inOrder.verify(cds).sendNetworkUpdatePost();
    }

    @Test
    public void testNackFreezesTheStream(@Mock DiscoveryServiceManagerMetrics metrics) {
        DiscoveryService<Message, DummyUpdate> eds = spy(new StateAwareFakeDiscoveryService(TypeUrl.EDS));
        QueueBacklog backlog = new QueueBacklog();
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(Map.of(TypeUrl.EDS, eds), List.of(TypeUrl.EDS), List.of(TypeUrl.EDS), backlog, metrics);
        dsm.init(new DummyUpdate(), null, null);
        final var update = new DummyUpdate();
        final var queued = new DummyUpdate();
        final var queuedToo = new DummyUpdate();
        final var later = new DummyUpdate();

        dsm.pushUpdates(update);
        dsm.pushUpdates(queued); // waits behind update
        dsm.pushUpdates(queuedToo); // this backlog doesn't merge them
        nack(dsm, eds);
        dsm.pushUpdates(later);

        assertThat(backlog.isEmpty()).as("backlog dropped").isTrue();
        verify(eds).abandonChange(); // so answers to later subscriptions don't send what's left of it
        verify(eds, never()).sendNetworkUpdatePost();
        verify(eds, never()).onNetworkUpdate(queued);
        verify(eds, never()).onNetworkUpdate(queuedToo);
        verify(eds, never()).onNetworkUpdate(later);
        verify(metrics).onPushComplete(); // the abandoned push ended, so the stream looks idle
    }

    @Test
    public void testConfiguredDelayWithoutAckBlocksQueuedUpdates() {
        final DiscoveryService<Message, DummyUpdate> cds = spy(new StateAwareFakeDiscoveryService(TypeUrl.CDS));
        final DiscoveryService<Message, DummyUpdate> rds = spy(new StateAwareFakeDiscoveryService(TypeUrl.RDS));

        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds, TypeUrl.RDS, rds),
            List.of(TypeUrl.CDS, TypeUrl.RDS), List.of(TypeUrl.RDS, TypeUrl.CDS),
            new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS
        );

        dsm.init(new DummyUpdate(), TypeUrl.RDS, null);

        final var update = new DummyUpdate();

        subscribe(dsm, cds);
        ack(dsm, cds);

        dsm.pushUpdates(update);

        subscribe(dsm, cds);
        ack(dsm, cds);

        verify(cds, never()).sendNetworkUpdatePre();
        verify(rds, never()).sendNetworkUpdatePre();
    }

    @Test
    public void testConfiguredDelayGivesUpWithoutAnAck() {
        final DiscoveryService<Message, DummyUpdate> cds = spy(new StateAwareFakeDiscoveryService(TypeUrl.CDS));
        final DiscoveryService<Message, DummyUpdate> rds = spy(new StateAwareFakeDiscoveryService(TypeUrl.RDS));
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds, TypeUrl.RDS, rds), List.of(TypeUrl.CDS, TypeUrl.RDS), List.of(TypeUrl.RDS, TypeUrl.CDS),
            new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS);
        AtomicLong now = new AtomicLong();
        dsm.setClock(now::get);
        dsm.init(new DummyUpdate(), TypeUrl.RDS, null); // but this client never subscribes to RDS
        final var update = new DummyUpdate();

        dsm.pushUpdates(update);
        verify(cds, never()).onNetworkUpdate(update);

        now.addAndGet(DiscoveryServiceManager.WAIT_FOR_ACK_TIMEOUT_NANOS);
        dsm.pushUpdates(new DummyUpdate()); // the next change
        verify(cds).onNetworkUpdate(update);
    }

    @Test
    public void testConfiguredDelayIgnoredForATypeTheClientDoesNotUse() {
        final DiscoveryService<Message, DummyUpdate> cds = spy(new StateAwareFakeDiscoveryService(TypeUrl.CDS));
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds), List.of(TypeUrl.CDS), List.of(TypeUrl.CDS), new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS);
        dsm.init(new DummyUpdate(), TypeUrl.RDS, Set.of(TypeUrl.CDS, TypeUrl.EDS));
        final var update = new DummyUpdate();

        dsm.pushUpdates(update);
        verify(cds).onNetworkUpdate(update);
    }

    @Test
    public void staleRemovalsGoBeforeAChangeAndCountAsAwaitingAnAck(@Mock DiscoveryServiceManagerMetrics metrics) {
        final DiscoveryService<Message, DummyUpdate> cds = spy(new StateAwareFakeDiscoveryService(TypeUrl.CDS).withStaleRemovals());
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds), List.of(TypeUrl.CDS), List.of(TypeUrl.CDS), new QueueBacklog(), metrics);
        dsm.init(new DummyUpdate(), null, null);
        final var update = new DummyUpdate();

        dsm.pushUpdates(update);
        verify(cds).sendDeferredReconnectRemovals();
        verify(metrics).onAwaitingAck();
        verify(cds, never()).onNetworkUpdate(update); // waits for their ACK

        ack(dsm, cds);
        verify(metrics).onMessageAcked();
        verify(cds).onNetworkUpdate(update);
    }

    @Test
    public void staleRemovalsWaitForEarlierTypesUntilTheyGiveUpForGood() {
        final DiscoveryService<Message, DummyUpdate> cds = spy(new StateAwareFakeDiscoveryService(TypeUrl.CDS).withStaleRemovals());
        final DiscoveryService<Message, DummyUpdate> eds = spy(new StateAwareFakeDiscoveryService(TypeUrl.EDS).withStaleRemovals());
        final DiscoveryService<Message, DummyUpdate> rds = spy(new StateAwareFakeDiscoveryService(TypeUrl.RDS));
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds, TypeUrl.EDS, eds, TypeUrl.RDS, rds),
            List.of(TypeUrl.CDS, TypeUrl.EDS, TypeUrl.RDS), List.of(TypeUrl.RDS, TypeUrl.CDS, TypeUrl.EDS),
            new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS);
        AtomicLong now = new AtomicLong();
        dsm.setClock(now::get);
        dsm.init(new DummyUpdate(), null, null); // and RDS never subscribes

        subscribe(dsm, eds);
        ack(dsm, eds);
        verify(cds, never()).sendDeferredReconnectRemovals(); // routes may still use the clusters

        now.addAndGet(DiscoveryServiceManager.WAIT_FOR_ACK_TIMEOUT_NANOS);
        subscribe(dsm, eds);
        ack(dsm, eds);
        verify(cds).sendDeferredReconnectRemovals();
        verify(eds, never()).sendDeferredReconnectRemovals(); // in remove order: after CDS's are ACKed

        ack(dsm, cds);
        verify(eds).sendDeferredReconnectRemovals();
    }

    @Test
    public void metricsFollowAPushFromBeginToComplete(@Mock DiscoveryServiceManagerMetrics metrics) {
        DiscoveryService<Message, DummyUpdate> eds = spy(new StateAwareFakeDiscoveryService(TypeUrl.EDS));
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.EDS, eds), List.of(TypeUrl.EDS), List.of(TypeUrl.EDS), new QueueBacklog(), metrics);
        dsm.init(new DummyUpdate(), null, null);

        dsm.pushUpdates(new DummyUpdate());
        verify(metrics).onPushBegin();
        ack(dsm, eds);
        verify(metrics, never()).onPushComplete(); // its removals aren't sent yet
        ack(dsm, eds);
        verify(metrics).onPushComplete();
    }

    @Test
    public void metricsCountAnAnswerAsAwaitingAnAckUntilItIsAcked(@Mock DiscoveryServiceManagerMetrics metrics) {
        DiscoveryService<Message, DummyUpdate> eds = spy(new StateAwareFakeDiscoveryService(TypeUrl.EDS));
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.EDS, eds), List.of(TypeUrl.EDS), List.of(TypeUrl.EDS), new QueueBacklog(), metrics);
        dsm.init(new DummyUpdate(), null, null);

        subscribe(dsm, eds);
        verify(metrics).onAwaitingAck();
        verify(metrics, never()).onMessageAcked();
        ack(dsm, eds);
        verify(metrics).onMessageAcked();
    }

    @Test
    public void closingTheManagerClosesItsMetrics(@Mock DiscoveryServiceManagerMetrics metrics) {
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.EDS, new StateAwareFakeDiscoveryService(TypeUrl.EDS)), List.of(TypeUrl.EDS), List.of(TypeUrl.EDS), new QueueBacklog(), metrics);
        dsm.init(new DummyUpdate(), null, null);

        dsm.close();
        verify(metrics).close();
    }

    @Test
    public void closedManagerRefusesChanges() {
        DiscoveryService<Message, DummyUpdate> eds = spy(new StateAwareFakeDiscoveryService(TypeUrl.EDS));
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.EDS, eds), List.of(TypeUrl.EDS), List.of(TypeUrl.EDS), new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS);
        dsm.init(new DummyUpdate(), null, null);
        dsm.close();

        assertThatThrownBy(() -> dsm.pushUpdates(new DummyUpdate())).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        verify(eds, never()).onNetworkUpdate(any());
    }

    @Test
    public void managerRefusesRequestsBeforeInit() {
        DiscoveryService<Message, DummyUpdate> eds = spy(new StateAwareFakeDiscoveryService(TypeUrl.EDS));
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.EDS, eds), List.of(TypeUrl.EDS), List.of(TypeUrl.EDS), new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS);

        assertThatThrownBy(() -> dsm.processUpdate(request(TypeUrl.EDS))).isInstanceOf(IllegalStateException.class).hasMessageContaining("must be initialized");
        verify(eds, never()).processUpdate(any());
    }

    @Test
    public void typesWithNoReconnectRemovalsNeverWarnTheyAreRemovingStaleOnes() {
        // Like SotW's, the fake's services have no removals found on reconnecting.
        final DiscoveryService<Message, DummyUpdate> cds = new StateAwareFakeDiscoveryService(TypeUrl.CDS);
        final DiscoveryService<Message, DummyUpdate> rds = new StateAwareFakeDiscoveryService(TypeUrl.RDS);
        DiscoveryServiceManager<Message, DummyUpdate> dsm = new DiscoveryServiceManager<>(
            Map.of(TypeUrl.CDS, cds, TypeUrl.RDS, rds), List.of(TypeUrl.CDS, TypeUrl.RDS), List.of(TypeUrl.RDS, TypeUrl.CDS),
            new QueueBacklog(), DiscoveryServiceManagerMetrics.NOOP_METRICS);
        AtomicLong now = new AtomicLong();
        dsm.setClock(now::get);
        dsm.init(new DummyUpdate(), null, null);
        now.addAndGet(DiscoveryServiceManager.WAIT_FOR_ACK_TIMEOUT_NANOS); // and RDS hasn't answered

        try (LoggedWarnings warnings = new LoggedWarnings(DiscoveryServiceManager.class)) {
            dsm.pushUpdates(new DummyUpdate());

            assertThat(warnings.messages()).isEmpty();
        }
    }

    /** Envoy subscribes to something of this type, which the fake answers. */
    private static void subscribe(DiscoveryServiceManager<Message, DummyUpdate> dsm, DiscoveryService<Message, DummyUpdate> service) {
        assertThat(service.awaitingAck()).as("%s awaiting an ACK", service.getTypeUrl()).isFalse();
        dsm.processUpdate(request(service.getTypeUrl()));
    }

    /** Envoy ACKs what this service last sent. */
    private static void ack(DiscoveryServiceManager<Message, DummyUpdate> dsm, DiscoveryService<Message, DummyUpdate> service) {
        assertThat(service.awaitingAck()).as("%s awaiting an ACK", service.getTypeUrl()).isTrue();
        dsm.processUpdate(request(service.getTypeUrl()));
    }

    /** Envoy rejects what this service last sent. */
    private static void nack(DiscoveryServiceManager<Message, DummyUpdate> dsm, DiscoveryService<Message, DummyUpdate> service) {
        assertThat(service.awaitingAck()).as("%s awaiting an ACK", service.getTypeUrl()).isTrue();
        dsm.processUpdate(new CommonDiscoveryRequest<>(null, service.getTypeUrl().getTypeUrl(), null, com.google.rpc.Status.newBuilder().setMessage("rejected").build()));
    }

    /** The fake doesn't look at requests, so one per type is enough. */
    private static CommonDiscoveryRequest<Message> request(TypeUrl typeUrl) {
        return new CommonDiscoveryRequest<>(null, typeUrl.getTypeUrl(), null, null);
    }

    public static class StateAwareFakeDiscoveryService implements DiscoveryService<Message, DummyUpdate> {

        private final TypeUrl typeUrl;
        private boolean initialized = false;
        private boolean awaitingAck = false;
        private boolean answered = false;
        private boolean staleRemovals = false;

        public StateAwareFakeDiscoveryService(TypeUrl typeUrl) {
            this.typeUrl = typeUrl;
        }

        /** Envoy reconnected with resources of this type we no longer have, whose removal waits its turn. */
        StateAwareFakeDiscoveryService withStaleRemovals() {
            staleRemovals = true;
            return this;
        }

        @Override
        public void processUpdate(CommonDiscoveryRequest<Message> value) {
            Preconditions.checkState(initialized);
            // If we're waiting, assume this was an ack
            // If we're not waiting, assume it was a request to subscribe to new stuff, which we answer.
            awaitingAck = !awaitingAck;
            answered = true;
        }

        @Override
        public boolean hasAnswered() {
            return answered;
        }

        @Override
        public boolean hasDeferredReconnectRemovals() {
            return staleRemovals;
        }

        @Override
        public void sendDeferredReconnectRemovals() {
            staleRemovals = false;
            awaitingAck = true;
            answered = true;
        }

        @Override
        public boolean awaitingAck() {
            return awaitingAck;
        }

        @Override
        public boolean isServed() {
            return true;
        }

        @Override
        public void init(DummyUpdate state) {
            Preconditions.checkState(!initialized);
            initialized = true;
        }

        @Override
        public void onNetworkUpdate(DummyUpdate changes) {
            Preconditions.checkState(initialized);
        }

        @Override
        public void sendNetworkUpdatePre() {
            Preconditions.checkState(initialized);
            // Assume we sent something
            awaitingAck = true;
            answered = true;
        }

        @Override
        public void sendNetworkUpdatePost() {
            Preconditions.checkState(initialized);
            awaitingAck = true;
            answered = true;
        }

        @Override
        public void abandonChange() {
        }

        @Override
        public TypeUrl getTypeUrl() {
            return typeUrl;
        }
    }

}
