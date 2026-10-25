package net.woaf.jono.envoy.xds.api.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import net.woaf.jono.envoy.xds.api.ClusterManagerEventListener;
import net.woaf.jono.envoy.xds.api.Mergeable;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class QueueingMergingEventListenerTest {

    private record SimpleMergeable(String name) implements Mergeable<SimpleMergeable> {
        @Override
        public SimpleMergeable merge(SimpleMergeable update) {
            return new SimpleMergeable(name + update.name());
        }
    }

    @Test
    public void testEmptyBacklogIsEmpty(@Mock ClusterManagerEventListener<SimpleMergeable> mockListener) {
        QueueingMergingEventListener<SimpleMergeable> merger = QueueingMergingEventListener.createAndStart(mockListener);
        merger.close();
        merger.waitUntilFinished();

        verify(mockListener, never()).onNetworkChange(any());
        verify(mockListener, times(1)).close();
        verifyNoMoreInteractions(mockListener);
    }

    @Test
    public void testWhatGoesInMustComeOut(@Mock ClusterManagerEventListener<SimpleMergeable> mockListener) {
        BarrierEventListener<SimpleMergeable> barrierEventListener = new BarrierEventListener<>(mockListener);
        QueueingMergingEventListener<SimpleMergeable> merger = QueueingMergingEventListener.createAndStart(barrierEventListener);
        SimpleMergeable a = new SimpleMergeable("a");
        merger.onNetworkChange(a);
        {
            barrierEventListener.await(5, TimeUnit.SECONDS); // start on change call
            barrierEventListener.awaitAndRunFree(5, TimeUnit.SECONDS); // complete on change call
            verify(mockListener, times(1)).onNetworkChange(a);
            verifyNoMoreInteractions(mockListener);
        }

        merger.close();
        merger.waitUntilFinished();
        {
            verify(mockListener, times(1)).close();
            verifyNoMoreInteractions(mockListener);
        }
    }


    @Test
    public void testQueueIsOrdered(@Mock ClusterManagerEventListener<SimpleMergeable> mockListener) {
        BarrierEventListener<SimpleMergeable> barrierEventListener = new BarrierEventListener<>(mockListener);
        QueueingMergingEventListener<SimpleMergeable> merger = QueueingMergingEventListener.createAndStart(barrierEventListener);

        final SimpleMergeable a = new SimpleMergeable("a");
        final SimpleMergeable b = new SimpleMergeable("b");
        final SimpleMergeable c = new SimpleMergeable("c");

        merger.onNetworkChange(a);
        barrierEventListener.await(5, TimeUnit.SECONDS); // Wait until the change event for a is being processed
        merger.onNetworkChange(b); // b and c will go into the queue
        merger.onNetworkChange(c);
        barrierEventListener.await(5, TimeUnit.SECONDS); // Finish processing a
        barrierEventListener.await(5, TimeUnit.SECONDS); // Start processing bc
        barrierEventListener.awaitAndRunFree(5, TimeUnit.SECONDS); // Finish processing bc, let it run free and check our result.

        InOrder inOrder = inOrder(mockListener);
        // a will have been dequeued before merging could happen.
        inOrder.verify(mockListener).onNetworkChange(a);
        inOrder.verify(mockListener).onNetworkChange(new SimpleMergeable("bc"));
        verifyNoMoreInteractions(mockListener);

        merger.close();
        merger.waitUntilFinished();

        verify(mockListener, times(1)).close();
        verifyNoMoreInteractions(mockListener);
    }

}
