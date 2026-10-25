package net.woaf.jono.envoy.xds.api.utils;

import static org.assertj.core.api.Assertions.assertThat;

import net.woaf.jono.envoy.xds.api.Mergeable;
import org.junit.jupiter.api.Test;

public class MergingStateBacklogTest {

    private record SimpleMergeable(String name) implements Mergeable<SimpleMergeable> {
        @Override
        public SimpleMergeable merge(SimpleMergeable update) {
            return new SimpleMergeable(name + update.name());
        }
    }

    @Test
    public void testEmptyBacklogIsEmpty() {
        MergingStateBacklog<SimpleMergeable> merger = MergingStateBacklog.<SimpleMergeable>factory().build();
        assertThat(merger.isEmpty()).isTrue();
        assertThat(merger.take()).isNull();
    }

    @Test
    public void testWhatGoesInMustComeOut() {
        MergingStateBacklog<SimpleMergeable> merger = MergingStateBacklog.<SimpleMergeable>factory().build();
        SimpleMergeable a = new SimpleMergeable("a");
        merger.put(a);
        assertThat(merger.isEmpty()).isFalse();
        assertThat(merger.take()).isSameAs(a);
    }

    @Test
    public void testEmptiedIsEmpty() {
        MergingStateBacklog<SimpleMergeable> merger = MergingStateBacklog.<SimpleMergeable>factory().build();
        SimpleMergeable a = new SimpleMergeable("a");
        merger.put(a);
        merger.take();
        assertThat(merger.isEmpty()).isTrue();
        assertThat(merger.take()).isNull();
    }

    @Test
    public void testMergeIsOrdered() {
        MergingStateBacklog<SimpleMergeable> merger = MergingStateBacklog.<SimpleMergeable>factory().build();
        SimpleMergeable a = new SimpleMergeable("a");
        merger.put(a);
        SimpleMergeable b = new SimpleMergeable("b");
        merger.put(b);
        // a then b...
        assertThat(merger.isEmpty()).isFalse();
        assertThat(merger.take()).isEqualTo(new SimpleMergeable("ab"));
        assertThat(merger.isEmpty()).isTrue();
        assertThat(merger.take()).isNull();
    }

}
