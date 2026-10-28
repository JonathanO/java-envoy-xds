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

        // "|" ends a phase: nothing later merges into it.
        @Override
        public boolean mergesWithLater() {
            return !name.endsWith("|");
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

    @Test
    public void laterUpdatesAreNotMergedIntoOneThatEndsAPhase() {
        MergingStateBacklog<SimpleMergeable> merger = MergingStateBacklog.<SimpleMergeable>factory().build();
        merger.put(new SimpleMergeable("a"));
        merger.put(new SimpleMergeable("b|"));
        merger.put(new SimpleMergeable("c"));
        merger.put(new SimpleMergeable("d"));
        assertThat(merger.take()).as("earlier updates still merge into it").isEqualTo(new SimpleMergeable("ab|"));
        assertThat(merger.take()).isEqualTo(new SimpleMergeable("cd"));
        assertThat(merger.take()).isNull();
    }

    private record PlainMergeable(String name) implements Mergeable<PlainMergeable> {
        @Override
        public PlainMergeable merge(PlainMergeable update) {
            return new PlainMergeable(name + update.name());
        }
    }

    @Test
    public void updatesMergeWithLaterOnesUnlessTheySayOtherwise() {
        MergingStateBacklog<PlainMergeable> merger = MergingStateBacklog.<PlainMergeable>factory().build();
        merger.put(new PlainMergeable("a"));
        merger.put(new PlainMergeable("b"));
        assertThat(merger.take()).isEqualTo(new PlainMergeable("ab"));
    }

}
