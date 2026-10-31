package net.woaf.jono.envoy.xds.internal.delta;

import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import net.woaf.jono.envoy.xds.internal.delta.SubListSubManager;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("Duplicates")
public class SubListSubManagerSpec {

    @Test
    public void testSubscribeTwice() {
        SubListSubManager subManager = new SubListSubManager();

        Predicate<String> filter = subManager.processResourceListChange(ImmutableSet.of("foo"), ImmutableSet.of()).orElseThrow();

        assertThat(filter.test("foo")).isTrue();
        assertThat(filter.test("bar")).isFalse();
        assertThat(filter.test("baz")).isFalse();

        // xDS spec says envoy can sub twice if it has e.g. forgotten the resource for some reason
        // If this happens the spec says we should send the current state.
        filter = subManager.processResourceListChange(ImmutableSet.of("foo", "bar"), ImmutableSet.of()).orElseThrow();

        assertThat(filter.test("foo")).isTrue();
        assertThat(filter.test("bar")).isTrue();
        assertThat(filter.test("baz")).isFalse();

        assertThat(subManager.isSubscribedTo("foo")).isTrue();
        assertThat(subManager.isSubscribedTo("bar")).isTrue();
        assertThat(subManager.isSubscribedTo("baz")).isFalse();
    }
    
    @Test
    public void testSubscribeRemove() {
        SubListSubManager subManager = new SubListSubManager();

        Predicate<String> filter = subManager.processResourceListChange(ImmutableSet.of("foo", "bar"), ImmutableSet.of()).orElseThrow();

        assertThat(filter.test("foo")).isTrue();
        assertThat(filter.test("bar")).isTrue();
        assertThat(filter.test("baz")).isFalse();

        // Unsubscribing needs no response, so no filter returned.
        assertThat(subManager.processResourceListChange(ImmutableSet.of(), ImmutableSet.of("foo"))).isNotPresent();

        assertThat(subManager.isSubscribedTo("foo")).isFalse();
        assertThat(subManager.isSubscribedTo("bar")).isTrue();
        assertThat(subManager.isSubscribedTo("baz")).isFalse();
    }

    @Test
    public void testSubscribeToEmpty() {
        SubListSubManager subManager = new SubListSubManager();

        // Nothing changes, so no filter returned.
        assertThat(subManager.processResourceListChange(ImmutableSet.of(), ImmutableSet.of())).isNotPresent();

        assertThat(subManager.isSubscribedTo("foo")).isFalse();
    }

    @Test
    public void testSubscribeUnsubscribeSameResourceThrows() {
        SubListSubManager subManager = new SubListSubManager();

        assertThatThrownBy(() -> subManager.processResourceListChange(ImmutableSet.of("foo"), ImmutableSet.of("foo")))
            .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

}
