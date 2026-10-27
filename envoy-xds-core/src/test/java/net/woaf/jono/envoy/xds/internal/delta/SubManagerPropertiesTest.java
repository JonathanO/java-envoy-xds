package net.woaf.jono.envoy.xds.internal.delta;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import dev.hegel.HegelTest;
import dev.hegel.TestCase;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static dev.hegel.Generators.booleans;
import static dev.hegel.Generators.integers;
import static dev.hegel.Generators.sampledFrom;
import static dev.hegel.Generators.sets;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Delta sub managers against a model of the xDS spec's subscription rules, over random request sequences.
 * <a href="https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#how-the-client-specifies-what-resources-to-return">How
 * the client specifies what resources to return</a>
 */
public class SubManagerPropertiesTest {

    private static final String WILDCARD = "*";

    private static final List<String> NAMES = List.of("a", "b", "c");

    /** What the spec says the client is subscribed to: the wildcard and the names it asked for are tracked apart. */
    private static final class Model {
        private final boolean wildcardType;
        private boolean first = true;
        private boolean wildcard = false;
        private final Set<String> names = new HashSet<>();

        Model(boolean wildcardType) {
            this.wildcardType = wildcardType;
        }

        void apply(Set<String> subscribe, Set<String> unsubscribe) {
            boolean legacyWildcard = wildcardType && first && unsubscribe.isEmpty() && subscribe.isEmpty();
            first = false;
            if (legacyWildcard) {
                wildcard = true;
                return;
            }
            wildcard = (wildcard || subscribe.contains(WILDCARD)) && !unsubscribe.contains(WILDCARD);
            names.addAll(withoutWildcard(subscribe));
            names.removeAll(unsubscribe);
        }

        Set<String> subscribed() {
            return wildcard ? Set.copyOf(NAMES) : Set.copyOf(names);
        }
    }

    private record Step(Set<String> subscribe, Set<String> before, Set<String> after, boolean startedWildcard,
                        Optional<Predicate<String>> answer, SubManager manager) {
    }

    /** Send the manager a random request sequence, checking each step. */
    private static void run(TestCase tc, Consumer<Step> check) {
        boolean wildcardType = tc.draw(booleans(), "wildcardType");
        SubManager manager = wildcardType ? new WildcardSubManager() : new SubListSubManager();
        Model model = new Model(wildcardType);
        List<String> names = wildcardType ? List.of("a", "b", "c", WILDCARD) : NAMES;

        int requests = tc.draw(integers().min(1).max(8), "requests");
        for (int i = 0; i < requests; i++) {
            Set<String> subscribe = tc.draw(sets(sampledFrom(names)), "subscribe");
            // Subscribing to and unsubscribing from one name in one request is rejected; see SubListSubManagerSpec.
            Set<String> unsubscribe = Sets.difference(tc.draw(sets(sampledFrom(names)), "unsubscribe"), subscribe);

            Set<String> before = model.subscribed();
            boolean wasWildcard = model.wildcard;
            Optional<Predicate<String>> answer = manager.processResourceListChange(ImmutableSet.copyOf(subscribe), ImmutableSet.copyOf(unsubscribe));
            model.apply(subscribe, unsubscribe);
            check.accept(new Step(subscribe, before, model.subscribed(), model.wildcard && !wasWildcard, answer, manager));
        }
    }

    private static Set<String> withoutWildcard(Set<String> names) {
        return names.stream().filter(n -> !n.equals(WILDCARD)).collect(Collectors.toSet());
    }

    @HegelTest
    void subscribedToWhatTheSpecSays(TestCase tc) {
        run(tc, step -> {
            assertThat(NAMES.stream().filter(step.manager()::isSubscribedTo)).containsExactlyInAnyOrderElementsOf(step.after());
            assertThat(step.manager().isSubscribedTo(WILDCARD)).isFalse();
        });
    }

    @HegelTest
    void answersExactlyRequestsThatNameResourcesOrStartTheWildcard(TestCase tc) {
        // Unsubscribing needs no answer, and an empty one would hold up pushes until Envoy ACKs it.
        run(tc, step -> assertThat(step.answer().isPresent())
            .isEqualTo(!withoutWildcard(step.subscribe()).isEmpty() || step.startedWildcard()));
    }

    @HegelTest
    void answerCoversNamesAskedForAndNewlySubscribed(TestCase tc) {
        // The spec requires an answer for each name subscribed to, even one we already sent: removed_resources if it
        // doesn't exist.
        run(tc, step -> step.answer().ifPresent(answer -> assertThat(answer)
            .acceptsAll(withoutWildcard(step.subscribe()))
            .acceptsAll(Sets.difference(step.after(), step.before()))));
    }

    @HegelTest
    void answerOnlyCoversSubscribedNames(TestCase tc) {
        run(tc, step -> step.answer().ifPresent(answer -> assertThat(answer)
            .rejectsAll(Sets.difference(Set.copyOf(NAMES), step.after()))
            .rejects(WILDCARD)));
    }
}
