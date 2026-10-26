package net.woaf.jono.envoy.xds.internal.sotw;

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
import static dev.hegel.Generators.lists;
import static dev.hegel.Generators.sampledFrom;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * SotW sub managers against a model of the xDS spec's subscription rules, over random request sequences.
 * <a href="https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#how-the-client-specifies-what-resources-to-return">How
 * the client specifies what resources to return</a>
 */
public class SubManagerPropertiesTest {

    private static final String WILDCARD = "*";

    private static final List<String> NAMES = List.of("a", "b", "c");

    /** What the spec says the client is subscribed to: each request lists everything it wants. */
    private static final class Model {
        private final boolean wildcardType;
        private boolean sentNames = false;
        private boolean wildcard = false;
        private Set<String> names = Set.of();

        Model(boolean wildcardType) {
            this.wildcardType = wildcardType;
        }

        void apply(List<String> resourceNames) {
            if (wildcardType) {
                // An empty list is the legacy wildcard, until the client has sent names: then it means nothing.
                wildcard = resourceNames.contains(WILDCARD) || (resourceNames.isEmpty() && !sentNames);
                sentNames |= !resourceNames.isEmpty();
            }
            names = resourceNames.stream().filter(n -> !n.equals(WILDCARD)).collect(Collectors.toSet());
        }

        Set<String> subscribed() {
            return wildcard ? Set.copyOf(NAMES) : names;
        }
    }

    private record Step(boolean changed, Set<String> before, Set<String> after, Optional<Predicate<String>> answer,
                        SubManager manager) {
    }

    /** Send the manager a random request sequence, checking each step. */
    private static void run(TestCase tc, Consumer<Step> check) {
        boolean wildcardType = tc.draw(booleans(), "wildcardType");
        SubManager manager = wildcardType ? new WildcardSubManager() : new SubListSubManager();
        Model model = new Model(wildcardType);
        List<String> names = wildcardType ? List.of("a", "b", "c", WILDCARD) : NAMES;

        int requests = tc.draw(integers().min(1).max(8), "requests");
        for (int i = 0; i < requests; i++) {
            // A repeated field: nothing stops a client listing a name twice.
            List<String> resourceNames = tc.draw(lists(sampledFrom(names)).maxSize(5), "resourceNames");

            Set<String> before = model.subscribed();
            boolean wasWildcard = model.wildcard;
            Set<String> namesBefore = model.names;
            Optional<Predicate<String>> answer = manager.processResourceListChange(resourceNames);
            model.apply(resourceNames);
            boolean changed = model.wildcard != wasWildcard || !model.names.equals(namesBefore);
            check.accept(new Step(changed, before, model.subscribed(), answer, manager));
        }
    }

    @HegelTest
    void subscribedToWhatTheSpecSays(TestCase tc) {
        run(tc, step -> assertThat(NAMES.stream().filter(step.manager()::isSubscribedTo)).containsExactlyInAnyOrderElementsOf(step.after()));
    }

    @HegelTest
    void answersEveryChangedSubscription(TestCase tc) {
        // Without an answer we neither drop the resources unsubscribed from nor fetch the new ones.
        run(tc, step -> assertThat(step.answer().isPresent() || !step.changed()).isTrue());
    }

    @HegelTest
    void answerCoversNewlySubscribedNames(TestCase tc) {
        run(tc, step -> step.answer().ifPresent(answer -> assertThat(answer)
            .acceptsAll(Sets.difference(step.after(), step.before()))));
    }

    @HegelTest
    void answerOnlyCoversSubscribedNames(TestCase tc) {
        run(tc, step -> step.answer().ifPresent(answer -> assertThat(answer)
            .rejectsAll(Sets.difference(new HashSet<>(NAMES), step.after()))));
    }
}
