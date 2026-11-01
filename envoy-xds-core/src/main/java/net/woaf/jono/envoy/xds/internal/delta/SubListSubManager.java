package net.woaf.jono.envoy.xds.internal.delta;

import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

class SubListSubManager implements SubManager {

    private final Set<String> subs = new HashSet<>();

    @Override
    public Optional<Predicate<String>> processResourceListChange(ImmutableSet<String> resourceNamesSubscribe, ImmutableSet<String> resourceNamesUnsubscribe) {
        Set<String> newSubs = new HashSet<>(resourceNamesSubscribe);

        // A name in both is subscribed to: the xDS spec doesn't say which wins, and a resource Envoy doesn't want does
        // less harm than missing one it does.
        subs.removeAll(resourceNamesUnsubscribe);
        subs.addAll(resourceNamesSubscribe);
        if (newSubs.isEmpty()) {
            // Unsubscribing needs no response, and sending an empty one would hold up pushes until Envoy ACKs it.
            return Optional.empty();
        }
        return Optional.of(newSubs::contains);
    }

    @Override
    public boolean isSubscribedTo(String resourceName) {
        return subs.contains(resourceName);
    }
}
