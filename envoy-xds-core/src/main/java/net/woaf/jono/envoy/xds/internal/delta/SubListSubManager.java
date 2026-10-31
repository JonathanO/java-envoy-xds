package net.woaf.jono.envoy.xds.internal.delta;

import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

class SubListSubManager implements SubManager {

    private final Set<String> subs = new HashSet<>();

    @Override
    public Optional<Predicate<String>> processResourceListChange(ImmutableSet<String> resourceNamesSubscribe, ImmutableSet<String> resourceNamesUnsubscribe) {
        Set<String> newSubs = new HashSet<>(resourceNamesSubscribe);

        for (String name: resourceNamesSubscribe) {
            subs.add(name);
            if (resourceNamesUnsubscribe.contains(name)) {
                // This should never happen, but if it does the xDS spec provides no guidance on correct behaviour.
                // Both possible options are potentially bad, so it seems best if this is an error. It's Envoy's, so the
                // stream ends with INVALID_ARGUMENT.
                throw Status.INVALID_ARGUMENT.withDescription("Attempt to subscribe and unsubscribe from the same resource in one request: " + name)
                    .asRuntimeException();
            }
        }
        subs.removeAll(resourceNamesUnsubscribe);
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
