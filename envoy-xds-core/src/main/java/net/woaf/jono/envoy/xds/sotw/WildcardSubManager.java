package net.woaf.jono.envoy.xds.sotw;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class WildcardSubManager implements SubManager {

    private static final Logger log = LoggerFactory.getLogger(WildcardSubManager.class);

    private static final String WILDCARD = "*";

    private boolean subscribed = false;
    private boolean sentNames = false;

    // The spec allows wildcard types to be used with explicit names too, so they're tracked as a sublist for when the
    // client leaves wildcard mode.
    private final SubListSubManager subListSubManager = new SubListSubManager();

    @VisibleForTesting
    boolean isSubscribed() {
        return subscribed;
    }

    @Override
    public Optional<Predicate<String>> processResourceListChange(List<String> resourceNames) {
        // An empty list is a (legacy) wildcard subscription, unless the client has ever sent names: then it means
        // unsubscribe from everything.
        boolean wildcard = resourceNames.contains(WILDCARD) || (resourceNames.isEmpty() && !sentNames);
        sentNames |= !resourceNames.isEmpty();
        Optional<Predicate<String>> subListChange = subListSubManager.processResourceListChange(
            resourceNames.stream().filter(n -> !n.equals(WILDCARD)).collect(Collectors.toList()));

        boolean wasSubscribed = subscribed;
        subscribed = wildcard;
        if (subscribed && !wasSubscribed) {
            log.debug("Subscribed to *");
            return Optional.of(name -> true);
        }
        if (wasSubscribed && !subscribed) {
            log.debug("Unsubscribed from *");
            // The caller drops everything no longer subscribed to; the remaining names are already known.
            return Optional.of(subListChange.orElse(name -> false));
        }
        return subListChange;
    }

    @Override
    public <X> Map<String, X> filterSubs(Map<String, X> things) {
        if (!subscribed) {
            return subListSubManager.filterSubs(things);
        }
        return things;
    }

    @Override
    public boolean isSubscribedTo(String resourceName) {
        return subscribed || subListSubManager.isSubscribedTo(resourceName);
    }
}
