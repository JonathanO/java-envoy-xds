package net.woaf.jono.envoy.xds.internal.sotw;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

class SubListSubManager implements SubManager {

    private static final Logger log = LoggerFactory.getLogger(SubListSubManager.class);

    private Set<String> subs = new HashSet<>();

    @VisibleForTesting
    Set<String> getSubs() {
        return subs;
    }

    @Override
    public Optional<Predicate<String>> processResourceListChange(List<String> resourceNames) {
        Set<String> next = new HashSet<>(resourceNames);
        if (next.equals(subs)) {
            return Optional.empty();
        }
        log.debug("Subscribed to {}", resourceNames);
        Set<String> newSubs = Sets.difference(next, subs).immutableCopy();
        subs = next;
        return Optional.of(newSubs::contains);
    }

    @Override
    public boolean isSubscribedTo(String resourceName) {
        return subs.contains(resourceName);
    }
}
