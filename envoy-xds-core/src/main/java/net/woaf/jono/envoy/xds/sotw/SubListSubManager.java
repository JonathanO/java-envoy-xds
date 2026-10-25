package net.woaf.jono.envoy.xds.sotw;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
class SubListSubManager implements SubManager {

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
    public <X> Map<String, X> filterSubs(Map<String, X> things) {
        return things.entrySet().stream().filter((e) -> subs.contains(e.getKey())).collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                )
        );
    }

    @Override
    public boolean isSubscribedTo(String resourceName) {
        return subs.contains(resourceName);
    }
}
