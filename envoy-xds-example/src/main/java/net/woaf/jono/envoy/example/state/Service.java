package net.woaf.jono.envoy.example.state;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;

/**
 * A very simple representation of a service. In our example a service (cluster) has a name, and a set of endpoints
 *
 * @param name the service (cluster) name
 * @param endpoints where it runs
 */
public record Service(String name, ImmutableSet<HostAndPort> endpoints) {

    public Service(String name, Iterable<HostAndPort> endpoints) {
        this(name, ImmutableSet.copyOf(endpoints));
    }
}
