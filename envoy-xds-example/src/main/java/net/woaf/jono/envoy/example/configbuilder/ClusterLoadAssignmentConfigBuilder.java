package net.woaf.jono.envoy.example.configbuilder;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import net.woaf.jono.envoy.example.config.ClientConfig;
import net.woaf.jono.envoy.example.state.Service;
import net.woaf.jono.envoy.example.state.SimpleUpdate;
import io.envoyproxy.envoy.config.core.v3.Address;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.endpoint.v3.Endpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LbEndpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LocalityLbEndpoints;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ClusterLoadAssignmentConfigBuilder extends AbstractConfigBuilder<ClusterLoadAssignment> {

    @Override
    public Response<ClusterLoadAssignment> addOrder(SimpleUpdate diff, Predicate<String> resourceInSubListChange, ClientConfig clientDetails) {
        // We're going to assume that endpoints should not be removed in add order, and we have to combine old and new together.
        // This may or may not make any sense, but it provides a more complex example.

        List<NamedMessage<ClusterLoadAssignment>> addAndUpdates = new ArrayList<>();

        for (Map.Entry<String, Service> entry : diff.after().entrySet()) {
            if (!resourceInSubListChange.test(entry.getKey())) {
                // This simple example only produces 1 resource per service, named after the service, so this check is easy.
                continue;
            }

            Service before = diff.before().get(entry.getKey());
            Service after = entry.getValue();

            if (before == null) {
                // Cluster is being added
                addAndUpdates.add(clusterLoadAssignmentForService(entry.getValue(), clientDetails));
                continue;
            }

            if (!before.endpoints().containsAll(after.endpoints())) {
                // Endpoints are being added, combine the two sets.
                addAndUpdates.add(clusterLoadAssignmentForService(combined(before, after), clientDetails));
            }
        }

        return new Response<>(addAndUpdates, List.of());
    }

    @Override
    public Response<ClusterLoadAssignment> removeOrder(SimpleUpdate diff, Predicate<String> resourceInSubListChange, ClientConfig clientDetails) {
        // We're going to assume that all endpoint removals happen in remove order.
        List<NamedMessage<ClusterLoadAssignment>> addAndUpdates = new ArrayList<>();
        List<String> removes = new ArrayList<>();

        for (Map.Entry<String, Service> entry : diff.before().entrySet()) {

            if (!resourceInSubListChange.test(entry.getKey())) {
                // This simple example only produces 1 resource per service, named after the service, so this check is easy.
                continue;
            }

            Service before = entry.getValue();
            Service after = diff.after().get(entry.getKey());

            if (after == null) {
                // Cluster is being removed entirely
                removes.add(entry.getKey());
                continue;
            }

            if (!after.endpoints().containsAll(before.endpoints())) {
                // Some endpoints got removed, we need to inform envoy since in add order we sent the union of old and new.
                addAndUpdates.add(clusterLoadAssignmentForService(after, clientDetails));
            }
        }

        return new Response<>(addAndUpdates, removes);
    }

    @Override
    public Resources<ClusterLoadAssignment> getResourcesAddOrder(SimpleUpdate services, Predicate<String> resourceInSubListChange, ClientConfig clientDetails) {
        // Mid-change, before remove order, Envoy must get what addOrder sent: a service's old endpoints alongside its new ones.
        // Answering with only the new ones would drop the old ones early, e.g. when Envoy re-asks for a cluster's endpoints after its CDS update.
        List<Service> preRemove = new ArrayList<>();
        for (Service service : services.getServicesPreRemove()) {
            Service before = services.before().get(service.name());
            preRemove.add(before == null ? service : combined(before, service));
        }
        return getResources(preRemove, resourceInSubListChange, clientDetails);
    }

    /** A service with its endpoints both before and after a change. */
    private static Service combined(Service before, Service after) {
        return new Service(after.name(), ImmutableSet.<HostAndPort>builder()
            .addAll(after.endpoints())
            .addAll(before.endpoints())
            .build());
    }

    private NamedMessage<ClusterLoadAssignment> clusterLoadAssignmentForService(Service service, ClientConfig unusedClientDetails) {
        return NamedMessage.of(ClusterLoadAssignment.newBuilder()
            .setClusterName(service.name())
            .addEndpoints(LocalityLbEndpoints.newBuilder()
                .addAllLbEndpoints(service.endpoints().stream().map(e -> LbEndpoint.newBuilder()
                    .setEndpoint(Endpoint.newBuilder()
                        .setAddress(Address.newBuilder()
                            .setSocketAddress(SocketAddress.newBuilder()
                                .setProtocol(SocketAddress.Protocol.TCP)
                                .setAddress(e.getHost())
                                .setPortValue(e.getPortOrDefault(80))
                                .build())
                            .build())
                        .build())
                    .build()).collect(Collectors.toSet()))
                .build())
            .build());
    }

    @Override
    public Resources<ClusterLoadAssignment> getResources(Collection<Service> services, Predicate<String> resourceInSubListChange, ClientConfig clientDetails) {
        // In this toy example the output resource name == the service name, so we can pre-apply resourceInSubListChange. This might not be true in a real service mesh, so don't assume you can do this, think about it!

        List<NamedMessage<ClusterLoadAssignment>> resources = new ArrayList<>();
        for (Service service : services) {
            if (!resourceInSubListChange.test(service.name())) {
                // This simple example only produces 1 resource per service, named after the service, so this check is easy.
                continue;
            }

            resources.add(clusterLoadAssignmentForService(service, clientDetails));
        }
        return new Resources<>(resources);
    }

    @Override
    public Class<ClusterLoadAssignment> handlesType() {
        return ClusterLoadAssignment.class;
    }
}
