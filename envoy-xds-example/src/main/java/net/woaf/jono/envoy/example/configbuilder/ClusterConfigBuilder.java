package net.woaf.jono.envoy.example.configbuilder;

import com.google.protobuf.Duration;
import net.woaf.jono.envoy.example.config.ClientConfig;
import net.woaf.jono.envoy.example.state.Service;
import net.woaf.jono.envoy.example.state.SimpleUpdate;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.AggregatedConfigSource;
import io.envoyproxy.envoy.config.core.v3.ApiVersion;
import io.envoyproxy.envoy.config.core.v3.ConfigSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

public class ClusterConfigBuilder extends AbstractConfigBuilder<Cluster> {

    @Override
    public Response<Cluster> addOrder(SimpleUpdate diff, Predicate<String> resourceInSubListChange, ClientConfig clientDetails) {
        List<NamedMessage<Cluster>> addAndUpdates = new ArrayList<>();

        for (Map.Entry<String, Service> entry : diff.after().entrySet()) {
            if (diff.before().containsKey(entry.getKey())) {
                // In this very simple example there are no properties of a Service that can change the Clusters we generated.
                // So if Service already existed, there is nothing to do.
                continue;
            }
            if (!resourceInSubListChange.test(entry.getKey())) {
                // In this example the output resource name == the service name, and only one Cluster can be generated for each Service.
                // This means it's safe for us to pre-apply resourceInSubListChange here.
                // In our real control plane we often generate multiple named Clusters for one Service, so we have to be more careful than this!
                continue;
            }
            addAndUpdates.add(clusterForService(entry.getValue(), clientDetails));
        }

        return new Response<>(addAndUpdates, List.of());
    }

    @Override
    public Response<Cluster> removeOrder(SimpleUpdate diff, Predicate<String> resourceInSubListChange, ClientConfig clientDetails) {
        List<String> removes = new ArrayList<>();

        for (Map.Entry<String, Service> entry : diff.before().entrySet()) {
            // Similar to above, in this simple example we only generate a single cluster per service, and it has the same name as the service. This makes removal very simple.
            if (!resourceInSubListChange.test(entry.getKey())) {
                continue;
            }
            if (!diff.after().containsKey(entry.getKey())) {
                removes.add(entry.getKey());
            }
        }

        return new Response<>(List.of(), removes);
    }


    @Override
    public Resources<Cluster> getResources(Collection<Service> services, Predicate<String> resourceInSubListChange, ClientConfig clientDetails) {
        List<NamedMessage<Cluster>> resources = new ArrayList<>();

        for (Service service : services) {
            // Similar to above, in this simple example we only generate a single cluster per service, and it has the same name as the service.
            if (!resourceInSubListChange.test(service.name())) {
                continue;
            }
            resources.add(clusterForService(service, clientDetails));
        }

        return new Resources<>(resources);
    }

    private NamedMessage<Cluster> clusterForService(Service service, ClientConfig unusedClientDetails) {
        // Build the appropriate cluster for our service for this particular client.
        // In our example this is really simple, and doesn't depend on anything from the client config.
        return NamedMessage.of(Cluster.newBuilder()
            .setName(service.name())
            .setType(Cluster.DiscoveryType.EDS)
            .setConnectTimeout(Duration.newBuilder().setSeconds(1).build())
            .setEdsClusterConfig(Cluster.EdsClusterConfig.newBuilder()
                .setEdsConfig(
                    ConfigSource.newBuilder()
                        .setResourceApiVersion(ApiVersion.V3)
                        .setAds(AggregatedConfigSource.newBuilder().build())
                        .build()
                )
                .build())
            .build());
    }


    @Override
    public Class<Cluster> handlesType() {
        return Cluster.class;
    }
}
