package net.woaf.jono.envoy.xds.internal;

import net.woaf.jono.envoy.xds.TypeUrl;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class DiscoveryServiceOrderer {

    public static <T extends Message, StateUpdT> List<DiscoveryService<T, StateUpdT>> sort(List<TypeUrl> order, Map<TypeUrl, DiscoveryService<T, StateUpdT>> discoveryServices) {
        List<DiscoveryService<T, StateUpdT>> orderedDiscoveryServices = new ArrayList<>(order.size());
        // Skip types with no service: the manager reads the end of the list as the end of a phase.
        for (TypeUrl t : order) {
            DiscoveryService<T, StateUpdT> service = discoveryServices.get(t);
            if (service != null) {
                orderedDiscoveryServices.add(service);
            }
        }
        return orderedDiscoveryServices;
    }

}
