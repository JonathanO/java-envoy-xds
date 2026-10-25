package net.woaf.jono.envoy.xds;

import com.google.protobuf.Message;
import java.util.function.Predicate;
import net.woaf.jono.envoy.xds.api.IncrementalConfigBuilder;

/**
 * Stands in for a type we have no config builder for: it never has any resources.
 */
public final class NoOpConfigBuilder<ResourceT extends Message, StateUpdT, DetailsT> implements IncrementalConfigBuilder<ResourceT, StateUpdT, DetailsT> {

    private final Class<ResourceT> type;

    public NoOpConfigBuilder(Class<ResourceT> type) {
        this.type = type;
    }

    @Override
    public Response<ResourceT> addOrder(StateUpdT diff, Predicate<String> resourceInSubListChange, DetailsT clientDetails) {
        return Response.empty();
    }

    @Override
    public Response<ResourceT> removeOrder(StateUpdT diff, Predicate<String> resourceInSubListChange, DetailsT clientDetails) {
        return Response.empty();
    }

    @Override
    public Resources<ResourceT> getResourcesAddOrder(StateUpdT services, Predicate<String> resourceInSubListChange, DetailsT clientDetails) {
        return Resources.empty();
    }

    @Override
    public Resources<ResourceT> getResourcesRemoveOrder(StateUpdT services, Predicate<String> resourceInSubListChange, DetailsT clientDetails) {
        return Resources.empty();
    }

    @Override
    public Class<ResourceT> handlesType() {
        return type;
    }
}
