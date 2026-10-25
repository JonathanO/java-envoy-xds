package net.woaf.jono.envoy.testing.matchers;

import org.jspecify.annotations.Nullable;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import java.util.List;
import java.util.Optional;
import net.woaf.jono.envoy.testing.XdsTypes;

/**
 * A recorded xDS message, the same whether it was SotW or delta, for {@link XdsMatchers} to match on.
 */
public sealed interface XdsMessage {

    /**
     * {@return the message's type URL}
     */
    String typeUrl();

    /**
     * A message from Envoy.
     * @param typeUrl its type
     * @param responseNonce the nonce of the response it answers, or empty
     * @param subscribe names it subscribes to: delta's subscribe list, or SotW's whole resource name list
     * @param unsubscribe names it unsubscribes from: delta only, SotW requests carry the whole list instead
     * @param errorMessage why Envoy rejected the response it answers, or null if it didn't
     */
    record Request(String typeUrl, String responseNonce, List<String> subscribe, List<String> unsubscribe, @Nullable String errorMessage) implements XdsMessage {

        /**
         * {@return whether Envoy rejected the response this answers}
         */
        public boolean isNack() {
            return errorMessage != null;
        }
    }

    /**
     * A message to Envoy.
     * @param typeUrl its type
     * @param nonce its nonce
     * @param resources the resources it carries, unpacked if their type is known
     * @param removed names it removes: delta only, see {@link XdsMatchers#removing}
     * @param delta whether it is a delta response
     */
    record Response(String typeUrl, String nonce, List<Message> resources, List<String> removed, boolean delta) implements XdsMessage {
    }

    /**
     * View a recorded message.
     * @param message a DiscoveryRequest, DiscoveryResponse, DeltaDiscoveryRequest or DeltaDiscoveryResponse
     * @return the view, or empty for anything else
     */
    static Optional<XdsMessage> of(@Nullable Object message) {
        if (message instanceof DiscoveryRequest r) {
            return Optional.of(new Request(r.getTypeUrl(), r.getResponseNonce(), r.getResourceNamesList(), List.of(),
                r.hasErrorDetail() ? r.getErrorDetail().getMessage() : null));
        }
        if (message instanceof DeltaDiscoveryRequest r) {
            return Optional.of(new Request(r.getTypeUrl(), r.getResponseNonce(), r.getResourceNamesSubscribeList(), r.getResourceNamesUnsubscribeList(),
                r.hasErrorDetail() ? r.getErrorDetail().getMessage() : null));
        }
        if (message instanceof DiscoveryResponse r) {
            return Optional.of(new Response(r.getTypeUrl(), r.getNonce(), unpack(r.getResourcesList()), List.of(), false));
        }
        if (message instanceof DeltaDiscoveryResponse r) {
            return Optional.of(new Response(r.getTypeUrl(), r.getNonce(), unpack(r.getResourcesList().stream().map(Resource::getResource).toList()),
                r.getRemovedResourcesList(), true));
        }
        return Optional.empty();
    }

    private static List<Message> unpack(List<Any> resources) {
        ImmutableList.Builder<Message> unpacked = ImmutableList.builder();
        for (Any any : resources) {
            Class<? extends Message> type = XdsTypes.messageClass(any.getTypeUrl());
            try {
                unpacked.add(type == null ? any : any.unpack(type));
            } catch (InvalidProtocolBufferException e) {
                unpacked.add(any);
            }
        }
        return unpacked.build();
    }
}
