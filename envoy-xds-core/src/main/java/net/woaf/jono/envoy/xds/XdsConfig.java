package net.woaf.jono.envoy.xds;

import com.google.common.collect.ImmutableSet;
import org.jspecify.annotations.Nullable;
import java.util.Set;

/**
 * This represents the metadata and configuration of the controlplane for a specific client. Start from
 * {@link #of(Object)}.
 *
 * @param clientDetails Whatever details about the client and configuration that you need to pass to your
 *     ConfigBuilders.
 * @param delayUpdatesUntilAckOf If set then we will delay sending mesh updates to the client until we have received
 *     the first ACK for this TypeUrl. This allows us to prevent clients being sent endpoint updates, which interfere with
 *     the envoy init process, until they have acked a later DS (for example RDS or LDS, which are only ACKed by envoy
 *     after clusters have been fully initialised.) If null then we don't delay the updates. This is useful, for example,
 *     to work around bugs like https://github.com/envoyproxy/envoy/issues/16035 (we set this option to RDS in our own
 *     control plane for this reason.) Only set it to a type this client subscribes to. If subscribesTo leaves the type
 *     out, it's ignored; otherwise, with no ACK of it after 30 seconds, updates go anyway, with a warning, at the next
 *     message or change.
 * @param subscribesTo The types this client gets from us, or null (the default) for every type we serve. After a
 *     reconnect, we only remove a resource Envoy kept once the types that may still refer to it have been answered and
 *     ACKed, e.g. a cluster after the route configs. Leave out types this client doesn't subscribe to, e.g. RDS for an
 *     Envoy with static routes, or its stale removals wait for them, up to 30 seconds.
 * @param <DetailsT> Client details type
 */
public record XdsConfig<DetailsT>(DetailsT clientDetails, @Nullable TypeUrl delayUpdatesUntilAckOf, @Nullable Set<TypeUrl> subscribesTo) {

    /**
     * Copies subscribesTo, so later changes to the set you passed don't change the config.
     */
    public XdsConfig {
        subscribesTo = subscribesTo == null ? null : ImmutableSet.copyOf(subscribesTo);
    }

    /**
     * Config with no delay, for a client that gets every type we serve.
     * @param clientDetails passed to your ConfigBuilders
     * @param <DetailsT> Client details type
     * @return the config
     */
    public static <DetailsT> XdsConfig<DetailsT> of(DetailsT clientDetails) {
        return new XdsConfig<>(clientDetails, null, null);
    }

    /**
     * {@return this config, delaying updates until that type is ACKed}
     * @param typeUrl see {@link #delayUpdatesUntilAckOf()}
     */
    public XdsConfig<DetailsT> withDelayUpdatesUntilAckOf(@Nullable TypeUrl typeUrl) {
        return new XdsConfig<>(clientDetails, typeUrl, subscribesTo);
    }

    /**
     * {@return this config, for a client that gets these types from us}
     * @param types see {@link #subscribesTo()}
     */
    public XdsConfig<DetailsT> withSubscribesTo(@Nullable Set<TypeUrl> types) {
        return new XdsConfig<>(clientDetails, delayUpdatesUntilAckOf, types);
    }
}
