package net.woaf.jono.envoy.testing.assertions;

import static net.woaf.jono.envoy.testing.configdump.EnvoyAdminClient.sneakyUnpack;

import com.google.common.base.Preconditions;
import org.jspecify.annotations.Nullable;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import net.woaf.jono.envoy.testing.XdsTypes;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.assertj.core.api.AssertFactory;
import org.assertj.core.api.InstanceOfAssertFactory;

/**
 * Checks on a recorded SotW xDS conversation.
 */
public class SotwConversationAssert extends EnvoyAssert<SotwConversationAssert> {


    /**
     * For AssertJ's {@code asInstanceOf} and {@code extracting}.
     */
    public static final InstanceOfAssertFactory<Conversation, SotwConversationAssert> SOTW_CONVERSATION = new InstanceOfAssertFactory<>(Conversation.class, new Factory());

    /**
     * Use {@link #assertThat}.
     * @param actual the conversation
     */
    protected SotwConversationAssert(Conversation actual) {
        super(actual, SotwConversationAssert.class);
    }

    /**
     * {@return checks on this conversation}
     * @param actual the conversation
     */
    public static SotwConversationAssert assertThat(Conversation actual) {
        return new SotwConversationAssert(actual);
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert allWereAcked() {
        isNotNull();
        SetMultimap<String, String> sentNonces = HashMultimap.create();
        SetMultimap<String, String> unackedNonces = HashMultimap.create();
        Map<String, String> staleNonces = new HashMap<>(); // by type: a nonce Envoy echoed from its previous stream

        for (Object message : actual.getMessages()) {
            if (message instanceof DiscoveryRequest r) {
                // TODO: We probably should check versions for SOTW...
                // TODO: Don't accept old nonces once envoy has acked newer ones...
                if (Strings.emptyToNull(r.getResponseNonce()) != null) {
                    if (!sentNonces.containsKey(r.getTypeUrl())) {
                        // If envoy is reconnecting it may reuse nonces from its previous connection.
                        sentNonces.put(r.getTypeUrl(), r.getResponseNonce());
                        staleNonces.put(r.getTypeUrl(), r.getResponseNonce());
                    } else {
                        if (!sentNonces.containsEntry(r.getTypeUrl(), r.getResponseNonce())) {
                            failWithMessage("Found unexpected response nonce <%s> for <%s>, expected one of <%s>", r.getResponseNonce(), r.getTypeUrl(), sentNonces.get(r.getTypeUrl()));
                        }
                    }
                    unackedNonces.remove(r.getTypeUrl(), r.getResponseNonce());
                }
                if (r.hasErrorDetail()) {
                    failWithMessage("Envoy sent error <%s>", r.getErrorDetail().getMessage());
                }
            } else if (message instanceof DiscoveryResponse r) {
                if (r.getNonce().equals(staleNonces.get(r.getTypeUrl()))) {
                    failWithMessage("Sent nonce <%s> for <%s>, which Envoy had echoed from its previous stream: its requests can't be told"
                        + " from ACKs of this response. Make nonces unique across streams", r.getNonce(), r.getTypeUrl());
                }
                if (!sentNonces.put(r.getTypeUrl(), r.getNonce())) {
                    failWithMessage("Sent nonce <%s> for <%s> multiple times", r.getNonce(), r.getTypeUrl());
                }
                unackedNonces.put(r.getTypeUrl(), r.getNonce());
            } else {
                failWithMessage("Expected message to be of type DiscoveryRequest or DiscoveryResponse, but found <%s>", message.getClass());
            }
        }
        if (!unackedNonces.isEmpty()) {
            failWithMessage("Found unacked nonces <%s>", unackedNonces);
        }
        return this;
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert nothingSentUnaskedPre119Reconnect() {
        return nothingSentUnasked(true);
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert nothingSentUnasked() {
        return nothingSentUnasked(false);
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert nothingSentUnasked(boolean allowBrokenReconnectWildcardBehaviourPre119) {
        isNotNull();

        HashMultimap<String, String> subs = HashMultimap.create();
        HashSet<String> wildcards = new HashSet<>();
        HashSet<String> seen = new HashSet<>();
        HashSet<String> seenSubs = new HashSet<>();

        for (Object message : actual.getMessages()) {
            if (message instanceof DiscoveryRequest r) {
                Set<String> requestedSubs = Set.copyOf(r.getResourceNamesList());
                if (seen.add(r.getTypeUrl()) && (XdsTypes.isWildcard(r.getTypeUrl()) && allowBrokenReconnectWildcardBehaviourPre119)) {
                    // First request, allow bad behaviour from old versions.
                    requestedSubs = Set.of();
                }
                if (requestedSubs.isEmpty() && !seenSubs.contains(r.getTypeUrl())) {
                    // Old wildcard behaviour
                    wildcards.add(r.getTypeUrl());
                } else {
                    seenSubs.add(r.getTypeUrl()); // Track that this xDS has actually asked for things so that we expect old behaviour to be disabled.
                    if (requestedSubs.contains("*")) {
                        wildcards.add(r.getTypeUrl());
                        requestedSubs = requestedSubs.stream().filter(s -> !s.equals("*")).collect(Collectors.toSet());
                    } else {
                        wildcards.remove(r.getTypeUrl());
                    }
                    subs.replaceValues(r.getTypeUrl(), requestedSubs);
                }
            } else if (message instanceof DiscoveryResponse r) {
                if (!wildcards.contains(r.getTypeUrl())) {
                    if (!subs.containsKey(r.getTypeUrl())) {
                        failWithMessage("Sent message for type <%s> which envoy has never asked for", r.getTypeUrl());
                    }
                    List<String> unwanted = r.getResourcesList().stream()
                        .map(this::getNameForResource)
                        .filter(resName -> !subs.get(r.getTypeUrl()).contains(resName)).toList();
                    if (!unwanted.isEmpty()) {
                        failWithMessage("Sent message for type <%s> containing resource(s) <%s> which envoy has never asked for", r.getTypeUrl(), unwanted);
                    }
                }
            } else {
                failWithMessage("Expected message to be of type DeltaDiscoveryRequest or DeltaDiscoveryResponse, but found <%s>", message.getClass());
            }
        }
        return this;
    }

    private String getNameForResource(Any any) {
        Class<? extends Message> type = XdsTypes.messageClass(any.getTypeUrl());
        Object resource = type == null ? any : sneakyUnpack(any, type); // an unknown type fails below
        if (resource instanceof Cluster c) {
            return c.getName();
        }
        if (resource instanceof ClusterLoadAssignment cla) {
            return cla.getClusterName();
        }
        if (resource instanceof Listener l) {
            return l.getName();
        }
        if (resource instanceof RouteConfiguration r) {
            return r.getName();
        }
        throw failure("Found message of type I don't know how to decode <%s>", any.getTypeUrl());
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert unsubscribedFrom(String typeUrl, String resourceName) {
        isNotNull();
        boolean subscribed = false;
        for (Object message : actual.getMessages()) {
            if (message instanceof DiscoveryRequest r && r.getTypeUrl().equals(typeUrl)) {
                if (r.getResourceNamesList().contains(resourceName)) {
                    subscribed = true;
                } else if (subscribed) {
                    return this;
                }
            }
        }
        failWithMessage("Envoy never unsubscribed type <%s> from resource <%s>", typeUrl, resourceName);
        return this;
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert sentNoEmptyResponses(int fromMessage) {
        isNotNull();
        List<Object> messages = actual.getMessages();
        Set<String> answered = new HashSet<>();
        for (int i = 0; i < messages.size(); i++) {
            if (!(messages.get(i) instanceof DiscoveryResponse r)) {
                continue;
            }
            // A type's first answer may be empty: it tells Envoy there's nothing yet.
            boolean first = answered.add(r.getTypeUrl());
            // An empty response is meaningful for wildcard types: it removes everything.
            if (!first && i >= fromMessage && r.getResourcesCount() == 0 && !XdsTypes.isWildcard(r.getTypeUrl())) {
                failWithMessage("Sent empty response <%s> for type <%s>", r.getNonce(), r.getTypeUrl());
            }
        }
        return this;
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert wasMakeBeforeBreak(int fromMessage) {
        return wasMakeBeforeBreak(fromMessage, null);
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert wasMakeBeforeBreak(int fromMessage, @Nullable Conversation before) {
        isNotNull();
        AckedState state = new AckedState();
        if (before != null) {
            replay(state, before.getMessages(), Integer.MAX_VALUE);
        }
        replay(state, actual.getMessages(), fromMessage);
        if (!state.problems().isEmpty()) {
            failWithMessage("Not make before break: <%s>", state.problems());
        }
        return this;
    }

    // Track what Envoy ACKs, checking responses from checkFrom on.
    private void replay(AckedState state, List<Object> messages, int checkFrom) {
        Map<String, DiscoveryResponse> pending = new HashMap<>();
        Map<String, Set<String>> pendingRemovals = new HashMap<>();
        Map<String, Set<String>> subscribed = new HashMap<>(); // names, by non-wildcard type
        for (int i = 0; i < messages.size(); i++) {
            Object message = messages.get(i);
            if (message instanceof DiscoveryResponse r) {
                // LDS, CDS and SRDS are wildcard types: each response is the full set, so anything Envoy has that's missing is removed.
                Set<String> removed = new HashSet<>();
                if (r.getTypeUrl().equals(XdsTypes.CDS) || r.getTypeUrl().equals(XdsTypes.LDS)) {
                    removed.addAll(r.getTypeUrl().equals(XdsTypes.CDS) ? state.clusters() : state.listeners());
                    r.getResourcesList().forEach(any -> removed.remove(getNameForResource(any)));
                }
                if (i >= checkFrom) {
                    state.checkResponse(r.getTypeUrl(), r.getResourcesList(), removed);
                }
                pending.put(r.getNonce(), r);
                pendingRemovals.put(r.getNonce(), removed);
            } else if (message instanceof DiscoveryRequest r && !r.hasErrorDetail()) {
                DiscoveryResponse ackedResponse = pending.remove(r.getResponseNonce());
                if (ackedResponse != null) {
                    state.acked(ackedResponse.getTypeUrl(), ackedResponse.getResourcesList(), Preconditions.checkNotNull(pendingRemovals.remove(r.getResponseNonce())));
                }
                if (!XdsTypes.isWildcard(r.getTypeUrl())) {
                    // Envoy drops what it stops asking for, e.g. a route config once no listener uses it.
                    Set<String> names = new HashSet<>(r.getResourceNamesList());
                    Set<String> dropped = new HashSet<>(subscribed.getOrDefault(r.getTypeUrl(), Set.of()));
                    dropped.removeAll(names);
                    subscribed.put(r.getTypeUrl(), names);
                    state.acked(r.getTypeUrl(), List.of(), dropped);
                }
            }
        }
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert answeredEveryType() {
        isNotNull();
        Set<String> unanswered = new HashSet<>();
        for (Object message : actual.getMessages()) {
            if (message instanceof DiscoveryRequest r) {
                unanswered.add(r.getTypeUrl());
            }
        }
        for (Object message : actual.getMessages()) {
            if (message instanceof DiscoveryResponse r) {
                unanswered.remove(r.getTypeUrl());
            }
        }
        if (!unanswered.isEmpty()) {
            failWithMessage("Envoy got no response at all for <%s>", unanswered);
        }
        return this;
    }

    @Override
    @CanIgnoreReturnValue
    public SotwConversationAssert hadConversationsWith(String... typeUrls) {
        isNotNull();

        Set<String> needed = new HashSet<>(List.of(typeUrls));
        for (Object message : actual.getMessages()) {
            if (message instanceof DiscoveryRequest r) {
                needed.remove(r.getTypeUrl());
                if (needed.isEmpty()) {
                    return this;
                }
            }
        }
        if (!needed.isEmpty()) {
            failWithMessage("Did not have a conversation with <%s>", needed);
        }
        return this;
    }

    /** An AssertJ factory for these checks. */
    public static class Factory implements AssertFactory<Conversation, SotwConversationAssert> {

        /** Creates the factory. */
        public Factory() {
        }

        @Override
        public SotwConversationAssert createAssert(Conversation conversation) {
            return SotwConversationAssert.assertThat(conversation);
        }
    }

}
