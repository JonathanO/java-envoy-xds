package net.woaf.jono.envoy.testing.assertions;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Table;
import org.jspecify.annotations.Nullable;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Any;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.assertj.core.api.AssertFactory;
import org.assertj.core.api.InstanceOfAssertFactory;

/**
 * Checks on a recorded delta xDS conversation.
 */
public class DeltaConversationAssert extends EnvoyAssert<DeltaConversationAssert> {


    /**
     * For AssertJ's {@code asInstanceOf} and {@code extracting}.
     */
    public static final InstanceOfAssertFactory<Conversation, DeltaConversationAssert> DELTA_CONVERSATION = new InstanceOfAssertFactory<>(Conversation.class, new Factory());

    /**
     * Use {@link #assertThat}.
     * @param actual the conversation
     */
    protected DeltaConversationAssert(Conversation actual) {
        super(actual, DeltaConversationAssert.class);
    }

    /**
     * {@return checks on this conversation}
     * @param actual the conversation
     */
    public static DeltaConversationAssert assertThat(Conversation actual) {
        return new DeltaConversationAssert(actual);
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert allWereAcked() {
        isNotNull();
        // By type: Envoy keeps nonces per type, so a library may number them per type.
        SetMultimap<String, String> sentNonces = HashMultimap.create();
        SetMultimap<String, String> outstandingNonces = HashMultimap.create();

        for (Object message : actual.getMessages()) {
            if (message instanceof DeltaDiscoveryRequest r) {
                if (Strings.emptyToNull(r.getResponseNonce()) != null) {
                    if (!outstandingNonces.remove(r.getTypeUrl(), r.getResponseNonce())) {
                        failWithMessage("Found unexpected response nonce <%s> for <%s>, expected one of <%s>", r.getResponseNonce(), r.getTypeUrl(), outstandingNonces.get(r.getTypeUrl()));
                    }
                }
                if (r.hasErrorDetail()) {
                    failWithMessage("Envoy sent error <%s>", r.getErrorDetail().getMessage());
                }
            } else if (message instanceof DeltaDiscoveryResponse r) {
                if (!sentNonces.put(r.getTypeUrl(), r.getNonce())) {
                    failWithMessage("Sent nonce <%s> for <%s> multiple times", r.getNonce(), r.getTypeUrl());
                }
                outstandingNonces.put(r.getTypeUrl(), r.getNonce());
            } else {
                failWithMessage("Expected message to be of type DeltaDiscoveryRequest or DeltaDiscoveryResponse, but found <%s>", message.getClass());
            }
        }
        if (!outstandingNonces.isEmpty()) {
            failWithMessage("Found unacked nonces <%s>", outstandingNonces);
        }
        return this;
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert nothingSentUnasked() {
        isNotNull();

        HashMultimap<String, String> subscriptionsByTypeUrl = HashMultimap.create();
        HashSet<String> wildcardSubscriptionTypeUrls = new HashSet<>();
        HashSet<String> seenTypeUrls = new HashSet<>();

        for (Object message : actual.getMessages()) {
            if (message instanceof DeltaDiscoveryRequest r) {
                List<String> addsSubs = r.getResourceNamesSubscribeList();
                if (seenTypeUrls.add(r.getTypeUrl()) && addsSubs.isEmpty()) {
                    // First request of this type, allow old wildcard behaviour
                    addsSubs = List.of("*");
                }
                for (String addsSub : addsSubs) {
                    if (addsSub.equals("*")) {
                        if (!wildcardSubscriptionTypeUrls.add(r.getTypeUrl())) {
                            failWithMessage("Envoy attempted to subscribe type <%s> as wildcard more than once", r.getTypeUrl());
                        }
                    } else if (!subscriptionsByTypeUrl.put(r.getTypeUrl(), addsSub)) {
                        failWithMessage("Envoy attempted to subscribe type <%s> to resource <%s> more than once", r.getTypeUrl(), addsSub);
                    }

                }
                List<String> removesSubs = r.getResourceNamesUnsubscribeList();
                for (String removeSub : removesSubs) {
                    if (removeSub.equals("*")) {
                        if (!wildcardSubscriptionTypeUrls.remove(r.getTypeUrl())) {
                            failWithMessage("Envoy attempted to unsubscribe type <%s> as wildcard when it was not subscribed", r.getTypeUrl());
                        }
                    } else {
                        if (!subscriptionsByTypeUrl.remove(r.getTypeUrl(), removeSub)) {
                            failWithMessage("Envoy attempted to unsubscribe type <%s> from resource <%s> when it was not subscribed", r.getTypeUrl(), removeSub);
                        }
                    }
                }
            } else if (message instanceof DeltaDiscoveryResponse r) {
                if (!wildcardSubscriptionTypeUrls.contains(r.getTypeUrl())) {
                    if (!subscriptionsByTypeUrl.containsKey(r.getTypeUrl())) {
                        failWithMessage("Sent message for type <%s> which envoy has never asked for", r.getTypeUrl());
                    }
                    List<String> unwanted = r.getResourcesList().stream().map(Resource::getName).filter(resName -> !subscriptionsByTypeUrl.get(r.getTypeUrl()).contains(resName)).toList();
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

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert unsubscribedFrom(String typeUrl, String resourceName) {
        isNotNull();
        for (Object message : actual.getMessages()) {
            if (message instanceof DeltaDiscoveryRequest r && r.getTypeUrl().equals(typeUrl) && r.getResourceNamesUnsubscribeList().contains(resourceName)) {
                return this;
            }
        }
        failWithMessage("Envoy never unsubscribed type <%s> from resource <%s>", typeUrl, resourceName);
        return this;
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert sentNoEmptyResponses(int fromMessage) {
        isNotNull();
        List<Object> messages = actual.getMessages();
        Set<String> answered = new HashSet<>();
        for (int i = 0; i < messages.size(); i++) {
            if (!(messages.get(i) instanceof DeltaDiscoveryResponse r)) {
                continue;
            }
            // A type's first answer may be empty: it tells Envoy there's nothing yet.
            boolean first = answered.add(r.getTypeUrl());
            if (!first && i >= fromMessage && r.getResourcesCount() == 0 && r.getRemovedResourcesCount() == 0) {
                failWithMessage("Sent empty response <%s> for type <%s>", r.getNonce(), r.getTypeUrl());
            }
        }
        return this;
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert wasMakeBeforeBreak(int fromMessage) {
        return wasMakeBeforeBreak(fromMessage, null);
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert wasMakeBeforeBreak(int fromMessage, @Nullable Conversation before) {
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
    private static void replay(AckedState state, List<Object> messages, int checkFrom) {
        Table<String, String, DeltaDiscoveryResponse> pending = HashBasedTable.create(); // by type, then nonce
        for (int i = 0; i < messages.size(); i++) {
            Object message = messages.get(i);
            if (message instanceof DeltaDiscoveryResponse r) {
                if (i >= checkFrom) {
                    state.checkResponse(r.getTypeUrl(), resources(r), r.getRemovedResourcesList());
                }
                pending.put(r.getTypeUrl(), r.getNonce(), r);
            } else if (message instanceof DeltaDiscoveryRequest r && !r.hasErrorDetail()) {
                DeltaDiscoveryResponse ackedResponse = pending.remove(r.getTypeUrl(), r.getResponseNonce());
                if (ackedResponse != null) {
                    state.acked(ackedResponse.getTypeUrl(), resources(ackedResponse), ackedResponse.getRemovedResourcesList());
                }
                // Envoy drops what it unsubscribes from, e.g. a route config once no listener uses it.
                state.acked(r.getTypeUrl(), List.of(), r.getResourceNamesUnsubscribeList());
            }
        }
    }

    private static List<Any> resources(DeltaDiscoveryResponse response) {
        return response.getResourcesList().stream().map(Resource::getResource).toList();
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert askedForEveryRouteConfigInUse() {
        isNotNull();
        AckedState state = new AckedState();
        replay(state, actual.getMessages(), Integer.MAX_VALUE);
        Set<String> asked = new HashSet<>();
        for (Object message : actual.getMessages()) {
            if (message instanceof DeltaDiscoveryRequest r && r.getTypeUrl().equals(XdsTypes.RDS)) {
                asked.addAll(r.getResourceNamesSubscribeList());
                r.getResourceNamesUnsubscribeList().forEach(asked::remove);
            }
        }
        List<String> missing = state.routeConfigsNotAskedFor(asked);
        if (!missing.isEmpty()) {
            failWithMessage("Envoy hasn't asked for <%s>", missing);
        }
        return this;
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert answeredEveryType() {
        isNotNull();
        Set<String> unanswered = new HashSet<>();
        for (Object message : actual.getMessages()) {
            if (message instanceof DeltaDiscoveryRequest r) {
                unanswered.add(r.getTypeUrl());
            }
        }
        for (Object message : actual.getMessages()) {
            if (message instanceof DeltaDiscoveryResponse r) {
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
    public DeltaConversationAssert hadConversationsWith(String... typeUrls) {
        isNotNull();

        Set<String> needed = new HashSet<>(List.of(typeUrls));
        for (Object message : actual.getMessages()) {
            if (message instanceof DeltaDiscoveryRequest r) {
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
    public static class Factory implements AssertFactory<Conversation, DeltaConversationAssert> {

        /** Creates the factory. */
        public Factory() {
        }

        @Override
        public DeltaConversationAssert createAssert(Conversation conversation) {
            return DeltaConversationAssert.assertThat(conversation);
        }
    }

}
