package net.woaf.jono.envoy.testing.assertions;

import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Any;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import net.woaf.jono.envoy.testing.XdsTypes;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.AssertFactory;
import org.assertj.core.api.InstanceOfAssertFactory;

public class DeltaConversationAssert extends EnvoyAssert<DeltaConversationAssert> {


    public static InstanceOfAssertFactory<Conversation, DeltaConversationAssert> DELTA_CONVERSATION = new InstanceOfAssertFactory<>(Conversation.class, new Factory());

    protected DeltaConversationAssert(Conversation actual) {
        super(actual, DeltaConversationAssert.class);
    }

    public static DeltaConversationAssert assertThat(Conversation actual) {
        return new DeltaConversationAssert(actual);
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert allWereAcked() {
        isNotNull();
        HashSet<String> outstandingNonces = new HashSet<>();

        for (Object message : actual.getMessages()) {
            if (message instanceof DeltaDiscoveryRequest r) {
                if (Strings.emptyToNull(r.getResponseNonce()) != null) {
                    if (!outstandingNonces.remove(r.getResponseNonce())) {
                        failWithMessage("Found unexpected response nonce <%s>, expected one of <%s>", r.getResponseNonce(), outstandingNonces);
                    }
                }
                if (r.hasErrorDetail()) {
                    failWithMessage("Envoy sent error <%s>", r.getErrorDetail().getMessage());
                }
            } else if (message instanceof DeltaDiscoveryResponse r) {
                if (!outstandingNonces.add(r.getNonce())) {
                    failWithMessage("Sent nonce <%s> multiple times", r.getNonce());
                }
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
    public DeltaConversationAssert nothingSentUnaskedPre119Reconnect() {
        return nothingSentUnasked(true);
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert nothingSentUnasked() {
        return nothingSentUnasked(false);
    }

    @Override
    @CanIgnoreReturnValue
    public DeltaConversationAssert nothingSentUnasked(boolean allowBrokenReconnectWildcardBehaviourPre119) {
        isNotNull();

        HashMultimap<String, String> subscriptionsByTypeUrl = HashMultimap.create();
        HashSet<String> wildcardSubscriptionTypeUrls = new HashSet<>();
        HashSet<String> seenTypeUrls = new HashSet<>();

        for (Object message : actual.getMessages()) {
            if (message instanceof DeltaDiscoveryRequest r) {
                List<String> addsSubs = r.getResourceNamesSubscribeList();
                if (seenTypeUrls.add(r.getTypeUrl())
                    && (addsSubs.isEmpty() || (XdsTypes.isWildcard(r.getTypeUrl()) && allowBrokenReconnectWildcardBehaviourPre119))
                ) {
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
        for (Object message : actual.getMessages().subList(fromMessage, actual.getMessages().size())) {
            if (message instanceof DeltaDiscoveryResponse r && r.getResourcesCount() == 0 && r.getRemovedResourcesCount() == 0) {
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
    public DeltaConversationAssert wasMakeBeforeBreak(int fromMessage, Conversation before) {
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
        Map<String, DeltaDiscoveryResponse> pending = new HashMap<>();
        for (int i = 0; i < messages.size(); i++) {
            Object message = messages.get(i);
            if (message instanceof DeltaDiscoveryResponse r) {
                if (i >= checkFrom) {
                    state.checkResponse(r.getTypeUrl(), resources(r), r.getRemovedResourcesList());
                }
                pending.put(r.getNonce(), r);
            } else if (message instanceof DeltaDiscoveryRequest r && !r.hasErrorDetail()) {
                DeltaDiscoveryResponse ackedResponse = pending.remove(r.getResponseNonce());
                if (ackedResponse != null) {
                    state.acked(ackedResponse.getTypeUrl(), resources(ackedResponse), ackedResponse.getRemovedResourcesList());
                }
            }
        }
    }

    private static List<Any> resources(DeltaDiscoveryResponse response) {
        return response.getResourcesList().stream().map(Resource::getResource).toList();
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

    public static class Factory implements AssertFactory<Conversation, DeltaConversationAssert> {

        @Override
        public DeltaConversationAssert createAssert(Conversation conversation) {
            return DeltaConversationAssert.assertThat(conversation);
        }
    }

}
