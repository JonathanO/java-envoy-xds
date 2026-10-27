package net.woaf.jono.envoy.testing.matchers;

import static net.woaf.jono.envoy.testing.configdump.EnvoyAdminClient.sneakyUnpack;

import org.jspecify.annotations.Nullable;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.config.route.v3.WeightedCluster;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import net.woaf.jono.envoy.testing.XdsTypes;
import net.woaf.jono.envoy.testing.configdump.TypedEnvoyConfigDump;
import net.woaf.jono.envoy.testing.utils.ConversationLogger.Conversation;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * Hamcrest matchers for xDS resources, recorded messages and conversations, and what Envoy holds. Use them with
 * Hamcrest's assertThat, AssertJ's {@code HamcrestCondition}, Awaitility's {@code until(supplier, matcher)} or
 * Mockito's {@code argThat}.
 *
 * <pre>{@code
 * assertThat(conversation, hasInOrder(
 *     ackOf(response(ofType(XdsTypes.RDS), containing(routeConfig(not(routingTo("bar")))))),
 *     response(ofType(XdsTypes.CDS), removing("bar"))));
 * assertThat(envoy.admin().configDump(), activeClusterNames(containsInAnyOrder("foo", "baz")));
 * }</pre>
 */
public final class XdsMatchers {

    private XdsMatchers() {
    }

    // Resources

    /**
     * A resource with this name: its name field, or cluster_name for a ClusterLoadAssignment.
     * @param name resource name
     * @return matcher
     */
    public static Matcher<Message> named(String name) {
        return new FeatureMatcher<Message, @Nullable String>(Matchers.equalTo(name), "named", "name") {
            @Override
            protected @Nullable String featureValueOf(Message actual) {
                return XdsTypes.nameOf(actual);
            }
        };
    }

    /**
     * A Cluster matching all of these.
     * @param matchers e.g. named("foo"), ofType(Cluster.DiscoveryType.EDS)
     * @return matcher
     */
    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    public static Matcher<Message> cluster(Matcher<? super Cluster>... matchers) {
        return resource(Cluster.class, "a Cluster", matchers);
    }

    /**
     * A cluster of this discovery type.
     * @param type e.g. EDS
     * @return matcher
     */
    public static Matcher<Cluster> ofType(Cluster.DiscoveryType type) {
        return feature("of type", "type", Cluster::getType, Matchers.equalTo(type));
    }

    /**
     * A recorded message of this type.
     * @param typeUrl e.g. XdsTypes.CDS
     * @return matcher, for response(), request() or nack()
     */
    public static Matcher<XdsMessage> ofType(String typeUrl) {
        return feature("of type", "type", XdsMessage::typeUrl, Matchers.equalTo(typeUrl));
    }

    /**
     * A RouteConfiguration matching all of these.
     * @param matchers e.g. named("all_routes"), routingTo("foo")
     * @return matcher
     */
    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    public static Matcher<Message> routeConfig(Matcher<? super RouteConfiguration>... matchers) {
        return resource(RouteConfiguration.class, "a RouteConfiguration", matchers);
    }

    /**
     * Some route sends traffic to this cluster, directly or as one of its weighted clusters.
     * @param cluster cluster name
     * @return matcher
     */
    public static Matcher<RouteConfiguration> routingTo(String cluster) {
        return any("routing to", "clusters routed to", r -> clustersRoutedTo(r.getVirtualHostsList().stream()), Matchers.equalTo(cluster));
    }

    /**
     * The virtual host of this name sends traffic to this cluster.
     * @param virtualHost virtual host name
     * @param cluster cluster name
     * @return matcher
     */
    public static Matcher<RouteConfiguration> routing(String virtualHost, String cluster) {
        return any("routing virtual host " + virtualHost + " to", "clusters it routes to",
            r -> clustersRoutedTo(r.getVirtualHostsList().stream().filter(v -> v.getName().equals(virtualHost))), Matchers.equalTo(cluster));
    }

    /**
     * A ClusterLoadAssignment matching all of these.
     * @param matchers e.g. named("foo"), withEndpoint("10.0.0.1", 8080)
     * @return matcher
     */
    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    public static Matcher<Message> endpointAssignment(Matcher<? super ClusterLoadAssignment>... matchers) {
        return resource(ClusterLoadAssignment.class, "a ClusterLoadAssignment", matchers);
    }

    /**
     * An endpoint assignment with this endpoint.
     * @param address the endpoint's address
     * @param port its port
     * @return matcher
     */
    public static Matcher<ClusterLoadAssignment> withEndpoint(String address, int port) {
        return any("with endpoint", "endpoints", cla -> cla.getEndpointsList().stream()
            .flatMap(l -> l.getLbEndpointsList().stream())
            .map(e -> e.getEndpoint().getAddress().getSocketAddress())
            .map(a -> a.getAddress() + ":" + a.getPortValue())
            .toList(), Matchers.equalTo(address + ":" + port));
    }

    // Recorded messages

    /**
     * A response to Envoy matching all of these.
     * @param matchers e.g. ofType(XdsTypes.CDS), containing(cluster(named("foo"))), removing("bar")
     * @return matcher on a recorded message
     */
    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    public static Matcher<Object> response(Matcher<? super XdsMessage.Response>... matchers) {
        return message(XdsMessage.Response.class, "a response", matchers);
    }

    /**
     * A request from Envoy matching all of these.
     * @param matchers e.g. ofType(XdsTypes.EDS), subscribingTo("foo")
     * @return matcher on a recorded message
     */
    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    public static Matcher<Object> request(Matcher<? super XdsMessage.Request>... matchers) {
        return message(XdsMessage.Request.class, "a request", matchers);
    }

    /**
     * Envoy rejecting a response, matching all of these.
     * @param matchers e.g. ofType(XdsTypes.CDS), withMessage(containsString("connect_timeout"))
     * @return matcher on a recorded message
     */
    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    public static Matcher<Object> nack(Matcher<? super XdsMessage.Request>... matchers) {
        return message(XdsMessage.Request.class, "a NACK",
            Stream.concat(Stream.of(feature("that is", "NACK", XdsMessage.Request::isNack, Matchers.is(true))), Arrays.stream(matchers)).toList());
    }

    /**
     * A response with a matching resource.
     * @param resource e.g. cluster(named("foo"))
     * @return matcher
     */
    public static Matcher<XdsMessage.Response> containing(Matcher<? super Message> resource) {
        return any("containing", "resources", XdsMessage.Response::resources, resource);
    }

    /**
     * Removes a resource. For delta, it's in removed_resources. For SotW, only wildcard types (LDS, CDS, SRDS, VHDS) can remove one,
     * by leaving it out, so this matches a wildcard response without it.
     * @param name resource name
     * @return matcher
     */
    public static Matcher<XdsMessage.Response> removing(String name) {
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(XdsMessage.Response response, Description mismatch) {
                boolean removes = response.delta()
                    ? response.removed().contains(name)
                    : XdsTypes.isWildcard(response.typeUrl()) && response.resources().stream().noneMatch(r -> name.equals(XdsTypes.nameOf(r)));
                if (!removes) {
                    mismatch.appendText(response.delta() ? "removed " + response.removed() : "a SotW " + response.typeUrl() + " response that has it, or isn't wildcard");
                }
                return removes;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("removing ").appendValue(name);
            }
        };
    }

    /**
     * A request subscribing to this name.
     * @param name resource name
     * @return matcher
     */
    public static Matcher<XdsMessage.Request> subscribingTo(String name) {
        return any("subscribing to", "names", XdsMessage.Request::subscribe, Matchers.equalTo(name));
    }

    /**
     * Unsubscribes from a resource. Delta only: SotW requests list every name, so an unsubscribe shows only against the
     * request before; use EnvoyAssert.unsubscribedFrom on the conversation instead.
     * @param name resource name
     * @return matcher
     */
    public static Matcher<XdsMessage.Request> unsubscribingFrom(String name) {
        return any("unsubscribing from", "unsubscribes", XdsMessage.Request::unsubscribe, Matchers.equalTo(name));
    }

    /**
     * A NACK whose error message matches.
     * @param message e.g. containsString("unknown cluster")
     * @return matcher
     */
    public static Matcher<XdsMessage.Request> withMessage(Matcher<? super String> message) {
        return feature("with error", "error", XdsMessage.Request::errorMessage, message);
    }

    // Conversations

    /**
     * A conversation with a matching message.
     * @param message e.g. nack()
     * @return matcher
     */
    public static Matcher<Conversation> hasMessage(Matcher<Object> message) {
        return any("a conversation with", "messages", Conversation::getMessages, message);
    }

    /**
     * Each step happened, in this order: each one later in the conversation than the one before.
     * @param steps message matchers, or {@link #ackOf} one
     * @return matcher
     */
    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    public static Matcher<Conversation> hasInOrder(Matcher<Object>... steps) {
        List<Matcher<Object>> stepList = List.of(steps);
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(Conversation conversation, Description mismatch) {
                List<Object> messages = conversation.getMessages();
                int position = -1;
                for (int s = 0; s < stepList.size(); s++) {
                    Matcher<Object> step = stepList.get(s);
                    int found = step instanceof AckOf ackOf ? ackOf.find(messages, position + 1) : first(messages, position + 1, step::matches);
                    if (found < 0) {
                        mismatch.appendText("step " + (s + 1) + ", ").appendDescriptionOf(step)
                            .appendText(position < 0 ? ", not found" : ", not found after message " + position)
                            .appendText(" of ").appendValue(messages.size());
                        return false;
                    }
                    position = found;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendList("a conversation with, in order: ", ", then ", "", stepList);
            }
        };
    }

    /**
     * Envoy ACKing a response that matches. Only meaningful inside {@link #hasInOrder}, where the step's position is the
     * ACK's.
     * @param response matcher for the response
     * @return step
     */
    public static Matcher<Object> ackOf(Matcher<Object> response) {
        return new AckOf(response);
    }

    // What Envoy holds

    /**
     * Envoy has a matching active dynamic cluster.
     * @param cluster e.g. named("foo")
     * @return matcher
     */
    public static Matcher<TypedEnvoyConfigDump> hasActiveCluster(Matcher<? super Cluster> cluster) {
        return any("Envoy with an active cluster", "active clusters", XdsMatchers::activeClusters, cluster);
    }

    /**
     * The names of Envoy's active dynamic clusters.
     * @param names e.g. containsInAnyOrder("foo", "bar")
     * @return matcher
     */
    public static Matcher<TypedEnvoyConfigDump> activeClusterNames(Matcher<? super Collection<String>> names) {
        return feature("Envoy with active clusters", "active clusters", d -> activeClusters(d).stream().map(Cluster::getName).toList(), names);
    }

    /**
     * Envoy has a matching dynamic route config.
     * @param route e.g. named("routes")
     * @return matcher
     */
    public static Matcher<TypedEnvoyConfigDump> hasRouteConfig(Matcher<? super RouteConfiguration> route) {
        return feature("Envoy with a dynamic route config", "dynamic route configs",
            d -> d.routesConfigDump().getDynamicRouteConfigsList().stream().map(r -> sneakyUnpack(r.getRouteConfig(), RouteConfiguration.class)).toList(),
            Matchers.hasItem(route));
    }

    /**
     * A stat, e.g. from {@code envoy.admin().stats()}. A stat Envoy doesn't have never matches, even
     * {@code not(greaterThan(0L))}, so a misspelt name fails rather than passes.
     * @param name stat name, e.g. "cluster.foo.upstream_rq_total"
     * @param value e.g. greaterThan(0L)
     * @return matcher
     */
    public static Matcher<Map<String, Long>> hasStat(String name, Matcher<? super Long> value) {
        Matcher<Map<String, Long>> stat = feature("stat " + name, name, stats -> stats.get(name), value);
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(Map<String, Long> stats, Description mismatch) {
                if (!stats.containsKey(name)) {
                    mismatch.appendText("Envoy has no stat ").appendText(name);
                    return false;
                }
                if (!stat.matches(stats)) {
                    stat.describeMismatch(stats, mismatch);
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                stat.describeTo(description);
            }
        };
    }

    private static List<Cluster> activeClusters(TypedEnvoyConfigDump dump) {
        return dump.clustersConfigDump().getDynamicActiveClustersList().stream().map(c -> sneakyUnpack(c.getCluster(), Cluster.class)).toList();
    }

    private static List<String> clustersRoutedTo(Stream<VirtualHost> virtualHosts) {
        return virtualHosts.flatMap(v -> v.getRoutesList().stream())
            .flatMap(r -> Stream.concat(Stream.of(r.getRoute().getCluster()), r.getRoute().getWeightedClusters().getClustersList().stream().map(WeightedCluster.ClusterWeight::getName)))
            .filter(c -> !c.isEmpty())
            .toList();
    }

    private static int first(List<Object> messages, int from, Predicate<Object> matches) {
        for (int i = from; i < messages.size(); i++) {
            if (matches.test(messages.get(i))) {
                return i;
            }
        }
        return -1;
    }

    // One of the values matches. Described as "description item", rather than hasItem's "a collection containing item".
    private static <T, U> Matcher<T> any(String description, String name, Function<T, ? extends Collection<? extends U>> values, Matcher<? super U> item) {
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(T actual, Description mismatch) {
                Collection<? extends U> found = values.apply(actual);
                if (found.stream().anyMatch(item::matches)) {
                    return true;
                }
                mismatch.appendText(name).appendText(" were ").appendValueList("[", ", ", "]", found);
                return false;
            }

            @Override
            public void describeTo(Description d) {
                d.appendText(description).appendText(" ").appendDescriptionOf(item);
            }
        };
    }

    private static <T, U extends @Nullable Object> Matcher<T> feature(String description, String name, Function<T, U> value, Matcher<? super U> matcher) {
        return new FeatureMatcher<T, U>(matcher, description, name) {
            @Override
            protected U featureValueOf(T actual) {
                return value.apply(actual);
            }
        };
    }

    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    private static <R extends Message> Matcher<Message> resource(Class<R> type, String description, Matcher<? super R>... matchers) {
        Matcher<R> all = Matchers.<R>allOf(Arrays.<Matcher<? super R>>asList(matchers));
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(Message item, Description mismatch) {
                if (!type.isInstance(item)) {
                    mismatch.appendText("was a ").appendText(item.getDescriptorForType().getFullName());
                    return false;
                }
                if (!all.matches(item)) {
                    all.describeMismatch(item, mismatch);
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description d) {
                d.appendText(description);
                if (matchers.length > 0) {
                    d.appendText(" ").appendDescriptionOf(all);
                }
            }
        };
    }

    @SafeVarargs
    @SuppressWarnings("varargs") // only read
    private static <M extends XdsMessage> Matcher<Object> message(Class<M> type, String description, Matcher<? super M>... matchers) {
        return message(type, description, List.of(matchers));
    }

    private static <M extends XdsMessage> Matcher<Object> message(Class<M> type, String description, List<Matcher<? super M>> matchers) {
        Matcher<M> all = Matchers.<M>allOf(matchers);
        return new BaseMatcher<>() {
            @Override
            public boolean matches(@Nullable Object item) {
                Optional<M> message = XdsMessage.of(item).filter(type::isInstance).map(type::cast);
                return message.isPresent() && all.matches(message.get());
            }

            @Override
            public void describeMismatch(@Nullable Object item, Description mismatch) {
                Optional<M> message = XdsMessage.of(item).filter(type::isInstance).map(type::cast);
                if (message.isEmpty()) {
                    mismatch.appendText("was ").appendText(item == null ? "null" : item.getClass().getSimpleName());
                } else {
                    all.describeMismatch(message.get(), mismatch);
                }
            }

            @Override
            public void describeTo(Description d) {
                d.appendText(description);
                if (!matchers.isEmpty()) {
                    d.appendText(" ").appendDescriptionOf(all);
                }
            }
        };
    }

    // See ackOf().
    private static final class AckOf extends BaseMatcher<Object> {

        private final Matcher<Object> response;

        private AckOf(Matcher<Object> response) {
            this.response = response;
        }

        // Index of the earliest ACK, from `from` on, of a response that matches.
        int find(List<Object> messages, int from) {
            int earliest = -1;
            for (int i = 0; i < messages.size(); i++) {
                Optional<XdsMessage> sent = XdsMessage.of(messages.get(i));
                if (sent.isEmpty() || !(sent.get() instanceof XdsMessage.Response r) || !response.matches(messages.get(i))) {
                    continue;
                }
                int ack = first(messages, Math.max(i + 1, from), m -> XdsMessage.of(m)
                    .filter(x -> x instanceof XdsMessage.Request q && !q.isNack() && Objects.equals(q.responseNonce(), r.nonce())).isPresent());
                if (ack >= 0 && (earliest < 0 || ack < earliest)) {
                    earliest = ack;
                }
            }
            return earliest;
        }

        @Override
        public boolean matches(Object item) {
            return false;
        }

        @Override
        public void describeMismatch(Object item, Description mismatch) {
            mismatch.appendText("ackOf() only matches inside hasInOrder()");
        }

        @Override
        public void describeTo(Description d) {
            d.appendText("an ACK of ").appendDescriptionOf(response);
        }
    }
}
