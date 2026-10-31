package net.woaf.jono.envoy.xds.spec;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Internal;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.Status;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import net.woaf.jono.envoy.xds.AggregatedDiscoveryService;
import net.woaf.jono.envoy.xds.TypeUrl;
import net.woaf.jono.envoy.xds.XdsConfig;
import net.woaf.jono.envoy.xds.api.DiscoveryServiceManagerMetrics;
import net.woaf.jono.envoy.xds.api.utils.MergingStateBacklog;
import net.woaf.jono.envoy.xds.snapshot.Snapshot;
import net.woaf.jono.envoy.xds.snapshot.SnapshotConfigBuilder;
import net.woaf.jono.envoy.xds.snapshot.SnapshotSource;
import net.woaf.jono.envoy.xds.snapshot.SnapshotUpdate;
import org.jspecify.annotations.Nullable;

/**
 * One vector, on one protocol: a SnapshotSource served by the real AggregatedDiscoveryService, and a fake Envoy per
 * envoy in the vector. Source updates are delivered on the test's thread after each step, so every response to a step
 * has been sent when the step's expectations are checked. See docs/VECTORS.md in the spec repo for the format, and
 * docs/REPLAY.md for {@link #replay()}.
 */
final class VectorRun {

    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

    /** A reference to a response relative to the last of its type: "last", or "last-1" for the one before it. */
    private static final Pattern LAST = Pattern.compile("last(?:-([0-9]+))?");

    private static final Pattern SECONDS = Pattern.compile("([0-9]+)s");

    private final JsonObject vector;

    private final boolean delta;

    private final String title;

    /** Replaying, as docs/REPLAY.md says: an ack or nack of "last" with no such response is left out. */
    private final boolean replaying;

    private final Queue<Runnable> tasks = new ArrayDeque<>();

    private final Map<String, FakeEnvoy> envoys = new LinkedHashMap<>();

    private final Map<String, Received> labels = new HashMap<>();

    private final SnapshotSource<String, String, String> source;

    /** The server's clock, in nanoseconds: it moves only when a step says. */
    private long now;

    private final AggregatedDiscoveryService<SnapshotUpdate<String>, String> ads;

    VectorRun(JsonObject vector, String protocol) {
        this(vector, protocol, false);
    }

    VectorRun(JsonObject vector, String protocol, boolean replaying) {
        this.vector = vector;
        this.replaying = replaying;
        this.delta = switch (protocol) {
            case "delta" -> true;
            case "sotw" -> false;
            default -> throw new IllegalArgumentException("unknown protocol " + protocol);
        };
        this.title = vector.get("name").getAsString() + " [" + protocol + "]";
        Map<String, String> views = new HashMap<>();
        Map<String, XdsConfig<String>> configs = new HashMap<>();
        for (JsonElement e : vector.getAsJsonArray("envoys")) {
            JsonObject envoy = e.getAsJsonObject();
            String name = envoy.get("name").getAsString();
            views.put(name, envoy.get("view").getAsString());
            XdsConfig<String> config = XdsConfig.of(name);
            if (envoy.has("subscribesTo")) {
                config = config.withSubscribesTo(Set.copyOf(types(envoy.getAsJsonArray("subscribesTo"))));
            }
            if (envoy.has("delayUpdatesUntilAckOf")) {
                config = config.withDelayUpdatesUntilAckOf(type(envoy.get("delayUpdatesUntilAckOf")));
            }
            configs.put(name, config);
            envoys.put(name, new FakeEnvoy(name));
        }
        source = new SnapshotSource<>(views::get, tasks::add);
        @SuppressWarnings("unchecked")
        Class<? extends Message>[] served = types(vector.getAsJsonArray("serves")).stream().map(TypeUrl::getMessageClazz).toArray(Class[]::new);
        ads = new AggregatedDiscoveryService<>(source, SnapshotConfigBuilder.<String>forTypes(served), node -> configs.get(node.getId()),
            ImmutableList.of(), MergingStateBacklog.factory(), DiscoveryServiceManagerMetrics.METRICS_DISABLED, new Ticker() {
                @Override
                public long read() {
                    return now;
                }
            });
    }

    void run() {
        JsonArray steps = vector.getAsJsonArray("steps");
        for (int i = 0; i < steps.size(); i++) {
            JsonObject step = steps.get(i).getAsJsonObject();
            JsonObject shown = step.deepCopy();
            shown.remove("expect");
            String where = String.format("%s, step %d: %s", title, i + 1, shown);
            try {
                perform(step);
                check(step, where);
            } catch (RuntimeException e) {
                throw new AssertionError(where + "\nfailed: " + e, e);
            }
        }
    }

    /**
     * Runs every step, recording what each Envoy got instead of checking it.
     * @return the run, as docs/REPLAY.md says: {@code {"steps": [...]}}, with {@code "error"} if the vector is wrong
     */
    JsonObject replay() {
        JsonObject run = new JsonObject();
        JsonArray steps = new JsonArray();
        run.add("steps", steps);
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            try {
                perform(step.getAsJsonObject());
            } catch (RuntimeException e) {
                run.addProperty("error", e.toString());
                return run;
            }
            JsonArray responses = new JsonArray();
            boolean ended = false;
            for (FakeEnvoy envoy : envoys.values()) {
                for (Received response : envoy.received) {
                    responses.add(replayed(envoy.name, response));
                }
                envoy.received.clear();
                if (envoy.ended != null) {
                    JsonObject end = new JsonObject();
                    end.addProperty("envoy", envoy.name);
                    end.addProperty("ended", envoy.ended);
                    responses.add(end);
                    ended = true;
                }
            }
            steps.add(responses);
            if (ended) {
                return run;
            }
        }
        return run;
    }

    private JsonObject replayed(String envoy, Received received) {
        Contents contents = received.contents();
        JsonObject response = new JsonObject();
        response.addProperty("envoy", envoy);
        response.addProperty("type", received.type().name());
        JsonArray resources = new JsonArray();
        contents.resources().values().forEach(m -> resources.add(JsonParser.parseString(print(m))));
        response.add("resources", resources);
        if (delta) {
            JsonArray removed = new JsonArray();
            contents.removed().forEach(removed::add);
            response.add("removed", removed);
        }
        return response;
    }

    private void perform(JsonObject step) {
        if (step.has("update")) {
            update(step.getAsJsonArray("update"));
        } else if (step.has("request")) {
            envoy(step).send(step);
        } else if (step.has("disconnect")) {
            envoyNamed(step.get("disconnect").getAsString()).disconnect();
        } else if (step.has("advance")) {
            Matcher seconds = SECONDS.matcher(step.get("advance").getAsString());
            if (!seconds.matches()) {
                throw new IllegalArgumentException("advance takes seconds, such as \"30s\": " + step.get("advance"));
            }
            now += TimeUnit.SECONDS.toNanos(Long.parseLong(seconds.group(1)));
        } else {
            throw new IllegalArgumentException("unknown step " + step);
        }
        for (Runnable task = tasks.poll(); task != null; task = tasks.poll()) {
            task.run();
        }
    }

    private void update(JsonArray ops) {
        source.update(change -> {
            for (JsonElement element : ops) {
                if (element.isJsonPrimitive() && element.getAsString().equals("then")) {
                    change.then();
                    continue;
                }
                JsonObject op = element.getAsJsonObject();
                if (op.has("setView")) {
                    change.view(op.get("setView").getAsString(), strings(op, "groups"));
                } else if (op.has("editGroup")) {
                    change.group(op.get("editGroup").getAsString(), editor -> {
                        resources(op, "put").forEach(editor::put);
                        if (op.has("remove")) {
                            for (Map.Entry<String, JsonElement> names : op.getAsJsonObject("remove").entrySet()) {
                                TypeUrl type = TypeUrl.valueOf(names.getKey());
                                names.getValue().getAsJsonArray().forEach(name -> editor.remove(type.getMessageClazz(), name.getAsString()));
                            }
                        }
                        return editor;
                    });
                } else if (op.has("setGroup")) {
                    Snapshot.Editor editor = Snapshot.empty().edit();
                    resources(op, "resources").forEach(editor::put);
                    change.group(op.get("setGroup").getAsString(), editor.build());
                } else if (op.has("removeGroup")) {
                    change.removeGroup(op.get("removeGroup").getAsString());
                } else {
                    throw new IllegalArgumentException("unknown update op " + op);
                }
            }
        });
    }

    /** Checks each Envoy got exactly the responses the step expects of it, in order. */
    private void check(JsonObject step, String where) {
        Map<String, List<JsonObject>> expected = new HashMap<>();
        if (step.has("expect")) {
            for (JsonElement e : step.getAsJsonArray("expect")) {
                JsonObject response = e.getAsJsonObject();
                String envoy = response.has("envoy") ? response.get("envoy").getAsString()
                    : step.has("request") ? envoy(step).name : onlyEnvoy().name;
                expected.computeIfAbsent(envoyNamed(envoy).name, k -> new ArrayList<>()).add(response);
            }
        }
        for (FakeEnvoy envoy : envoys.values()) {
            if (envoy.failed != null) {
                throw new AssertionError(where + "\nthe server failed Envoy " + envoy.name + "'s stream: " + envoy.failed, envoy.failed);
            }
            List<JsonObject> want = expected.getOrDefault(envoy.name, List.of());
            List<Received> got = List.copyOf(envoy.received);
            envoy.received.clear();
            boolean matches = want.size() == got.size();
            for (int i = 0; matches && i < want.size(); i++) {
                matches = contents(want.get(i)).equals(got.get(i).contents());
            }
            if (!matches) {
                throw new AssertionError(String.format("%s%nEnvoy %s expected:%n%s%nbut got:%n%s", where, envoy.name,
                    describe(want.stream().map(this::contents).toList()), describe(got.stream().map(Received::contents).toList())));
            }
            for (int i = 0; i < want.size(); i++) {
                if (want.get(i).has("as") && LAST.matcher(want.get(i).get("as").getAsString()).matches()) {
                    throw new IllegalArgumentException("label " + want.get(i).get("as") + " means a response relative to the last");
                }
                if (want.get(i).has("as") && labels.put(want.get(i).get("as").getAsString(), got.get(i)) != null) {
                    throw new IllegalArgumentException("label " + want.get(i).get("as") + " is used twice");
                }
            }
        }
    }

    /** What a response should hold on this run's protocol: the base fields, with the protocol's own replacing them. */
    private Contents contents(JsonObject response) {
        TypeUrl type = type(response.get("type"));
        JsonObject fields = response.deepCopy();
        String protocol = delta ? "delta" : "sotw";
        if (response.has(protocol)) {
            response.getAsJsonObject(protocol).entrySet().forEach(e -> fields.add(e.getKey(), e.getValue()));
        }
        Map<String, Message> resources = new TreeMap<>();
        if (fields.has("resources")) {
            for (JsonElement resource : fields.getAsJsonArray("resources")) {
                Message message = parse(type, resource.getAsJsonObject());
                resources.put(nameOf(message), message);
            }
        }
        Set<String> removed = new TreeSet<>(strings(fields, "removed"));
        if (!delta && !removed.isEmpty()) {
            throw new IllegalArgumentException("a SotW response can't have removed: " + response);
        }
        return new Contents(type.getTypeUrl(), resources, removed);
    }

    private FakeEnvoy envoy(JsonObject step) {
        return step.has("envoy") ? envoyNamed(step.get("envoy").getAsString()) : onlyEnvoy();
    }

    private FakeEnvoy envoyNamed(String name) {
        FakeEnvoy envoy = envoys.get(name);
        if (envoy == null) {
            throw new IllegalArgumentException("no envoy " + name);
        }
        return envoy;
    }

    private FakeEnvoy onlyEnvoy() {
        if (envoys.size() != 1) {
            throw new IllegalArgumentException("there are several envoys, so say which");
        }
        return envoys.values().iterator().next();
    }

    private static TypeUrl type(JsonElement name) {
        return TypeUrl.valueOf(name.getAsString());
    }

    private static List<TypeUrl> types(JsonArray names) {
        List<TypeUrl> types = new ArrayList<>();
        names.forEach(name -> types.add(type(name)));
        return types;
    }

    private static List<String> strings(JsonObject object, String field) {
        List<String> strings = new ArrayList<>();
        if (object.has(field)) {
            object.getAsJsonArray(field).forEach(s -> strings.add(s.getAsString()));
        }
        return strings;
    }

    /** The resources of a field that lists them by type. */
    private static List<Message> resources(JsonObject object, String field) {
        List<Message> resources = new ArrayList<>();
        if (object.has(field)) {
            for (Map.Entry<String, JsonElement> ofType : object.getAsJsonObject(field).entrySet()) {
                TypeUrl type = TypeUrl.valueOf(ofType.getKey());
                ofType.getValue().getAsJsonArray().forEach(r -> resources.add(parse(type, r.getAsJsonObject())));
            }
        }
        return resources;
    }

    private static Message parse(TypeUrl type, JsonObject json) {
        Message.Builder builder = Internal.getDefaultInstance(type.getMessageClazz()).newBuilderForType();
        try {
            JsonFormat.parser().merge(json.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("not a " + type + " resource: " + json, e);
        }
        return builder.build();
    }

    /** A resource's name, as Envoy names it: its name, or a ClusterLoadAssignment's cluster_name. */
    private static String nameOf(Message resource) {
        FieldDescriptor name = resource.getDescriptorForType().findFieldByName("name");
        return (String) resource.getField(name != null ? name : resource.getDescriptorForType().findFieldByName("cluster_name"));
    }

    private static Message unpack(Any any) {
        TypeUrl type = TypeUrl.of(any.getTypeUrl());
        if (type == null) {
            throw new IllegalStateException("a resource of unknown type " + any.getTypeUrl());
        }
        try {
            return any.unpack(type.getMessageClazz());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String describe(List<Contents> responses) {
        if (responses.isEmpty()) {
            return "  nothing";
        }
        return responses.stream().map(r -> "  " + r).collect(Collectors.joining("\n"));
    }

    private static String print(Message message) {
        try {
            return PRINTER.print(message);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    /** What a response holds that a vector compares: its type, resources by name, and the names it removes. */
    private record Contents(String typeUrl, Map<String, Message> resources, Set<String> removed) {
        @Override
        public String toString() {
            TypeUrl known = TypeUrl.of(typeUrl);
            String type = known != null ? known.name() : typeUrl;
            String shown = resources.values().stream().map(VectorRun::print).collect(Collectors.joining(", ", "[", "]"));
            return type + " resources " + shown + (removed.isEmpty() ? "" : " removed " + removed);
        }
    }

    /** A response an Envoy got. */
    private record Received(TypeUrl type, String version, String nonce, Contents contents) {
    }

    private static Received received(String typeUrl, String version, String nonce, List<Message> resources, List<String> names, List<String> removed) {
        TypeUrl type = TypeUrl.of(typeUrl);
        if (type == null) {
            throw new IllegalStateException("a response of unknown type " + typeUrl);
        }
        Map<String, Message> byName = new TreeMap<>();
        for (int i = 0; i < resources.size(); i++) {
            String name = names.get(i);
            if (byName.put(name, resources.get(i)) != null) {
                byName.put(name + " (again)", resources.get(i)); // so it shows as a mismatch
            }
        }
        return new Received(type, version, nonce, new Contents(typeUrl, byName, new TreeSet<>(removed)));
    }

    /** An Envoy, played by the runner: it sends the requests the vector says, and keeps what it gets. */
    private final class FakeEnvoy {

        final String name;

        final List<Received> received = new ArrayList<>();

        @Nullable Throwable failed;

        /** The gRPC status code the server ended this Envoy's stream with, if it did. */
        @Nullable String ended;

        private @Nullable StreamObserver<DiscoveryRequest> sotw;

        private @Nullable StreamObserver<DeltaDiscoveryRequest> deltaStream;

        private boolean first = true;

        private boolean closing;

        // On this stream: the responses of each type, in order, and for SotW, the version last ACKed and the names wanted.
        private final Map<TypeUrl, List<Received>> history = new EnumMap<>(TypeUrl.class);

        private final Map<TypeUrl, String> accepted = new EnumMap<>(TypeUrl.class);

        private final Map<TypeUrl, Set<String>> names = new EnumMap<>(TypeUrl.class);

        FakeEnvoy(String name) {
            this.name = name;
        }

        void send(JsonObject step) {
            TypeUrl type = type(step.get("request"));
            List<String> subscribe = strings(step, "subscribe");
            List<String> unsubscribe = strings(step, "unsubscribe");
            List<String> kept = strings(step, "kept");
            boolean nack = step.has("nack");
            Received answering = step.has("ack") ? ref(type, step.get("ack").getAsString())
                : nack ? ref(type, step.get("nack").getAsString()) : null;
            nack = nack && answering != null; // replaying, a nack of no response is left out
            Status rejection = Status.newBuilder().setMessage("rejected, as the vector says").build();
            Node node = Node.newBuilder().setId(name).build();
            if (delta) {
                DeltaDiscoveryRequest.Builder request = DeltaDiscoveryRequest.newBuilder().setTypeUrl(type.getTypeUrl())
                    .addAllResourceNamesSubscribe(subscribe).addAllResourceNamesUnsubscribe(unsubscribe);
                kept.forEach(n -> request.putInitialResourceVersions(n, "kept")); // ports compare names only
                if (answering != null) {
                    request.setResponseNonce(answering.nonce());
                }
                if (nack) {
                    request.setErrorDetail(rejection);
                }
                if (first) {
                    request.setNode(node);
                }
                deliver(() -> deltaStream().onNext(request.build()));
            } else {
                if (!kept.isEmpty()) {
                    throw new IllegalArgumentException("kept is for delta only");
                }
                Set<String> wanted = names.computeIfAbsent(type, t -> new LinkedHashSet<>());
                wanted.addAll(subscribe);
                unsubscribe.forEach(wanted::remove);
                DiscoveryRequest.Builder request = DiscoveryRequest.newBuilder().setTypeUrl(type.getTypeUrl()).addAllResourceNames(wanted);
                // Each request answers the last response of its type, unless the step says which.
                Received answers = answering != null ? answering : back(type, 0);
                if (answers != null) {
                    request.setResponseNonce(answers.nonce());
                }
                if (answering != null && !nack) {
                    accepted.put(type, answering.version());
                }
                request.setVersionInfo(accepted.getOrDefault(type, ""));
                if (nack) {
                    request.setErrorDetail(rejection);
                }
                if (first) {
                    request.setNode(node);
                }
                deliver(() -> sotwStream().onNext(request.build()));
            }
            first = false;
        }

        /** Sends a request. A status the server throws ends the stream with it, as gRPC would. */
        private void deliver(Runnable send) {
            try {
                send.run();
            } catch (StatusRuntimeException e) {
                failed = e;
                ended = e.getStatus().getCode().name();
            }
        }

        void disconnect() {
            if (first) {
                throw new IllegalArgumentException(name + " has no stream to close");
            }
            closing = true;
            if (delta) {
                deltaStream().onCompleted();
            } else {
                sotwStream().onCompleted();
            }
            closing = false;
            sotw = null;
            deltaStream = null;
            first = true;
            history.clear();
            accepted.clear();
            names.clear();
        }

        private @Nullable Received ref(TypeUrl type, String ref) {
            Matcher relative = LAST.matcher(ref);
            boolean isRelative = relative.matches();
            Received response = !isRelative ? labels.get(ref) : back(type, relative.group(1) == null ? 0 : Integer.parseInt(relative.group(1)));
            if (response == null && replaying && isRelative) {
                return null;
            }
            if (response == null) {
                throw new IllegalArgumentException("no response " + ref + " of " + type + " to answer");
            }
            if (response.type() != type) {
                throw new IllegalArgumentException(ref + " is a " + response.type() + " response, not " + type);
            }
            return response;
        }

        /** The response of a type this many before the last on this stream, if there is one. */
        private @Nullable Received back(TypeUrl type, int n) {
            List<Received> ofType = history.getOrDefault(type, List.of());
            return n < ofType.size() ? ofType.get(ofType.size() - 1 - n) : null;
        }

        private void got(Received response) {
            received.add(response);
            history.computeIfAbsent(response.type(), t -> new ArrayList<>()).add(response);
        }

        private StreamObserver<DiscoveryRequest> sotwStream() {
            if (sotw == null) {
                sotw = ads.streamAggregatedResources(observer(r -> received(r.getTypeUrl(), r.getVersionInfo(), r.getNonce(),
                    r.getResourcesList().stream().map(VectorRun::unpack).toList(),
                    r.getResourcesList().stream().map(VectorRun::unpack).map(VectorRun::nameOf).toList(), List.of())));
            }
            return sotw;
        }

        private StreamObserver<DeltaDiscoveryRequest> deltaStream() {
            if (deltaStream == null) {
                deltaStream = ads.deltaAggregatedResources(observer((DeltaDiscoveryResponse r) -> received(r.getTypeUrl(), r.getSystemVersionInfo(),
                    r.getNonce(), r.getResourcesList().stream().map(Resource::getResource).map(VectorRun::unpack).toList(),
                    r.getResourcesList().stream().map(Resource::getName).toList(), r.getRemovedResourcesList())));
            }
            return deltaStream;
        }

        private <R> StreamObserver<R> observer(Function<R, Received> convert) {
            return new StreamObserver<>() {
                @Override
                public void onNext(R response) {
                    got(convert.apply(response));
                }

                @Override
                public void onError(Throwable t) {
                    failed = t;
                    ended = io.grpc.Status.fromThrowable(t).getCode().name();
                }

                @Override
                public void onCompleted() {
                    if (!closing) {
                        failed = new IllegalStateException("the server ended the stream");
                        ended = "OK";
                    }
                }
            };
        }
    }
}
