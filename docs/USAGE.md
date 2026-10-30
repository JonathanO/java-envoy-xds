# Using envoy-xds

This is a guide to building a control plane with envoy-xds. For how it works inside, see
[ARCHITECTURE.md](ARCHITECTURE.md).

envoy-xds serves Envoy's Aggregated Discovery Service (ADS): one gRPC stream per Envoy, carrying every xDS resource type,
over either protocol. With SotW (state of the world) each response carries every resource of a type; with delta it
carries only what changed. You decide what config each Envoy should have, and the library sends it in a safe order,
make before break: a resource reaches Envoy before anything that refers to it, and leaves only after nothing does.

## 1. Dependencies

The modules are published to Maven Central under `net.woaf.jono.envoy`:

- `envoy-xds-core`: the ADS service. You need this.
- `envoy-xds-snapshot`: snapshot-style config, so you describe the config you want instead of writing config builders.
- `envoy-xds-testing`: for testing your control plane against a real Envoy.

`envoy-xds-snapshot` and `envoy-xds-testing` are new since 2.0.0: see the [CHANGELOG](../CHANGELOG.md).

You also need the Envoy API, the generated protobuf classes, which the library only has as a compile-only dependency.
Use the one matching the Envoy you run, e.g. [net.woaf.jono.envoy:envoy-api](https://github.com/JonathanO/envoy-api-java),
published for each Envoy release. Other builds of the same protos, such as `com.transferwise.envoy:envoy-api` or
java-control-plane's `io.envoyproxy.controlplane:api`, should also work, as the generated packages are the same. You
need a gRPC server transport too, such as `grpc-netty-shaded`.

```groovy
dependencies {
    implementation 'net.woaf.jono.envoy:envoy-xds-core:<version>'
    implementation 'net.woaf.jono.envoy:envoy-xds-snapshot:<version>'
    implementation 'net.woaf.jono.envoy:envoy-api:<your Envoy version>'
    runtimeOnly 'io.grpc:grpc-netty-shaded:<gRPC version>'
    testImplementation 'net.woaf.jono.envoy:envoy-xds-testing:<version>'
}
```

The library needs Java 17 or later.

## 2. A quick start with snapshots

A `SnapshotSource` holds the config you want, as groups of resources, and views made of them. Serve it with a
`SnapshotConfigBuilder` for each type:

```java
// Every Envoy sees view "all".
SnapshotSource<String, String, String> source = new SnapshotSource<>(details -> "all");

AggregatedDiscoveryService<SnapshotUpdate<String>, String> ads = new AggregatedDiscoveryService<>(
    source,
    SnapshotConfigBuilder.forTypes(Cluster.class, ClusterLoadAssignment.class, Listener.class, RouteConfiguration.class),
    node -> XdsConfig.of(node.getId()),  // the client details: here, just its node id
    ImmutableList.of(),                  // event listeners
    MergingStateBacklog.factory(),       // merge changes that arrive while an Envoy is busy
    DiscoveryServiceManagerMetrics.METRICS_DISABLED);

Server server = ServerBuilder.forPort(6566).addService(ads).build().start();

// View "all" is group "all", which has a cluster, its endpoints, a listener and its routes.
source.update(c -> c
    .view("all", "all")
    .group("all", g -> g.put(cluster).put(endpoints).put(listener).put(routes)));
```

Point Envoy's `ads_config` at the server, with `api_type: GRPC` for SotW or `DELTA_GRPC` for delta, and set
`ads: {}` as the config source of each type it should get from us. The example's
[envoy-delta.yaml](../envoy-xds-example/src/main/resources/envoy-delta.yaml) does this for clusters and routes.

Then change the config with `update`. Each update reaches every Envoy as one change, make before break: added and
changed resources go out in add order, removed ones in remove order. A new cluster arrives before the route that uses
it, and a removed one goes only once the routes have stopped using it.

### Groups and views

A group is a `Snapshot`: any resources, of any types. A view is an ordered list of groups, and a client sees every
resource in its view's groups; where two groups have a resource of the same type and name, the first one's wins.
Groups and views are named by keys of any type with `equals` and `hashCode`, e.g. strings, or your own types, so the
compiler stops a view name or a stray string being used as a group.

```java
// A service mesh: a group per service, and a view per caller of its own listener and routes, then what it calls.
source.update(c -> c
    .group("svc-b", g -> g.put(clusterB).put(endpointsB))
    .group("caller-a", g -> g.put(listener).put(routesForA).put(clusterBWithLongerTimeout)) // overrides svc-b's cluster
    .view("caller-a", "caller-a", "svc-b"));

// Only callers of svc-b are woken, and they compare only the names touched.
source.update(c -> c.group("svc-b", g -> g.put(newEndpointsB)));
```

Edit groups (`group(name, edit)`) so only the names you touch are compared; a group replaced whole
(`group(name, snapshot)`) compares everything in it. Putting a resource equal to the one already there changes nothing.
`put(resource)` names a resource as Envoy does, by its `name`, or its `cluster_name` for a `ClusterLoadAssignment`; use
`put(name, resource)` for anything else. Groups and views changed in one `update()` reach each client as one change, so
a new cluster in one group arrives before the route to it in another. `source.group(name)` and `source.view(name)`
return what was last published.

## 3. Per-client config

### `ClientConfigProvider`

When an Envoy sends its first request, the library calls your `ClientConfigProvider` with the `Node` it sent: its id,
cluster, locality, metadata and so on. It returns an `XdsConfig`, which holds:

- the **client details**: anything you like, passed to your config builders with every call, and to your snapshot
  source when the client subscribes;
- options for this client, described in [section 5](#5-xdsconfig-options).

```java
record Caller(String service) { }

ClientConfigProvider<Caller> provider = node -> XdsConfig.of(new Caller(node.getCluster()));
```

It's called once per stream, on the gRPC thread, so it may read the gRPC `Context`, e.g. for the client's certificate.

### Picking views

`SnapshotSource` picks each client's view from its details, with the function you give it. Clients with the same view
share it: one update, and one comparison per resource, serves all of them.

```java
SnapshotSource<Caller, String, String> source = new SnapshotSource<>(Caller::service);
```

The function must not return null. A client whose view doesn't exist sees nothing until you add it. You can add it when
the client connects, from an `XdsEventListener`, which is called before the client subscribes:

```java
XdsEventListener<Caller> addViews = new XdsEventListener<>() {
    @Override
    public void onNewClient(ClientHandle client, Node node, XdsConfig<Caller> config) {
        String service = config.clientDetails().service();
        source.update(c -> c.view(service, service, "shared")); // no change if it's already there
    }

    @Override
    public void onClientDisconnected(ClientHandle client, Node node) {
    }
};
```

`SnapshotExample` in [envoy-xds-example](../envoy-xds-example/README.md) does this, with a view per listen port.

## 4. Writing your own `ClusterEventSource` and `IncrementalConfigBuilder`s

The snapshot module compares desired config. If you already know what changed, e.g. from your service registry's
events, you can skip the comparing: write your own source of state updates, and a config builder per type that turns an
update into resources, as `XdsExample` in envoy-xds-example does. Otherwise, use snapshots.

### The state update

The update type is yours. A config builder must be able to work out from it both what changed and the whole state. The
simplest is the state before and after. Make it `Mergeable`, so `MergingStateBacklog` can merge updates that arrive
while an Envoy is busy:

```java
record Mesh(ImmutableSet<String> before, ImmutableSet<String> after) implements Mergeable<Mesh> {
    @Override
    public Mesh merge(Mesh next) {
        return new Mesh(before, next.after()); // the oldest before, the newest after
    }
}
```

### The source

Each Envoy's stream subscribes once. `subscribe` returns the current state as an update, and later updates go to the
listener. They must follow on from each other, so no change is lost between the two. Wrap each listener in a
`SerialEventListener`, so your broadcast only hands updates to an executor and can't deadlock with a client connecting
(see [section 9](#9-threads)). `unsubscribe` must close the listener.

```java
final class MeshSource implements ClusterEventSource<Mesh> {

    private final Map<ClusterManagerEventListener<Mesh>, SerialEventListener<Mesh>> listeners = new ConcurrentHashMap<>();

    private ImmutableSet<String> services = ImmutableSet.of();

    @Override
    public synchronized Mesh subscribe(ClusterManagerEventListener<Mesh> listener) {
        listeners.computeIfAbsent(listener, SerialEventListener::new);
        return new Mesh(ImmutableSet.of(), services);
    }

    @Override
    public void unsubscribe(ClusterManagerEventListener<Mesh> listener) {
        SerialEventListener<Mesh> old = listeners.remove(listener);
        if (old != null) {
            old.close();
        }
    }

    synchronized void set(ImmutableSet<String> next) {
        Mesh change = new Mesh(services, next);
        services = next;
        listeners.values().forEach(l -> l.onNetworkChange(change)); // only hands it to an executor
    }
}
```

To give clients different config, override `subscribe(listener, clientDetails)` too: the library calls that one, with
the details your `ClientConfigProvider` returned. By default it calls `subscribe(listener)`.

### The config builders

An `IncrementalConfigBuilder` serves one type. Each change calls it twice: `addOrder` for the add round, and
`removeOrder` for the remove round. Return new and changed resources from `addOrder`, and removals from `removeOrder`.
Some changes are in effect removals, so think about which round each belongs in. Each method gets a predicate of the
names Envoy subscribed to: return only those, or the library logs a warning.

`getResourcesAddOrder` and `getResourcesRemoveOrder` return the whole state, to answer Envoy when it subscribes to
something. The add-order one is the state part way through a change: everything new added, nothing old removed yet.
The remove-order one is the state once the change is done. Mid-change, Envoy is answered from the add-order one until
the type's remove round has gone, and after a NACK stops the change.

```java
final class ClusterBuilder implements IncrementalConfigBuilder<Cluster, Mesh, Caller> {

    @Override
    public Response<Cluster> addOrder(Mesh diff, Predicate<String> subscribed, Caller caller) {
        return new Response<>(clusters(Sets.difference(diff.after(), diff.before()), subscribed), List.of());
    }

    @Override
    public Response<Cluster> removeOrder(Mesh diff, Predicate<String> subscribed, Caller caller) {
        return new Response<>(List.of(), Sets.difference(diff.before(), diff.after()).stream().filter(subscribed).toList());
    }

    @Override
    public Resources<Cluster> getResourcesAddOrder(Mesh state, Predicate<String> subscribed, Caller caller) {
        return new Resources<>(clusters(Sets.union(state.before(), state.after()), subscribed)); // nothing removed yet
    }

    @Override
    public Resources<Cluster> getResourcesRemoveOrder(Mesh state, Predicate<String> subscribed, Caller caller) {
        return new Resources<>(clusters(state.after(), subscribed));
    }

    @Override
    public Class<Cluster> handlesType() {
        return Cluster.class;
    }

    private static List<NamedMessage<Cluster>> clusters(Set<String> names, Predicate<String> subscribed) {
        return names.stream().filter(subscribed).map(name -> NamedMessage.of(Cluster.newBuilder().setName(name).build())).toList();
    }
}
```

Pass your source and builders to `AggregatedDiscoveryService` as in the quick start:

```java
new AggregatedDiscoveryService<>(mesh, ImmutableList.of(new ClusterBuilder()), provider, ImmutableList.of(),
    MergingStateBacklog.factory(), DiscoveryServiceManagerMetrics.METRICS_DISABLED);
```

Returning only what changed keeps delta responses small, but there's no harm, just overhead, in returning unchanged
resources. A type you give no builder for is still answered, with nothing.

### Backlogs

The `StateBacklogFactory` makes a backlog per stream, which holds the updates that arrive while that Envoy is busy with
a push. `MergingStateBacklog.factory()` merges them, so a slow Envoy gets one push, not many. Use it unless you have a
reason not to. `QueueingStateBacklog.factory()` keeps every update in order, which can be useful in tests, as it
doesn't depend on how quickly Envoy ACKs.

## 5. `XdsConfig` options

Both options are about what a client waits for, mostly after it reconnects. Envoy keeps its config while it's
disconnected, and re-subscribes to everything at once when it reconnects. We answer route and listener subscriptions
only once the clusters and endpoints they may use are sent and ACKed. A subscription that arrives while a change is
going out gets the change's add round: what the change removes stays until its type's remove round, so nothing goes
before what may refer to it. With delta, we remove what Envoy kept that no longer
exists in remove order, once the types that may still use it have been answered and ACKed. SotW can't do the same for
clusters: its CDS answer is the whole set, so a cluster Envoy kept goes at once. Prefer delta.

Start from `XdsConfig.of(details)`, which gets every type we serve, with no delay:

```java
XdsConfig.of(caller)
    .withSubscribesTo(Set.of(TypeUrl.CDS, TypeUrl.EDS, TypeUrl.RDS))
    .withDelayUpdatesUntilAckOf(TypeUrl.RDS);
```

- `withSubscribesTo(types)`: the types this client gets from us. Null, the default, means every type you have a config
  builder for. When Envoy reconnects we remove what it kept that no longer exists, but only once the types that may still
  refer to it have been answered and ACKed. If a client never asks for one of those, its removals wait for it: at least
  30 seconds after it connects, then until its next request or your next change. So leave out types a client doesn't
  get from us, e.g. LDS for an Envoy with static listeners, or SRDS and VHDS where only some Envoys use them: we can't
  tell an Envoy that won't ask for them from one that hasn't yet. `subscribesTo` only changes what we wait for: an Envoy
  asking for a type left out still gets it.
- `withDelayUpdatesUntilAckOf(type)`: hold back changes to this client until it first ACKs that type. It works around
  Envoy problems with updates during its start up, such as [envoy#16035](https://github.com/envoyproxy/envoy/issues/16035),
  where endpoint updates interfere with Envoy initialising. RDS is a good choice, as Envoy only ACKs it once its
  clusters are ready. Only set it to a type this client subscribes to. If `subscribesTo` leaves the type out it's
  ignored; otherwise, with no ACK of it after 30 seconds, changes go anyway, with a warning, at the next request or
  change.

## 6. Phases

Make before break orders adds and removals by what refers to what. Some changes need an order it can't see: a cluster
that drops a subset a route still selects, say, must wait until the route stops using it. Split such a change into
phases with `then()`, and each client gets each phase, and ACKs it, before the next:

```java
source.update(c -> c
    .group("routing", g -> g.put(routesWithoutSubset))
    .then()
    .group("svc-b", g -> g.put(clusterBWithoutSubset)));
```

Each phase is make before break within itself. With `MergingStateBacklog`, updates queued for a busy client are never
merged across phases. Only clients whose view changes in a later phase wait. A client connecting meanwhile gets the
end result.

With your own source, do the same by publishing each phase as its own update, whose `mergesWithLater()` returns false
for every phase but the last.

## 7. NACKs and resync

Once an Envoy NACKs, we stop sending it changes on that stream. It keeps serving its last good config, which goes
stale. We still answer its new subscriptions, keeping what the rejected change would remove, as those removals never
went out. On SotW each answer is the whole set, so for the type Envoy rejected
it repeats what was rejected: Envoy rejects it again and keeps what it has. Rolling it back instead would undo
whatever part of the rejected answer Envoy did take. Retrying would only be rejected again, and dropping the stream
makes Envoy reconnect straight into the same config, in a loop.

`XdsEventListener.onClientNack` tells you it happened, so alert on it. `ClientHandle.resync()` closes the stream, so
Envoy reconnects and syncs from scratch. Call it once the config is fixed, or add `ResyncAfterNack` to your listeners
to retry on a backoff:

```java
ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
ImmutableList<XdsEventListener<Caller>> listeners = ImmutableList.of(
    new ResyncAfterNack<>(scheduler, Duration.ofMinutes(1), Duration.ofMinutes(30)),
    alertOnNack);
```

It resyncs a stream `initialDelay` after its first NACK, and doubles the wait each time the same node NACKs again after
a resync, up to `maxDelay`. A node that goes `2 * maxDelay` without a NACK starts again from `initialDelay`. Pick the
longest delay you're willing to have an Envoy serve stale endpoints for.

## 8. Events, metrics and logging

### Events

An `XdsEventListener` hears of each client:

- `onNewClient(handle, node, config)`: its first request arrived. The `ClientHandle` identifies the stream until it
  disconnects.
- `onClientDisconnected(handle, node)`: the stream ended.
- `onClientNack(handle, node, typeUrl, errorDetail)`: it rejected config. Optional.

```java
XdsEventListener<Caller> alertOnNack = new XdsEventListener<>() {
    @Override
    public void onNewClient(ClientHandle client, Node node, XdsConfig<Caller> config) {
    }

    @Override
    public void onClientDisconnected(ClientHandle client, Node node) {
    }

    @Override
    public void onClientNack(ClientHandle client, Node node, TypeUrl type, Status error) {
        log.error("{} rejected {}: {}", node.getId(), type, error.getMessage());
    }
};
```

They're called on the stream's thread, holding its lock, so keep them quick. An exception from `onNewClient` or
`onClientNack` fails the stream.

### Metrics

`AggregatedDiscoveryService` takes a `Supplier<DiscoveryServiceManagerMetrics>`, called once per stream. Every method
does nothing by default; `DiscoveryServiceManagerMetrics.METRICS_DISABLED` records nothing at all.

- `onPushBegin()` and `onPushComplete()`: a change started going to this Envoy, and the Envoy has ACKed all of it (or
  NACKed, and we stopped). The time between them is how long a change takes to reach it.
- `onAwaitingAck()` and `onMessageAcked()`: an answer to one of Envoy's requests, or a removal after a reconnect, was
  sent, and was ACKed. A change's own responses aren't counted here.
- `close()`: the stream ended.

```java
final class PushTimer implements DiscoveryServiceManagerMetrics {

    private final LongConsumer recordNanos;

    private long began;

    PushTimer(LongConsumer recordNanos) {
        this.recordNanos = recordNanos;
    }

    @Override
    public void onPushBegin() {
        began = System.nanoTime();
    }

    @Override
    public void onPushComplete() {
        recordNanos.accept(System.nanoTime() - began);
    }
}

Supplier<DiscoveryServiceManagerMetrics> metrics = () -> new PushTimer(histogram::record);
```

Each instance is only called with its stream's lock held. If instances share storage, such as a histogram, that must
be thread safe.

### Logging

The library logs through SLF4J, so add the binding you use. Every record logged while working for a client carries
its node id in the MDC as `node`: show it with `%X{node}` in a logback pattern, as envoy-xds-example's logback.xml
does. Things to watch for:

- warn `Client ... rejected ..., so we'll send it no more changes on this stream`: a NACK;
- error `Failed to push an update to ..., so closing its stream`: a config builder threw;
- warn `Config builder for ... returned unasked for ...`: a builder ignored the subscribed-names predicate;
- warn `No answer to or ACK of ... after 30s` and `No ACK of ... after 30s`: a client didn't ask for a type you said it
  would, in `subscribesTo` or `delayUpdatesUntilAckOf`;
- warn `... asked for unknown type URL ...`: Envoy asked for a type we don't serve, such as ECDS. It's left unanswered.

## 9. Threads

Each client's updates are delivered in order, one at a time, on a shared pool with one thread per core, so thousands
of Envoys don't need thousands of threads. Your config builders run there when a change goes out, and on gRPC's
threads when they answer Envoy's requests or carry on a change after an ACK. If they block, e.g. on a database, give
`SnapshotSource` your own executor:

```java
SnapshotSource<Caller, String, String> source = new SnapshotSource<>(Caller::service, Executors.newFixedThreadPool(32));
```

If you write your own `ClusterEventSource`, wrap your listeners in `SerialEventListener`, with your executor if you
like (`new SerialEventListener<>(executor, listener)`). `SnapshotSource`'s executor must run tasks on threads of its
own: with a same-thread one, such as `MoreExecutors.directExecutor()`, it deadlocks.

A source must not call a listener while holding a lock that its `subscribe` or `unsubscribe` also take: a stream holds
its own lock while it subscribes, and takes it when it gets an update, so a client connecting while you broadcast
would deadlock. A broadcast must also cope with a listener unsubscribing during it, as a stream whose update fails
does. `SerialEventListener` takes care of both.

`ClientHandle.resync()` is safe to call from any thread.

## 10. Testing against a real Envoy

`envoy-xds-testing` runs your control plane against a real Envoy in Docker. It records the xDS conversation and
checks it, including make before break, and checks Envoy's stats and log. It works for any Java control plane, not
just this library's, and includes a conformance suite for xDS libraries. See [its README](../envoy-xds-testing/README.md).

In short: start your gRPC server on a free port with a `ConversationLogger` interceptor, start an `EnvoyContainer` with
a bootstrap config pointing at it, wait for the initial sync with `Settled`, take a `Checkpoint`, make a change the way
your users would, wait for it to settle, then `checkpoint.assertSane()`.

`Settled.settled(conversation, hook)` can use your control plane's idea of whether a stream is idle, a `Settled.Hook`.
The metrics above give you one: idle is no push in progress, no answer awaiting an ACK, and nothing for a moment.
`DiscoveryServiceManagerEventListener` in [envoy-xds-e2e-tests](../envoy-xds-e2e-tests) is one, which you can copy.

## 11. Examples

[envoy-xds-example](../envoy-xds-example/README.md) is a toy ADS driven from the command line: `add` and `remove`
services and their endpoints, and Envoy routes to them.

- `XdsExample` uses hand-written config builders (`ClusterConfigBuilder`, `ClusterLoadAssignmentConfigBuilder`,
  `RouteConfigurationConfigBuilder`) and its own source (`ClusterManager`), with `SimpleUpdate` as the state update.
- `SnapshotExample` is the same CLI on the snapshot module. It keeps the clusters and endpoints in one group, and gives
  each listen port a group with its route config and a view, made when the first Envoy on that port connects.

Neither is production ready. A real control plane would watch your service registry, and wire up metrics.
