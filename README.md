# Envoy xDS

> Forked from [transferwise/wise-envoy-xds](https://github.com/transferwise/wise-envoy-xds), originally developed at Wise.

Envoy xDS is an Envoy ADS implementation. It provides the framework necessary to correctly sequence xDS updates towards
Envoy instances. See the envoy [xDS docs](https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol) for details
of the protocol.

This is not a service mesh control plane, it is a library that can be used to implement one.

## Why did you write this?

There wasn't a choice at the time. It was originally written at Wise, where the first production deployment was in
early 2018, making it one of the first xDS implementations to be deployed large scale to a production environment. It
went on to form the core of EGADS, Wise's service mesh control plane, and was open sourced as
[transferwise/wise-envoy-xds](https://github.com/transferwise/wise-envoy-xds). This repository is an independently
maintained fork of that project.

## Does it do anything special?

Its main features are:
* Make before break - It sequences the different discovery service updates to ensure they get applied in a safe
order, as per the xDS protocol spec;
* Easy to implement an incremental control plane;
* Highly customisable behaviour per client;
* Workarounds for various envoy behaviours/bugs.

## Does it scale?

In the largest cluster at Wise it ran about 25 network changes a second through it, with up to around 1000 envoys per instance,
resulting in peaks of emitting around 333 DeltaDiscoveryResponses per second for a single instance (plus handling the
corresponding acks from envoy.)
Is that good? I have no idea. But mostly its Pods ticked along at under half a core. This isn't very quantitative, is it?
But I guess the answer is - it scales well enough under non-trivial loads.

## Usage

envoy-xds-example contains a toy example ADS, documented [here](envoy-xds-example/README.md).
Any implementation will need to provide a dependency on a version of the Envoy API supported by the envoy version they
run, e.g. [net.woaf.jono.envoy:envoy-api](https://github.com/JonathanO/envoy-api-java), which is published for each
Envoy release.

### Snapshots (experimental)

`envoy-xds-snapshot` lets you describe the whole desired config instead of writing `IncrementalConfigBuilder`s.
Publish groups of resources, and views made of them, to a `SnapshotSource`, and serve them with
`SnapshotConfigBuilder.forTypes(...)`. Each change is still applied make-before-break: added and changed resources go
out in add order, deleted ones in remove order.

A group is a `Snapshot`: any resources, of any types. A view is an ordered list of groups, and a client sees every
resource in its view's groups; where two groups have a resource of the same type and name, the first one's wins. You
pick each client's view from the client details your `ClientConfigProvider` returned, so clients with the same details
share a view. Groups and views are named by keys of any type with equals and hashCode, e.g. strings, or your own
types, so the compiler stops a view name or a stray string being used as a group.

```java
SnapshotSource<ClientConfig, String, String> source = new SnapshotSource<>(client -> client.callingService());
AggregatedDiscoveryService<SnapshotUpdate<ClientConfig>, ClientConfig> ads = new AggregatedDiscoveryService<>(
    source,
    SnapshotConfigBuilder.forTypes(Cluster.class, ClusterLoadAssignment.class, Listener.class, RouteConfiguration.class),
    clientConfigProvider, ImmutableList.of(), MergingStateBacklog.factory(), metricsFactory);

// A service mesh: a group per service, and a view per caller of its own listener and routes, then what it calls.
source.update(c -> c
    .group("svc-b", g -> g.put(clusterB).put(endpointsB))
    .group("caller-a", g -> g.put(listener).put(routesForA).put(clusterBWithLongerTimeout)) // overrides svc-b's cluster
    .view("caller-a", "caller-a", "svc-b"));

// Only callers of svc-b are woken, and they compare only the names touched.
source.update(c -> c.group("svc-b", g -> g.put(newEndpointsB)));
```

Everyone seeing the same config is one group in one view: `new SnapshotSource<>(client -> "all")` and
`source.update(c -> c.view("all", "all").group("all", g -> g.put(cluster)))`. Edit groups (`group(name, edit)`) so only
the names you touch are compared; a group replaced whole (`group(name, snapshot)`) compares everything in it. Groups
and views changed in one `update()` reach each client as one change, so a new cluster in one group arrives before the
route to it in another. A client whose view doesn't exist sees nothing until you add it, which you can do when it
connects, from an `XdsEventListener`.

`SnapshotExample` in [envoy-xds-example](envoy-xds-example/README.md) is a runnable version of the toy example built this way.

### Threads

Each client's updates are delivered in order, one at a time, on a shared pool with one thread per core, so thousands
of Envoys don't need thousands of threads. Your config builders run there. If they block, give
`SnapshotSource` your own executor (`new SnapshotSource<>(viewOf, executor)`), or wrap your listeners in
`SerialEventListener` with one if you write your own `ClusterEventSource`. `SnapshotSource`'s executor must run tasks on
threads of its own: with a same-thread one such as `MoreExecutors.directExecutor()` it deadlocks.

### When Envoy reconnects

Envoy keeps its config while it's disconnected, and re-subscribes to everything at once when it reconnects. We answer
route and listener subscriptions only once the clusters and endpoints they may use are ACKed. With delta, we remove
what Envoy kept that no longer exists in remove order, once the types that may still use it have been answered and
ACKed. If a client doesn't get every type from us, say so with `XdsConfig.subscribesTo`, e.g. `Set.of(TypeUrl.CDS,
TypeUrl.EDS)` for an Envoy with static routes, or its removals wait up to 30 seconds for types it never asks for. This
matters most when you serve types only some Envoys use, such as scoped routes (SRDS) or VHDS: we can't tell an Envoy
that won't ask for them from one that hasn't yet, as Envoy only asks once the listeners and routes naming them arrive.
`subscribesTo` only changes what removals wait for: an Envoy asking for a type left out still gets it.
SotW can't do the same for clusters: its CDS answer is the whole set, so a cluster Envoy kept goes at once. Prefer delta.

### When Envoy rejects config

Once an Envoy NACKs, we stop sending it changes on that stream. It keeps serving its last good config, which goes
stale. We still answer its new subscriptions. On SotW each answer is the whole set, so for the type Envoy rejected
it repeats what was rejected: Envoy rejects it again and keeps what it has. Rolling it back instead would undo
whatever part of the rejected answer Envoy did take. Retrying would only be rejected again, and dropping the stream makes Envoy reconnect straight into the same
config, in a loop. `XdsEventListener.onClientNack` tells you it happened, so alert on it. `ClientHandle.resync()` closes
the stream, so Envoy reconnects and syncs from scratch. Call it once the config is fixed, or add `ResyncAfterNack` to
your listeners to retry on a backoff:

```java
new ResyncAfterNack<>(scheduler, Duration.ofMinutes(1), Duration.ofMinutes(30))
```

Pick the longest delay you're willing to have an Envoy serve stale endpoints for.

### Testing against a real Envoy

`envoy-xds-testing` runs your control plane against a real Envoy in Docker. It records the xDS conversation and
checks it, including make before break, and checks Envoy's stats and log. It works for any Java control plane, not
just this library's, and includes a conformance suite for xDS libraries. See [its README](envoy-xds-testing/README.md).

## Versioning

The library has a compile-only dependency on net.woaf.jono.envoy:envoy-api, built from the oldest Envoy release the
e2e tests cover. It relies only on classes and fields that are common to all versions of the v3 API, there is no
dependency on anything known to be envoy version specific.

Users of the library can depend on the net.woaf.jono.envoy:envoy-api version matching the Envoy they run. Other builds
of the same protos, such as com.transferwise.envoy:envoy-api or java-control-plane's io.envoyproxy.controlplane:api,
should also work, as the generated packages are the same.

The e2e tests run against Envoy 1.36 to 1.40, over both SotW and delta ADS.
See Envoy's deprecation rules for specifics of what version you should choose.

In the future it's possible this library will support new API versions, or that the v3 API will change in some way that breaks
compatibility. If that happens we'll have to introduce some compatibility shim.

## Architecture

See [here](docs/ARCHITECTURE.md).

## History

- Oct 2017 - PoC service mesh control plane implementing the original envoy REST api (API v1)
- Nov 2017 - Rewritten to use the xDS API
- Feb 2018 - Merged into our service registry (eureka-service) codebase, first production deployment!
- Apr 2019 - Extracted into its own service, EGADS
- Mar 2020 - Major overhaul to focus on the delta protocol
- Nov 2020 - Dual API v2 and API v3 support (v2 later removed, but we can do it again for v4...)
- May 2022 - Split xds package out of EGADS to allow open source release

## Why is the test coverage so poor?
Since this lived in the EGADS repo for many years and was not genericised, much coverage relied on parts of the
codebase that are not part of this open sourcing attempt. Given time they'll be rewritten and added to this library.
Contributions in this area are also especially welcome!

## Acknowledgements

Because this has moved between repos, blame will claim @JonathanO wrote all of it. It's nearly correct in terms of
LoC, but nothing like this gets written in a vacuum. This would not have been possible without much
collaborative white-boarding and passionate exchanges of ideas in Google Docs. It took us a while to work out what good
looked like. The people who've worked on the proprietary service bridges, cluster manager and config builders that
make up the rest of EGADS - this code would have been useless without your efforts! Thank you too to all the people who've
discussed and reviewed PRs over the years, and the leads who believed it was worth investing time in.

## License
Copyright 2022 Wise Plc.
Copyright 2026 Jonathan Oddy.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
