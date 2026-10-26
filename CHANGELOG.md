# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
* Experimental `envoy-xds-snapshot` module: snapshot-style config with automatic make-before-break ordering
* `envoy-xds-testing` module: helpers for testing a control plane against a real Envoy, and an xDS library conformance suite
* Make before break across a reconnect: `wasMakeBeforeBreak(fromMessage, before)`, `Checkpoint.assertSaneAfterReconnect(before)`, and the `changesWhileDisconnected` conformance scenario
* `Checkpoint` and `EnvoyChecks.assertRoutedVia` in `envoy-xds-testing`: one call to check everything since a mark, and which cluster served a request
* `XdsMatchers` in `envoy-xds-testing`: Hamcrest matchers for xDS resources, recorded messages, their order, and what Envoy holds
* `XdsEventListener.onClientNack`, `ClientHandle.resync()`, and `ResyncAfterNack` to resync an Envoy that NACKed on a backoff
* JSpecify nullness annotations: every package is `@NullMarked`, so anything that may be null says `@Nullable`. NullAway checks this strictly at build time, and Error Prone and javac warnings now fail the build
* Error Prone checks locking and immutability at build time: fields shared between threads are `@GuardedBy` their lock, and `Snapshot` and `NamedMessage` are `@Immutable`
* Results of our methods must be used, unless marked `@CanIgnoreReturnValue` (`SnapshotSource.update` and `publish` are)
* Conformance scenarios for scoped routes (SRDS) and on-demand discovery: `scopedRoutes`, `onDemandScopes`, `onDemandClusters` (ODCDS) and `onDemandVirtualHosts` (VHDS). The make-before-break checks now follow scoped route configs to their route configs, and VHDS virtual hosts to their clusters
* `ClusterEventSource.subscribe(listener, clientDetails)`: the core now tells your source which client is subscribing, so it can choose what that client sees. It defaults to `subscribe(listener)`, so existing sources need no change

### Changed
* Built and tested against `net.woaf.jono.envoy:envoy-api` 1.36.0 (from [envoy-api-java](https://github.com/JonathanO/envoy-api-java), published for each Envoy release) instead of `com.transferwise.envoy:envoy-api` 1.17.0. It's still `compileOnly`, so bring the envoy-api you already use
* `delayUpdatesUntilAckOf` no longer blocks updates forever when the client never ACKs that type: it's ignored if `subscribesTo` leaves the type out, and otherwise gives up after 30 seconds with a warning
* e2e tests run on Envoy 1.36 to 1.40, the releases upstream supports, instead of also 1.17 to 1.24
* **Breaking (unreleased API):** `XdsConformanceTest` runs its own matrix: every scenario on both protocols (or `protocols()`), on Envoy 1.36 and 1.40, with and without rate limiting. Libraries implement `newControlPlane()` once, instead of a subclass per variant, and record deviations with `knownDeviations()`.
* **Breaking:** after Envoy NACKs, we stop sending it changes on that stream, for both SotW and delta. Envoy keeps its last good config until it reconnects. Previously delta dropped the stream (Envoy then reconnects in a tight loop), and SotW stalled without saying so while its backlog grew. `ClientNackException` is gone.
* When Envoy reconnects, it re-subscribes to everything at once. Route and listener answers now wait until the clusters and endpoints they may use are ACKed. Stale resources Envoy kept (delta) are removed in remove order, once the types that may still use them have been answered and ACKed, without needing `delayUpdatesUntilAckOf`. `XdsConfig.subscribesTo` says which types a client gets from us, so its removals don't wait for types it never asks for; they wait at most 30 seconds anyway. On SotW, a cluster Envoy kept is still removed straight away: the CDS answer is the whole set.
* `TypedEnvoyConfigDump` sections Envoy left out of the dump are empty instead of null
* Dependencies: core and snapshot expose Guava, protobuf-java, grpc-stub (core) and error_prone_annotations as `api`, since their types are in our API. spotbugs-annotations and javax.annotation-api are no longer dependencies. `./gradlew buildHealth` reports unused and undeclared dependencies
* **Breaking:** no more Lombok. `XdsConfig`, `NodeConfig`, `CommonDiscoveryRequest`, `NamedMessage`, `Response`, `Resources` and `TypedEnvoyConfigDump` are records, so their accessors drop the `get`/`is` prefix (`name()`, `addAndUpdates()`, `clientDetails()`). Their builders are gone: use `XdsConfig.of(details)`, with `withDelayUpdatesUntilAckOf` and `withSubscribesTo`; `new Response<>(addAndUpdates, removes)` or `Response.empty()`; `new Resources<>(resources)` or `Resources.empty()`; `TypedEnvoyConfigDump.of(dump)`. The unused `Response.merge()`, `Resources.merge()` and `Resources.getByName()` are removed. `XdsConfig.delayUpdatesUntilAckOf` and `subscribesTo` are now marked `@Nullable`, as documented
* **Breaking:** clients no longer get a thread each. `SnapshotSource` delivers updates on a shared pool with one thread per core, or on your own `Executor` (`new SnapshotSource<>(overlay, executor)`). `QueueingMergingEventListener`, `QueueingEventListener` and `ThreadedQueuingEventListener` are replaced by `SerialEventListener`, which delivers each client's updates in order on an executor and doesn't merge: updates that arrive while a client is busy are merged by its `StateBacklog`, so use `MergingStateBacklog` to keep merging
* **Breaking:** the protocol machinery moved to `net.woaf.jono.envoy.xds.internal` (and `.internal.delta`, `.internal.sotw`), which is not API and may change in any release: `DiscoveryServiceManager`, `CommonDiscoveryStreamObserver`, `NodeConfig`, `CommonDiscoveryRequest`, the discovery services and their factories. Use `AggregatedDiscoveryService`; its constructor taking the per-protocol services is gone
* Every public class and method in core, snapshot and testing has Javadoc, and the Javadoc build fails on warnings
* **Breaking:** removed `XdsConfig.silentNacks`. Every NACK now freezes the stream, and is logged at warn; use `onClientNack` to act on it.

### Fixed
* Scoped route configs (SRDS) are removed before the route configs they name, not after, so a scope never points at a route config Envoy no longer has
* `ResyncAfterNack` forgets nodes that have gone 2 * maxDelay without a NACK, instead of keeping a backoff for every node id it ever saw
* A request that arrives after we closed the stream to resync Envoy is ignored, instead of throwing and logging a misleading warning
* SotW: after Envoy NACKs, a later subscription change no longer sends the rest of the rejected change. Its answer covers only the names newly asked for
* SRDS is a wildcard type, as Envoy subscribes to it with no names. Previously that first request went unanswered, so Envoy waited out its initial fetch timeout without scoped routes
* Delta: names subscribed explicitly while subscribed to `*` are answered, with `removed_resources` for names that don't exist
* SotW: an explicit `*` subscription, alone or with names, no longer closes the stream, and names sent alongside `*` stay subscribed after leaving wildcard
* SotW: a NACK whose `version_info` isn't numeric no longer fails the stream
* A request for an xDS type we don't serve (e.g. ECDS) is logged and left unanswered, instead of closing the stream
* Normal client disconnects (`CANCELLED`) are no longer logged as warnings
* Each resource is packed into an `Any` once, instead of once per client
* SotW: a request that lists a name twice no longer leaves names it dropped subscribed
* `Snapshot.Editor.put` and `remove` can be called for their effect alone: with Error Prone, filling an editor in a loop no longer fails `CheckReturnValue`

## [2.0.0] - 2026-10-16
### Changed
* Forked from [transferwise/wise-envoy-xds](https://github.com/transferwise/wise-envoy-xds)
* **Breaking:** Java packages renamed from `com.transferwise.envoy` to `net.woaf.jono.envoy`
* **Breaking:** Maven coordinates changed from `com.transferwise.envoy:wise-envoy-xds-core` to `net.woaf.jono.envoy:envoy-xds-core`
* Replaced dependabot with Renovate
* Build now requires JDK 21+ (Error Prone); published bytecode still targets Java 17
* Bump Gradle 7.5.1 -> 9.8.1 and all build plugins
* Bump guava 31.1 -> 33.7.2, grpc-stub 1.52.1 -> 1.85.0, slf4j 2.0.6 -> 2.0.20, spotbugs-annotations 4.7.3 -> 4.10.5
* Bump test dependencies: JUnit 6, Mockito 5, Testcontainers 2
* Removed unmaintained yamlvalidator plugin
* Removed unused jqwik test dependency
* Core depends on protobuf-java instead of the unused protobuf-java-util
* Removed grgit plugin and unused tagRelease task

## [1.0.1] - 2026-08-19
### Changed
* Bump com.google.protobuf:protobuf-java from 3.21.12 to 3.25.9
* Bump com.google.protobuf:protobuf-java-util from 3.21.12 to 3.25.9

## [1.0.0] - 2023-02-09
### Added
* Disabled dependabot for envoy-api package
### Changed
* Bump info.solidsoft.pitest from 1.9.0 to 1.9.11
* Bump io.grpc:grpc-stub from 1.51.0 to 1.52.1
* Bump com.google.protobuf:protobuf-java from 3.21.9 to 3.21.12
* Bump org.projectlombok:lombok from 1.18.24 to 1.18.26

## [0.9.0] - 2023-02-08
### Added
* First public version.
