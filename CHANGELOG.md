# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
* Experimental `envoy-xds-snapshot` module: snapshot-style config with automatic make-before-break ordering. A `SnapshotSource` holds named groups of resources and named views, each an ordered list of groups, and picks each client's view from its details; changing a group only wakes the clients whose view has it
* `envoy-xds-testing`: helpers for testing a control plane against a real Envoy, and an xDS library conformance suite. It now lives in [envoy-xds-spec](https://github.com/JonathanO/envoy-xds-spec)'s `conformance/`, so every port runs the one harness, and isn't published from here: include that build, as `envoy-xds-e2e-tests` does
* Make before break across a reconnect: `wasMakeBeforeBreak(fromMessage, before)`, `Checkpoint.assertSaneAfterReconnect(before)`, and the `changesWhileDisconnected` conformance scenario
* `Checkpoint` and `EnvoyChecks.assertRoutedVia` in `envoy-xds-testing`: one call to check everything since a mark, and which cluster served a request
* `XdsMatchers` in `envoy-xds-testing`: Hamcrest matchers for xDS resources, recorded messages, their order, and what Envoy holds
* Every log a stream makes carries its Envoy's node ID in the SLF4J MDC, under `node`: add `%X{node}` to your log pattern
* `XdsEventListener.onClientNack`, `ClientHandle.resync()`, and `ResyncAfterNack` to resync an Envoy that NACKed on a backoff
* JSpecify nullness annotations: every package is `@NullMarked`, so anything that may be null says `@Nullable`. NullAway checks this strictly at build time, and Error Prone and javac warnings now fail the build
* Error Prone checks locking and immutability at build time: fields shared between threads are `@GuardedBy` their lock, and `Snapshot` and `NamedMessage` are `@Immutable`
* Results of our methods must be used, unless marked `@CanIgnoreReturnValue` (`SnapshotSource.update` and `publish` are)
* Conformance scenarios for scoped routes (SRDS) and on-demand discovery: `scopedRoutes`, `onDemandScopes`, `onDemandClusters` (ODCDS) and `onDemandVirtualHosts` (VHDS). The make-before-break checks now follow scoped route configs to their route configs, and VHDS virtual hosts to their clusters
* `changesEndpoints` conformance scenario: only a service's endpoints change, the commonest change in a mesh, and Envoy must route to the new ones without rebuilding the cluster. `EnvoyAdminClient.clusters()` gives Envoy's per-host values, such as each endpoint's `rq_total`
* `EnvoyContainer.withBootstrap(resource, adsPort)`: runs Envoy with a bootstrap config from the classpath, with `${ADS_PORT}` replaced by your control plane's port. With a server on a free port, tests can run in parallel. The conformance suite and e2e tests now do this instead of using port 6566
* `XdsConformanceTest.envoyImages()`: override it to run the suite on other Envoy versions than `ENVOY_IMAGES`
* `ClusterEventSource.subscribe(listener, clientDetails)`: the core now tells your source which client is subscribing, so it can choose what that client sees. It defaults to `subscribe(listener)`, so existing sources need no change
* Phases in the snapshot module: `SnapshotSource.Change.then()` splits a change, so each client gets each phase, and ACKs it, before the next. It's for changes whose order make before break can't see, such as a route that must stop using a cluster's subset before the cluster drops it. `Mergeable.mergesWithLater()` lets any update type keep `MergingStateBacklog` from merging later updates into it

### Changed
* Build scripts are Kotlin DSL, with the shared setup in convention plugins under `build-logic` (`envoy-xds.java-conventions`, `envoy-xds.published`, `envoy-xds.mutation-tested`) and every dependency version in `gradle/libs.versions.toml`. Only `envoy-xds-core` and `envoy-xds-snapshot` are published; the example and e2e tests no longer are. Publishing goes to Maven Central through the Central Portal's staging API, as OSSRH was retired. Builds resolve from Maven Central only, no longer from Maven Local
* Built and tested against `net.woaf.jono.envoy:envoy-api` 1.36.0 (from [envoy-api-java](https://github.com/JonathanO/envoy-api-java), published for each Envoy release) instead of `com.transferwise.envoy:envoy-api` 1.17.0. It's still `compileOnly`, so bring the envoy-api you already use
* `delayUpdatesUntilAckOf` no longer blocks updates forever when the client never ACKs that type: it's ignored if `subscribesTo` leaves the type out, and otherwise gives up after 30 seconds with a warning
* e2e tests run on Envoy 1.36 to 1.40, the releases upstream supports, instead of also 1.17 to 1.24
* e2e tests are the conformance suite, on every one of those releases, plus one test that changes arriving during a push are merged. The example-driven e2e tests, whose cases the conformance scenarios cover, are gone
* e2e tests run four at a time, each with its own Envoy. `-PtestParallelism=N` changes how many, and 1 runs them one at a time
* **Breaking (unreleased API):** `nothingSentUnaskedPre119Reconnect()` and `nothingSentUnasked(boolean)` are removed from `EnvoyAssert`, `SotwConversationAssert` and `DeltaConversationAssert` in `envoy-xds-testing`: use `nothingSentUnasked()`. The core drops its workaround for Envoy before 1.19, which sent resource names on its first request for a wildcard type after reconnecting: such a request now subscribes to just those names, as the spec says. `NodeConfig`, which only added that flag to `XdsConfig`, is gone
* **Breaking (unreleased API):** `XdsConformanceTest` runs its own matrix: every scenario on both protocols (or `protocols()`), on Envoy 1.36 and 1.40, with and without rate limiting. Libraries implement `newControlPlane()` once, instead of a subclass per variant, and record deviations with `knownDeviations()`.
* **Breaking:** after Envoy NACKs, we stop sending it changes on that stream, for both SotW and delta. Envoy keeps its last good config until it reconnects. Previously delta dropped the stream (Envoy then reconnects in a tight loop), and SotW stalled without saying so while its backlog grew. `ClientNackException` is gone.
* When Envoy reconnects, it re-subscribes to everything at once. Route and listener answers now wait until the clusters and endpoints they may use are ACKed. Stale resources Envoy kept (delta) are removed in remove order, once the types that may still use them have been answered and ACKed, without needing `delayUpdatesUntilAckOf`. `XdsConfig.subscribesTo` says which types a client gets from us, so its removals don't wait for types it never asks for; otherwise they wait at least 30 seconds after it connects, then until its next request or your next change. On SotW, a cluster Envoy kept is still removed straight away: the CDS answer is the whole set.
* `TypedEnvoyConfigDump` sections Envoy left out of the dump are empty instead of null
* Dependencies: core and snapshot expose Guava, protobuf-java, grpc-stub (core) and error_prone_annotations as `api`, since their types are in our API. spotbugs-annotations and javax.annotation-api are no longer dependencies. `./gradlew buildHealth` reports unused and undeclared dependencies
* **Breaking:** no more Lombok. `XdsConfig`, `CommonDiscoveryRequest`, `NamedMessage`, `Response`, `Resources` and `TypedEnvoyConfigDump` are records, so their accessors drop the `get`/`is` prefix (`name()`, `addAndUpdates()`, `clientDetails()`). Their builders are gone: use `XdsConfig.of(details)`, with `withDelayUpdatesUntilAckOf` and `withSubscribesTo`; `new Response<>(addAndUpdates, removes)` or `Response.empty()`; `new Resources<>(resources)` or `Resources.empty()`; `TypedEnvoyConfigDump.of(dump)`. The unused `Response.merge()`, `Resources.merge()` and `Resources.getByName()` are removed. `XdsConfig.delayUpdatesUntilAckOf` and `subscribesTo` are now marked `@Nullable`, as documented
* **Breaking:** clients no longer get a thread each. `SnapshotSource` delivers updates on a shared pool with one thread per core, or on your own `Executor` (`new SnapshotSource<>(viewOf, executor)`). `QueueingMergingEventListener`, `QueueingEventListener` and `ThreadedQueuingEventListener` are replaced by `SerialEventListener`, which delivers each client's updates in order on an executor and doesn't merge: updates that arrive while a client is busy are merged by its `StateBacklog`, so use `MergingStateBacklog` to keep merging
* A client whose update fails, e.g. because a config builder threw, is logged once, at error with its node id, and its stream is closed. The exception no longer propagates to your `ClusterEventSource`, so `SerialEventListener` doesn't log it a second time
* **Breaking:** the protocol machinery moved to `net.woaf.jono.envoy.xds.internal` (and `.internal.delta`, `.internal.sotw`), which is not API and may change in any release: `DiscoveryServiceManager`, `CommonDiscoveryStreamObserver`, `CommonDiscoveryRequest`, the discovery services and their factories. Use `AggregatedDiscoveryService`; its constructor taking the per-protocol services is gone
* Every public class and method in core, snapshot and testing has Javadoc, and the Javadoc build fails on warnings
* A stream that fails ends with a gRPC status that says whose fault it was, instead of `UNKNOWN` with our exception: `INVALID_ARGUMENT` for a bad request, such as a delta request that subscribes to and unsubscribes from the same name, and `INTERNAL` with the message "xds: internal error" for our own failures, such as a config builder throwing, even if it throws a gRPC status. A status thrown by your `ClientConfigProvider` still reaches Envoy as it is. The exception is logged, but no longer sent to Envoy
* SotW: each response lists its resources sorted by name, instead of in the order they were added, so the same set always makes the same response
* An update equal to the stream's current state is no longer skipped: it goes through your config builders like any other, and whatever they return is sent. Leaving out updates that change nothing is the source's job, which `SnapshotSource` already does, so the core doesn't compare every update with the last
* A NACK unsubscribes the stream from your `ClusterEventSource` at once, rather than when the stream ends: a frozen stream sends no more changes, so updates for it would only be dropped. If you wrote your own source, `unsubscribe` is now called for that listener at the NACK, and not again when the stream closes
* Every public class and method in core and snapshot has Javadoc, and the Javadoc build fails on warnings
* **Breaking:** removed `XdsConfig.silentNacks`. Every NACK now freezes the stream, and is logged at warn; use `onClientNack` to act on it.
* Delta: an ACK that also changes subscriptions is no longer logged at info. Envoy does this by design, so it was noise
* SotW: a request that only unsubscribes, such as Envoy dropping a removed cluster's endpoints, is no longer answered with the whole remaining set. Envoy doesn't wait for an answer, so each one only cost a response, and held up changes until Envoy ACKed it
* `XdsConformanceTest` runs Envoy at its default log level, info, instead of debug, and no longer relays every line of Envoy's log to SLF4J: debug was about nine times the log, and nothing checks it. Warnings and errors still fail a test, and a failed test prints Envoy's whole log to stderr, which the test report keeps

### Fixed
* A change, or a removal of what a reconnecting Envoy kept, no longer starts while listener or route requests are held for the clusters. It skipped the types that hadn't heard their first request yet, so a delta Envoy reconnecting while a change waited could lose a cluster its kept listeners still routed to. It now starts once the last held request is answered
* A subscription answered while a change is going out no longer gets ahead of the change. It's answered from the change's add round until its type's remove round has been sent, which then goes in turn: SotW used to send the type's removals in the answer, so a reconnected Envoy asking for clusters mid-change could lose one that a listener, not yet removed, still routed to. Route and listener subscriptions also wait while the change's clusters and endpoints are still to be sent, not only while they await an ACK. After a NACK, subscriptions are answered without the rejected change's removals, which never went out
* Conformance: a scenario waits for Envoy's listeners to warm before its first request. With Envoy's xDS requests rate limited, a listener's last subscription could still be queued in Envoy when the suite thought it had synced, so the first request was reset
* SotW `nothingSentUnasked` checks secrets (SDS) and runtime layers (RTDS) too, instead of failing because it didn't know how to read their names
* `Settled` matches Envoy's answers to responses by type as well as nonce, so a library that numbers nonces per type doesn't look settled while one type is still unanswered
* Delta: a resource sent to answer a subscription while a change waits its turn, such as the endpoints of a cluster the change adds, isn't sent again when the change gets to its type. That saved message is an ACK round trip less per added cluster or listener
* VHDS is a wildcard type: Envoy's VHDS subscription names nothing on the wire (it watches its route config's name as a namespace), and only becomes ready once it gets virtual hosts under that name, so its first request now gets every virtual host instead of nothing
* Scoped route configs (SRDS) are removed before the route configs they name, not after, so a scope never points at a route config Envoy no longer has
* `ResyncAfterNack` forgets nodes that have gone 2 * maxDelay without a NACK, instead of keeping a backoff for every node id it ever saw
* A request that arrives after we closed the stream to resync Envoy is ignored, instead of throwing and logging a misleading warning
* SotW: after Envoy NACKs, a later subscription change no longer sends the rest of the rejected change. For the type Envoy rejected, the answer repeats what it rejected, as SotW answers with the whole set: Envoy rejects it again and keeps what it has
* SRDS is a wildcard type, as Envoy subscribes to it with no names. Previously that first request went unanswered, so Envoy waited out its initial fetch timeout without scoped routes
* Delta: names subscribed explicitly while subscribed to `*` are answered, with `removed_resources` for names that don't exist
* SotW: an explicit `*` subscription, alone or with names, no longer closes the stream, and names sent alongside `*` stay subscribed after leaving wildcard
* SotW: a NACK whose `version_info` isn't numeric no longer fails the stream
* A request for an xDS type we don't serve (e.g. ECDS) is logged and left unanswered, instead of closing the stream
* Normal client disconnects (`CANCELLED`) are no longer logged as warnings
* Each resource is packed into an `Any` once, instead of once per client
* SotW: a request that lists a name twice no longer leaves names it dropped subscribed
* `Snapshot.Editor.put` and `remove` can be called for their effect alone: with Error Prone, filling an editor in a loop no longer fails `CheckReturnValue`
* `Snapshot.types()` no longer lists a type whose last resource was removed: `Snapshot.Editor.build()` drops a type left empty, as `types()` documents

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
