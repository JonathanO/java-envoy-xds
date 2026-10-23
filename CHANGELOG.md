# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
* Experimental `envoy-xds-snapshot` module: snapshot-style config with automatic make-before-break ordering
* `envoy-xds-testing` module: helpers for testing a control plane against a real Envoy, and an xDS library conformance suite
* Rate limited Envoy option for the conformance suite (`XdsConformanceTest(image, protocol, true)`)
* Make before break across a reconnect: `wasMakeBeforeBreak(fromMessage, before)`, `Checkpoint.assertSaneAfterReconnect(before)`, and the `changesWhileDisconnected` conformance scenario
* `Checkpoint` and `EnvoyChecks.assertRoutedVia` in `envoy-xds-testing`: one call to check everything since a mark, and which cluster served a request
* `XdsMatchers` in `envoy-xds-testing`: Hamcrest matchers for xDS resources, recorded messages, their order, and what Envoy holds
* `XdsEventListener.onClientNack`, `ClientHandle.resync()`, and `ResyncAfterNack` to resync an Envoy that NACKed on a backoff

### Changed
* **Breaking:** after Envoy NACKs, we stop sending it changes on that stream, for both SotW and delta. Envoy keeps its last good config until it reconnects. Previously delta dropped the stream (Envoy then reconnects in a tight loop), and SotW stalled without saying so while its backlog grew. `ClientNackException` is gone.
* When Envoy reconnects, it re-subscribes to everything at once. Route and listener answers now wait until the clusters and endpoints they may use are ACKed. Stale resources Envoy kept (delta) are removed in remove order, once the types that may still use them have been answered and ACKed, without needing `delayUpdatesUntilAckOf`. `XdsConfig.subscribesTo` says which types a client gets from us, so its removals don't wait for types it never asks for; they wait at most 30 seconds anyway. On SotW, a cluster Envoy kept is still removed straight away: the CDS answer is the whole set.
* **Breaking:** removed `XdsConfig.silentNacks`. Every NACK now freezes the stream, and is logged at warn; use `onClientNack` to act on it.

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
