# Agents

## The Go port

[go-envoy-xds](https://github.com/JonathanO/go-envoy-xds) ports this library to Go, and should behave the same. Its tests
are ported from these, as near as makes sense, so a test that passes in one and fails in the other shows where they
differ. When you change behaviour or a test here, make the same change there, or say why it doesn't apply.

## Checks

- Run `./gradlew check javadoc` before pushing: `check` doesn't run javadoc, and CI fails on its warnings.
- `./gradlew pitest` must kill every mutant in envoy-xds-core and envoy-xds-snapshot, each module with its own tests.
  An exception needs a strong reason, agreed with the maintainer.
- The e2e tests run real Envoys. Never mock Envoy. Their harness and conformance suite are the spec's `conformance/`
  build, which `settings.gradle.kts` includes from `spec/`. A change to the harness or a scenario is a PR there first,
  then a submodule bump here; the Go port runs the same suite against its conformance server.
- envoy-xds-spec-tests runs the trace vectors from the spec submodule, `spec/`: `git submodule update --init`. Every
  build needs it, as `settings.gradle.kts` includes `spec/conformance`. A
  behaviour change starts there, with a vector.

## Releasing

The maintainer releases by pushing a `v<version>` tag matching `version` in gradle.properties, e.g. `v2.0.0`. The
Release workflow checks the tag matches, runs `check`, and publishes envoy-xds-core and envoy-xds-snapshot to Maven
Central. Never push tags or create releases yourself. Before a release: set the version, and move the CHANGELOG's
unreleased entries under it.
