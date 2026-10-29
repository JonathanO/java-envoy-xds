# Agents

## The Go port

[go-envoy-xds](https://github.com/JonathanO/go-envoy-xds) ports this library to Go, and should behave the same. Its tests
are ported from these, as near as makes sense, so a test that passes in one and fails in the other shows where they
differ. When you change behaviour or a test here, make the same change there, or say why it doesn't apply.

## Checks

- Run `./gradlew check javadoc` before pushing: `check` doesn't run javadoc, and CI fails on its warnings.
- `./gradlew pitest` must kill every mutant in envoy-xds-core and envoy-xds-snapshot, each module with its own tests.
  An exception needs a strong reason, agreed with the maintainer.
- The e2e tests run real Envoys. Never mock Envoy.
