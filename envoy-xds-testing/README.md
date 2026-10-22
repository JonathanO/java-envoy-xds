# Envoy xDS testing

Helpers for testing any Java xDS control plane against a real Envoy. Nothing here depends on envoy-xds-core; you
provide your own `envoy-api` (or compatible) protos.

- `EnvoyContainer`: an Envoy Testcontainer that keeps its log, with an admin client and a helper to send HTTP through
  a listener. Envoy's config must put the admin interface on `0.0.0.0:9901`.
- `ConversationLogger`: a gRPC `ServerInterceptor` that records each xDS stream to and from your server.
- `DeltaConversationAssert` / `SotwConversationAssert`: AssertJ checks on a recorded stream. They check that every
  response was ACKed, nothing was sent that Envoy didn't ask for, no empty responses went out, every type got an
  answer, and changes were make before break.
- `EnvoyChecks`: checks on Envoy itself. It is connected, rejected nothing, has nothing warming, timed out no initial
  fetches, and logged no warnings or errors.
- `Settled`: conditions to wait on until the control plane and Envoy have stopped talking.

## Waiting for things to settle

If your control plane can tell when a stream is idle, give `Settled` that as a hook. Otherwise it judges from the
recorded conversation alone: every response answered, and nothing sent either way for a quiet period (500ms by
default).

```java
// With a hook from your control plane:
await().until(Settled.settled(conversation, myStream::isIdle));
// Without one:
await().until(Settled.settled(conversation));

// Waiting for a change you just made: it may not have started sending yet.
int mark = conversation.size();
makeAChange();
await().until(Settled.settledAfter(conversation, mark, Settled.settled(conversation)));
```

The envoy-xds e2e tests (`envoy-xds-e2e-tests`) use all of this, with envoy-xds's per-stream metrics as the hook.

## Conformance suite for xDS libraries

`XdsConformanceTest` checks the layer that serves config to Envoy: given desired config, does a real Envoy get there
safely? It doesn't test how a control plane decides what to serve. For that, write your own scenarios with the helpers
above.

Extend it once per Envoy version and protocol. Give it your ADS service and a way to set the whole desired config:

```java
class MyLibrarySotwTest extends XdsConformanceTest {
    private final MyCache cache = new MyCache(); // fields are fresh for each test

    MyLibrarySotwTest() {
        super("envoyproxy/envoy:v1.40-latest", Protocol.SOTW);
    }

    @Override
    protected BindableService controlPlane() {
        return new MyAdsService(cache);
    }

    @Override
    protected void setConfig(Collection<? extends Message> resources) {
        cache.replaceAll(resources); // Clusters, ClusterLoadAssignments and RouteConfigurations
    }
}
```

Override `idleHook()` too if your library can tell when a stream is idle. The scenarios:

- `startsEmpty`: no config at first. Envoy gets an answer for every type it asks for, including a route config that
  doesn't exist yet, then takes the first service.
- `addsAndRemovesServices`: a route, its cluster and its endpoints added in one change, then removed in one change.
- `movesRouteToNewCluster`: a route moved to a new cluster, and the old cluster removed, in one change.
- `recoversFromRejectedConfig`: Envoy NACKs a bad cluster, and the next good config still gets through on the same
  stream.
- `reconnects`: a service is removed while Envoy is disconnected. After it reconnects, traffic still flows and updates
  still work.

After each change, the suite checks that the conversation stayed sane. Every response was ACKed, nothing was sent
unasked, there were no empty responses, and changes were make before break. It also checks that Envoy is healthy and
logged no warnings. To record a known deviation, override the test with `@Test @Disabled("why")`.

Envoy connects to port 6566 on the host, so run these one at a time. `EnvoyXdsConformanceTest` in
`envoy-xds-e2e-tests` runs the suite against envoy-xds.
