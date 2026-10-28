# Envoy xDS testing

Test a control plane against a real Envoy, in Docker via Testcontainers. Nothing here depends on envoy-xds-core, so
it works for any Java control plane. There are two ways to use it:

- [Testing your control plane](#testing-your-control-plane): write your own scenarios ("service X loses an
  instance") and check that Envoy ends up where it should, safely.
- [Conformance suite for xDS libraries](#conformance-suite-for-xds-libraries): check that a library serves any given
  config correctly.

## Setup

```groovy
testImplementation 'net.woaf.jono.envoy:envoy-xds-testing:<version>'
```

You also need Docker (or another [container runtime](#container-runtimes)), JUnit 5 or later, and an envoy-api with
the usual `io.envoyproxy.envoy` packages. That is whichever one your control plane already uses, e.g.
`net.woaf.jono.envoy:envoy-api` or java-control-plane's `io.envoyproxy.controlplane:api`.

What's in it:

- `EnvoyContainer`: an Envoy Testcontainer that keeps its log. It has an admin client (`admin()`, for `stats()`, per-host
  `clusters()` and `configDump()`) and `httpStatus(port, host)` to send a request through one of Envoy's listeners.
- `ConversationLogger`: a gRPC `ServerInterceptor` that records each xDS stream to and from your server.
- `SotwConversationAssert` / `DeltaConversationAssert`: AssertJ checks on a recorded stream.
- `EnvoyChecks`: checks on Envoy itself. It is connected, rejected nothing, has nothing warming, timed out no initial
  fetches, and logged no warnings or errors.
- `Settled`: conditions to wait on until your control plane and Envoy have stopped talking.
- `XdsMatchers`: Hamcrest matchers for resources, messages, the order things happened in, and what Envoy holds.
- `XdsConformanceTest`: the conformance suite.

### Container runtimes

Anything [Testcontainers supports](https://java.testcontainers.org/supported_docker_environment/) should do:

- Docker Engine on Linux, or Docker Desktop on macOS or Windows: works as is.
- [Rancher Desktop](https://java.testcontainers.org/supported_docker_environment/#rancher-desktop): set its engine to
  dockerd (moby), not containerd ([as Testcontainers for Go says](https://golang.testcontainers.org/system_requirements/rancher/)),
  and on macOS set the environment variables on that page.
- [Podman](https://java.testcontainers.org/supported_docker_environment/#podman): set `DOCKER_HOST` to its
  Docker-compatible socket, e.g. `unix://${XDG_RUNTIME_DIR}/podman/podman.sock` on Linux. Rootless, also set
  `TESTCONTAINERS_RYUK_DISABLED=true`.

Envoy reaches your control plane at `host.testcontainers.internal`, through
[Testcontainers' SSH tunnel to the host](https://java.testcontainers.org/features/networking/#exposing-host-ports-to-the-container),
not Docker's own route to the host, which differs by platform. So it needs nothing more of the runtime than the rest of
Testcontainers does, whether the runtime runs in a VM or not.

## Testing your control plane

### Envoy's config

Write a bootstrap config for the Envoy under test, and put it on your test classpath. The suite's
[`envoy-sotw.yaml`](src/main/resources/net/woaf/jono/envoy/testing/conformance/envoy-sotw.yaml) and
[`envoy-delta.yaml`](src/main/resources/net/woaf/jono/envoy/testing/conformance/envoy-delta.yaml) make a good start.
It must:

- put the admin interface on `0.0.0.0:9901`, which `EnvoyContainer` uses;
- point ADS at `host.testcontainers.internal`, on port `${ADS_PORT}`, which `withBootstrap` fills in with your gRPC
  server's;
- bind any listener you send traffic through to `0.0.0.0`, or it can't be reached from the test;
- use the node id and cluster that your control plane expects.

To give Envoy an upstream to send traffic to, use its own admin port. Every `127.0.0.x:9901` address inside the
container reaches it, and `GET /` answers 200.

### A test

```java
class MyControlPlaneTest {

    private final EnvoyContainer envoy = new EnvoyContainer("envoyproxy/envoy:v1.40-latest")
        .withExposedPorts(EnvoyContainer.ADMIN_PORT, 10000)
        .withAccessToHost(true);
    private final ConversationLogger conversations = new ConversationLogger();
    private Server server;

    @AfterEach
    void stop() {
        envoy.stop();
        server.shutdownNow();
    }

    @Test
    void addsAService() throws Exception {
        MyControlPlane controlPlane = new MyControlPlane();

        // Your server first, then Envoy. The other way round, Envoy backs off and can time out its first fetches.
        // On a free port, so tests can run in parallel.
        server = ServerBuilder.forPort(0).addService(controlPlane.adsService()).intercept(conversations).build().start();
        Testcontainers.exposeHostPorts(server.getPort());
        envoy.withBootstrap("envoy.yaml", server.getPort()).start();

        // Wait for the initial sync. Everything Envoy asked for must be answered well inside its 15s initial fetch timeout.
        await().atMost(30, SECONDS).until(() -> !conversations.getConversations().isEmpty());
        Conversation conversation = conversations.getConversations().get(0);
        await().atMost(10, SECONDS).untilAsserted(() -> SotwConversationAssert.assertThat(conversation)
            .hadConversationsWith(XdsTypes.CDS, XdsTypes.RDS)
            .answeredEveryType());
        await().atMost(30, SECONDS).until(Settled.settled(conversation));
        Checkpoint checkpoint = Checkpoint.take(envoy, conversation);

        // Make a change the way your users would, and wait for it to settle.
        controlPlane.register("foo", "127.0.0.1:9901");
        await().atMost(30, SECONDS).until(Settled.settledAfter(conversation, checkpoint.message(), Settled.settled(conversation)));

        checkpoint.assertSane(); // the conversation, and Envoy's health and log, since the checkpoint
        EnvoyChecks.assertRoutedVia(envoy, 10000, "foo", "foo");
    }
}
```

For delta xDS, use `DeltaConversationAssert` and set `api_type: DELTA_GRPC` in Envoy's config. `Checkpoint` and
`EnvoyAssert.of(conversation)` work out which protocol a conversation uses.

### What the checks mean

- `Checkpoint.take(envoy, conversation)`: marks the conversation, Envoy's log and its stats, so later checks only look
  at what happened since. Take one once Envoy has synced, and again after anything you did on purpose that Envoy warns
  about, like a disconnect or a NACK.
  - `assertSane()`: all of the conversation checks below from the checkpoint on, then `assertEnvoyHealthy()`.
  - `assertSaneAfterReconnect()`: the same without make before break, for a conversation Envoy started by
    reconnecting.
  - `assertSaneAfterReconnect(before)`: the same, plus make before break from the start of this conversation, starting
    from what Envoy last ACKed on `before`, the conversation before it.
  - `assertEnvoyHealthy()`: `EnvoyChecks.assertHealthy` and `assertNoWarningsSince`, both since the checkpoint.

- `nothingSentUnasked()`: every response is for a type, and resource names, that Envoy subscribed to.
- `allWereAcked()`: Envoy ACKed every response, and NACKed none.
- `answeredEveryType()`: every type Envoy asked for got a response. Without one, Envoy waits out its initial fetch
  timeout, or forever if that is disabled. A request for a name that doesn't exist needs a response too.
- `sentNoEmptyResponses(mark)`: no empty response after the mark. An empty first response for a wildcard type is fine,
  and is why this takes a mark.
- `wasMakeBeforeBreak(mark)`: no route was sent before Envoy ACKed the clusters and endpoints it uses, and no cluster
  was removed while an ACKed route still used it. Start after the initial sync: Envoy orders its own startup.
- `unsubscribedFrom(type, name)`: Envoy dropped a subscription, e.g. EDS for a cluster you removed. Wait for this
  before checking a removal has settled. Envoy unsubscribes after it ACKs the cluster removal.
- `EnvoyChecks.assertHealthy(envoy)`: Envoy is connected, rejected nothing, has nothing warming and timed out no
  initial fetches. After a deliberate NACK, pass a `stats()` baseline taken after it.
- `EnvoyChecks.assertNoWarningsSince(envoy, mark)`: no warnings or errors in Envoy's log since the mark. Take the mark
  after startup, and again after anything you did on purpose that Envoy warns about, like a disconnect.
- `EnvoyChecks.assertRoutedVia(envoy, listenerPort, virtualHost, cluster)`: a request to the virtual host answers 200
  and went to that cluster, going by its `upstream_rq_total`. A 200 alone doesn't say which cluster answered.

For anything else, look at what Envoy has: `envoy.admin().configDump()` gives typed clusters, endpoints, listeners and
routes, and `envoy.admin().stats()` the counters and gauges.

### Matchers

`XdsMatchers` are Hamcrest matchers for your own expectations: resources, recorded messages, the order things
happened in, and what Envoy holds. They work the same for SotW and delta. Use them with Hamcrest's `assertThat`,
AssertJ's `HamcrestCondition`, Awaitility's `until(supplier, matcher)` or Mockito's `argThat`.

```java
// Envoy ACKed routes that no longer use bar before bar's cluster was removed.
assertThat(conversation, hasInOrder(
    ackOf(response(ofType(XdsTypes.RDS), containing(routeConfig(not(routingTo("bar")))))),
    response(ofType(XdsTypes.CDS), removing("bar"))));

// Wait for Envoy to reject something.
await().until(() -> conversation, hasMessage(nack(ofType(XdsTypes.CDS), withMessage(containsString("connect_timeout")))));

// What Envoy holds.
assertThat(envoy.admin().configDump(), activeClusterNames(containsInAnyOrder("foo", "baz")));
assertThat(envoy.admin().configDump(), hasRouteConfig(routing("foo", "foo-v2")));
assertThat(envoy.admin().stats(), hasStat("cluster.foo-v2.upstream_rq_total", greaterThan(0L)));
```

- Resources: `cluster(...)`, `routeConfig(...)`, `endpointAssignment(...)`, with `named`, `ofType`, `routingTo`,
  `routing(virtualHost, cluster)` and `withEndpoint(address, port)`.
- Messages: `response(...)`, `request(...)`, `nack(...)`, with `ofType`, `containing`, `removing`, `subscribingTo`,
  `unsubscribingFrom` (delta only) and `withMessage`. For SotW, `removing` matches a wildcard (LDS, CDS, SRDS, VHDS) response
  without the resource.
- Conversations: `hasMessage`, and `hasInOrder` with `ackOf`, Envoy's ACK of a matching response.
- Envoy: `hasActiveCluster`, `activeClusterNames` and `hasRouteConfig` on a config dump, and `hasStat` on `stats()`.

### Reconnects

To test a reconnect, shut down your gRPC server and start a new one on the same port (`server.getPort()`) with the same service and
interceptor. Envoy then opens a second conversation. On that one, skip `wasMakeBeforeBreak`, because the checks can't
know what config Envoy kept.

### Things to know

- Each Envoy start costs a few seconds, and the first in a JVM much more. Add checks to the scenarios you have
  before adding new scenarios.
- Envoy connects to whichever port you give `withBootstrap`, so tests on free ports can run in parallel.
- Mark lines from `logSize()` and conversation sizes from `size()`. Then checks only look at what happened since.

## Waiting for things to settle

`Settled` returns `Callable<Boolean>` conditions for Awaitility. If your control plane can tell when a stream is idle,
give `Settled` that as a hook. Otherwise it judges from the recorded conversation alone: every response answered, and
nothing sent either way for a quiet period (500ms by default).

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

Extend it once, and give it a fresh control plane for each test: your ADS service, and a way to set the whole desired
config.

```java
class MyLibraryConformanceTest extends XdsConformanceTest {

    @Override
    protected ControlPlane newControlPlane() {
        MyCache cache = new MyCache();
        return new ControlPlane() {
            @Override
            public BindableService service() {
                return new MyAdsService(cache);
            }

            @Override
            public void setConfig(Collection<? extends Message> resources) {
                cache.replaceAll(resources); // Clusters, ClusterLoadAssignments and RouteConfigurations
            }
        };
    }
}
```

The suite owns the matrix, so every library is tested the same way. It runs every scenario:
- on both protocols, unless you override `protocols()` to support one. The on-demand cluster and virtual host
  scenarios run on delta only, as Envoy only does those over delta;
- on each of `ENVOY_IMAGES`, the oldest Envoy release upstream still supports (1.36) and the newest (1.40). Override
  `envoyImages()` to run on others, e.g. every supported release;
- with Envoy getting listeners from the control plane (LDS) as well as its static listener, so serve LDS too, and
  scoped route configs (SRDS) and virtual hosts (VHDS) for the scenarios that use them;
- normally, and with Envoy's requests to the control plane rate limited (`rate_limit_settings`). Envoy then queues
  them, and delta Envoy sends ACKs ahead of queued subscriptions, so after a reconnect requests arrive in a different
  order. The reconnect scenarios check the limit was actually hit.

That's 104 tests and takes several minutes, so run it in CI or before a release rather than on every build. Each test is
named by its path, e.g. `sotw/v1.36/rate-limited/startsEmpty`.

Implement `idleHook()` in your `ControlPlane` if your library can tell when a stream is idle, and `resync()` if it can
make Envoy reconnect. The scenarios:

- `startsEmpty`: no config at first. Envoy gets an answer for every type it asks for, including a route config that
  doesn't exist yet, then takes the first service.
- `addsAndRemovesServices`: a route, its cluster and its endpoints added in one change, then removed in one change.
  It also adds a cluster with no endpoints, which still needs an empty assignment, or Envoy leaves it warming.
- `changesEndpoints`: only a service's endpoints change, the commonest change in a mesh: one is replaced and one
  added, then one removed. Envoy must route to the new endpoints, without rebuilding the cluster.
- `movesRouteToNewCluster`: a route moved to a new cluster, and the old cluster removed, in one change.
- `addsAndRemovesListener`: a listener, its route config, cluster and endpoints added in one change, then removed in
  one change. The listener mustn't be left warming, and a route config mustn't go while a listener Envoy has uses it.
- `churns`: twenty changes in quick succession, each moving a route to a new cluster. Envoy must end on the last, with
  no stale clusters, make before break throughout.
- `containsRejectedConfig`: Envoy NACKs a bad cluster. The library mustn't send it again, not even as part of a later
  change, nor drop Envoy into a reconnect loop, and Envoy must keep serving its last good config. If you override
  `resync()` to make Envoy reconnect, a fixed config must get through after it.
- `changesWhileDisconnected`: a route moves to a new cluster, and the old cluster is removed, while Envoy is
  disconnected. Envoy keeps its config across the disconnect, so after it reconnects the move must be make before break
  from there.
- `controlPlaneRestarts`: Envoy reconnects to a fresh control plane with different config. The new control plane never
  sent what Envoy kept, so it can only go by what Envoy tells it.
- `reconnects`: a service is removed while Envoy is disconnected. After it reconnects, traffic still flows and updates
  still work.
- `scopedRoutes`: a listener picks a scope (SRDS) by the Host header, and each scope names its own route config. A
  scope, its route config and cluster are added in one change, then removed in one change: the scope must go before its
  route config, and the route config before its cluster.
- `onDemandScopes`: as `scopedRoutes`, but on demand. Envoy asks for a scope's route config when the first request for
  it arrives, and that first request must succeed.
- `onDemandClusters` (ODCDS): Envoy subscribes to no clusters as it starts, and asks for one, then its endpoints, when
  the first request routed to it arrives. That first request must succeed.
- `onDemandVirtualHosts` (VHDS): Envoy's route config has no virtual hosts of its own. Envoy holds it until it gets
  virtual hosts named `<route config>/...`, so a library sends some to start, and others when Envoy asks for one on the
  first request for its host. Either way, requests for each host get routed. It's skipped on Envoy 1.36, which only
  takes VHDS from a config source of its own, not over ADS.

After each change, the suite checks that the conversation stayed sane. Every response was ACKed, nothing was sent
unasked, there were no empty responses, and changes were make before break. It also checks that Envoy is healthy and
logged no warnings. To record a known deviation, override `knownDeviations()`. Keys are parts of a test's path, so
`Map.of("sotw/changesWhileDisconnected", "why")` skips that scenario on SotW, on every version and mode.

Each test's control plane listens on a free port, so tests can run in parallel, each with its own Envoy. Turn on
JUnit's parallel execution, e.g. in `src/test/resources/junit-platform.properties`:

```properties
junit.jupiter.execution.parallel.enabled=true
junit.jupiter.execution.parallel.mode.default=concurrent
junit.jupiter.execution.parallel.config.strategy=fixed
junit.jupiter.execution.parallel.config.fixed.parallelism=4
```

`newControlPlane()` is then called concurrently, so the control planes it returns mustn't share state.
`EnvoyXdsConformanceTest` in `envoy-xds-e2e-tests` runs the suite against envoy-xds, on every Envoy release upstream
supports, this way.
