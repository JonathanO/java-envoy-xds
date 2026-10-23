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

You also need Docker, JUnit 5 or later, and an envoy-api with the usual `io.envoyproxy.envoy` packages. That is
whichever one your control plane already uses, e.g. `com.transferwise.envoy:envoy-api` or java-control-plane's
`io.envoyproxy.controlplane:api`.

What's in it:

- `EnvoyContainer`: an Envoy Testcontainer that keeps its log. It has an admin client (`admin()`, for `stats()` and
  `configDump()`) and `httpStatus(port, host)` to send a request through one of Envoy's listeners.
- `ConversationLogger`: a gRPC `ServerInterceptor` that records each xDS stream to and from your server.
- `SotwConversationAssert` / `DeltaConversationAssert`: AssertJ checks on a recorded stream.
- `EnvoyChecks`: checks on Envoy itself. It is connected, rejected nothing, has nothing warming, timed out no initial
  fetches, and logged no warnings or errors.
- `Settled`: conditions to wait on until your control plane and Envoy have stopped talking.
- `XdsConformanceTest`: the conformance suite.

## Testing your control plane

### Envoy's config

Write a bootstrap config for the Envoy under test, and put it on your test classpath. The suite's
[`envoy-sotw.yaml`](src/main/resources/net/woaf/jono/envoy/testing/conformance/envoy-sotw.yaml) and
[`envoy-delta.yaml`](src/main/resources/net/woaf/jono/envoy/testing/conformance/envoy-delta.yaml) make a good start.
It must:

- put the admin interface on `0.0.0.0:9901`, which `EnvoyContainer` uses;
- point ADS at `host.testcontainers.internal` on your gRPC server's port;
- bind any listener you send traffic through to `0.0.0.0`, or it can't be reached from the test;
- use the node id and cluster that your control plane expects.

To give Envoy an upstream to send traffic to, use its own admin port. Every `127.0.0.x:9901` address inside the
container reaches it, and `GET /` answers 200.

### A test

```java
class MyControlPlaneTest {

    private final EnvoyContainer envoy = new EnvoyContainer("envoyproxy/envoy:v1.40-latest")
        .withCopyFileToContainer(MountableFile.forClasspathResource("envoy.yaml"), "/envoy.yaml")
        .withCommand("-c", "/envoy.yaml")
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
        server = ServerBuilder.forPort(6566).addService(controlPlane.adsService()).intercept(conversations).build().start();
        Testcontainers.exposeHostPorts(6566);
        envoy.start();

        // Wait for the initial sync. Everything Envoy asked for must be answered well inside its 15s initial fetch timeout.
        await().atMost(30, SECONDS).until(() -> !conversations.getConversations().isEmpty());
        Conversation conversation = conversations.getConversations().get(0);
        await().atMost(10, SECONDS).untilAsserted(() -> SotwConversationAssert.assertThat(conversation)
            .hadConversationsWith(XdsTypes.CDS, XdsTypes.RDS)
            .answeredEveryType());
        await().atMost(30, SECONDS).until(Settled.settled(conversation));
        int messageMark = conversation.size();
        int logMark = envoy.logSize();

        // Make a change the way your users would, and wait for it to settle.
        controlPlane.register("foo", "127.0.0.1:9901");
        await().atMost(30, SECONDS).until(Settled.settledAfter(conversation, messageMark, Settled.settled(conversation)));

        SotwConversationAssert.assertThat(conversation)
            .nothingSentUnasked()
            .allWereAcked()
            .sentNoEmptyResponses(messageMark)
            .wasMakeBeforeBreak(messageMark);
        EnvoyChecks.assertHealthy(envoy);
        EnvoyChecks.assertNoWarningsSince(envoy, logMark);
        assertThat(envoy.httpStatus(10000, "foo")).isEqualTo(200);
    }
}
```

For delta xDS, use `DeltaConversationAssert` and set `api_type: DELTA_GRPC` in Envoy's config.

### What the checks mean

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

For anything else, look at what Envoy has: `envoy.admin().configDump()` gives typed clusters, endpoints, listeners and
routes, and `envoy.admin().stats()` the counters and gauges.

### Reconnects

To test a reconnect, shut down your gRPC server and start a new one on the same port with the same service and
interceptor. Envoy then opens a second conversation. On that one, skip `wasMakeBeforeBreak`, because the checks can't
know what config Envoy kept. Envoy before 1.19 resends resource names for wildcard types when it reconnects, so use
`nothingSentUnasked(true)` there.

### Things to know

- Each Envoy start costs a few seconds, and the first in a JVM much more. Add checks to the scenarios you have
  before adding new scenarios.
- Envoy connects to a fixed port, so don't run these tests in parallel.
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

Override `idleHook()` too if your library can tell when a stream is idle, and `resync()` if it can make Envoy reconnect. The scenarios:

- `startsEmpty`: no config at first. Envoy gets an answer for every type it asks for, including a route config that
  doesn't exist yet, then takes the first service.
- `addsAndRemovesServices`: a route, its cluster and its endpoints added in one change, then removed in one change.
- `movesRouteToNewCluster`: a route moved to a new cluster, and the old cluster removed, in one change.
- `containsRejectedConfig`: Envoy NACKs a bad cluster. The library mustn't send it again, not even as part of a later
  change, nor drop Envoy into a reconnect loop, and Envoy must keep serving its last good config. If you override
  `resync()` to make Envoy reconnect, a fixed config must get through after it.
- `reconnects`: a service is removed while Envoy is disconnected. After it reconnects, traffic still flows and updates
  still work.

After each change, the suite checks that the conversation stayed sane. Every response was ACKed, nothing was sent
unasked, there were no empty responses, and changes were make before break. It also checks that Envoy is healthy and
logged no warnings. To record a known deviation, override the test with `@Test @Disabled("why")`.

Envoy connects to port 6566 on the host, so run these one at a time. `EnvoyXdsConformanceTest` in
`envoy-xds-e2e-tests` runs the suite against envoy-xds.
