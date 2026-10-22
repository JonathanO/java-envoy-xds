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
