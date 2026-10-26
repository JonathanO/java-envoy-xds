/**
 * Snapshot-style config: publish groups of resources, and views made of them, to a
 * {@link net.woaf.jono.envoy.xds.snapshot.SnapshotSource}, and serve each client its view with a
 * {@link net.woaf.jono.envoy.xds.snapshot.SnapshotConfigBuilder} per type. Changes reach each Envoy in
 * make-before-break order.
 */
@NullMarked
@CheckReturnValue
package net.woaf.jono.envoy.xds.snapshot;

import com.google.errorprone.annotations.CheckReturnValue;
import org.jspecify.annotations.NullMarked;
