/**
 * Snapshot-style config: publish whole snapshots to a {@link net.woaf.jono.envoy.xds.snapshot.SnapshotSource}, and
 * serve them with a {@link net.woaf.jono.envoy.xds.snapshot.SnapshotConfigBuilder} per type. Changes reach each Envoy
 * in make-before-break order.
 */
@NullMarked
@CheckReturnValue
package net.woaf.jono.envoy.xds.snapshot;

import com.google.errorprone.annotations.CheckReturnValue;
import org.jspecify.annotations.NullMarked;
