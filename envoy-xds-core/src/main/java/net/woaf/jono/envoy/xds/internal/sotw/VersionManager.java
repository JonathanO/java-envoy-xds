package net.woaf.jono.envoy.xds.internal.sotw;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.primitives.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.Nullable;

import java.util.UUID;

class VersionManager {

    private static final Logger log = LoggerFactory.getLogger(VersionManager.class);

    public VersionManager() {
        this(UnsignedLong.ONE);
    }

    @VisibleForTesting
    VersionManager(UnsignedLong initialVersion) {
        this.currentVersion = initialVersion;
    }

    private @Nullable String currentNonce;

    private @Nullable UnsignedLong acceptedVersion;
    private @Nullable UnsignedLong sentVersion;

    private UnsignedLong currentVersion;

    public String getNext() {
        if (currentVersion.equals(UnsignedLong.MAX_VALUE)) {
            throw new IndexOutOfBoundsException("Overflowed version counter");
        }
        currentVersion = currentVersion.plus(UnsignedLong.ONE);
        return currentVersion.toString();
    }

    public boolean hasAcceptedVersion(String version) {
        if (acceptedVersion == null) {
            return false;
        }

        return acceptedVersion.compareTo(UnsignedLong.valueOf(version)) >= 0;
    }

    @VisibleForTesting
    @Nullable UnsignedLong getSentVersion() {
        return sentVersion;
    }

    public boolean processUpdate(String responseNonce, String versionInfo) {
        if (sentVersion != null) {
            if (!responseNonce.equals(currentNonce)) {
                log.debug("Client sent stale nonce {}, ignoring", responseNonce.isEmpty() ? "''" : responseNonce);
                return false;
            }

            if (Strings.isNullOrEmpty(versionInfo)) {
                log.error("Client rejected version {} and has no version to roll back to!", sentVersion);
            } else {
                // Compare as strings: the client may echo a version from another control plane, which needn't be a number.
                if (!sentVersion.toString().equals(versionInfo)) {
                    log.error("Client rejected version {} and rolled back to {}", sentVersion, versionInfo);
                } else {
                    log.debug("Client accepted version {}", sentVersion);
                    acceptedVersion = sentVersion;
                }
            }
        } else if (!Strings.isNullOrEmpty(responseNonce)) {
            log.debug("Client sent nonce {} when we expected null. Probably this is a reconnect. Continuing!", responseNonce);
        }
        return true;
    }

    public String pushedVersion(String newVersion) {
        String nonce = String.valueOf(UUID.randomUUID());
        currentNonce = nonce;
        sentVersion = UnsignedLong.valueOf(newVersion);
        return nonce;
    }

}
