package com.floragunn.signals.watch;

import java.util.Objects;
import java.util.Optional;

/**
 * Code moved to separate class in order to easily write unit tests
 */
public class WatchInstanceIdService {

    static final String INSTANCE_ID_SEPARATOR = "+";

    private WatchInstanceIdService(){

    }

    /**
     * Extract watch id from watch instance id. If non-generic watch id is used, then watch id is returned.
     * Watch instance id is composed of generic watch id and parameters id.
     */
    public static String extractGenericWatchOrWatchId(String watchOrInstanceId) {
        Objects.requireNonNull(watchOrInstanceId, "Watch or instance id is required");
        if(watchOrInstanceId.contains(INSTANCE_ID_SEPARATOR)) {
            return watchOrInstanceId.substring(0, watchOrInstanceId.indexOf(INSTANCE_ID_SEPARATOR));
        }
        return watchOrInstanceId;
    }

    public static Optional<String> extractInstanceId(String watchAndInstanceId) {
        Objects.requireNonNull(watchAndInstanceId, "Watch and instance id is required");
        return Optional.of(watchAndInstanceId) //
            .filter(id -> id.contains(INSTANCE_ID_SEPARATOR)) //
            .map(compoundId -> compoundId.substring(compoundId.indexOf(INSTANCE_ID_SEPARATOR) + 1))
            .filter(instanceId -> instanceId.length() > 0);
    }

    public static String createInstanceId(String watchId, String instanceId) {
        Objects.requireNonNull(watchId, "Watch id is required");
        Objects.requireNonNull(instanceId, "Instance id is required");
        return watchId + INSTANCE_ID_SEPARATOR + instanceId;
    }
}
