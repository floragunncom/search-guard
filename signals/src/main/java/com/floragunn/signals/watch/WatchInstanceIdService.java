package com.floragunn.signals.watch;

import java.util.Objects;

/**
 * Code moved to separate class in order to easily write unit tests
 */
class WatchInstanceIdService {

    static final String INSTANCE_ID_SEPARATOR = "+";

    static final WatchInstanceIdService INSTANCE = new WatchInstanceIdService();

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

    public String createInstanceId(String watchId, String instanceId) {
        Objects.requireNonNull(watchId, "Watch id is required");
        Objects.requireNonNull(instanceId, "Instance id is required");
        return watchId + INSTANCE_ID_SEPARATOR + instanceId;
    }
}
