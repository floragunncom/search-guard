/*
 * Copyright 2019-2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.signals.watch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * Code moved to separate class in order to easily write unit tests
 */
public class WatchInstanceIdService {

    private static final Logger log = LogManager.getLogger(WatchInstanceIdService.class);

    static final String INSTANCE_ID_SEPARATOR = "/instances/";

    private WatchInstanceIdService(){

    }

    /**
     * If the method return false the id does not belong to generic watch instance. Otherwise, id may or may not be id of generic watch
     * instance
     *
     * @param genericWatchInstanceId ID to test
     * @return false when ID is not a generic watch instance ID, otherwise the ID might belong to generic watch instance.
     */
    public static boolean isPossibleGenericWatchInstanceId(String genericWatchInstanceId) {
        Objects.requireNonNull(genericWatchInstanceId, "Generic watch instance ID is required");
        return genericWatchInstanceId.contains(INSTANCE_ID_SEPARATOR);
    }

    /**
     * Before usage of this operation that <code>genericWatchInstanceId</code> belongs to generic watch instance. This operation may produce
     * incorrect results or throw {@link IllegalArgumentException} when operation is used for single instance watch or
     * generic watch (template).
     * @param genericWatchInstanceId
     * @return generic watch id
     */
    public static String extractParentGenericWatchId(String genericWatchInstanceId) {
        Objects.requireNonNull(genericWatchInstanceId, "Generic watch instance ID is required");
        if(genericWatchInstanceId.contains(INSTANCE_ID_SEPARATOR)) {
            String result =  genericWatchInstanceId.substring(0, genericWatchInstanceId.indexOf(INSTANCE_ID_SEPARATOR));
            log.debug("Extracted watch or generic watch id '{}' from '{}'", result, genericWatchInstanceId);
            return result;
        }
        String message = "Generic watch instance ID '"
            + genericWatchInstanceId
            + "' does not contains required separator '"
            + INSTANCE_ID_SEPARATOR + "'.";
        throw new IllegalArgumentException(message);
    }

    /**
     * This operation is always able to create full generic watch instance id.
     * @param watchId watch id
     * @param instanceId instance id
     * @return full generic watch instance id
     */
    static String createInstanceId(String watchId, String instanceId) {
        Objects.requireNonNull(watchId, "Watch id is required");
        Objects.requireNonNull(instanceId, "Instance id is required");
        if(watchId.contains(INSTANCE_ID_SEPARATOR)) {
            throw new IllegalArgumentException("Generic watch ID" + watchId + " contains instance separator '" + INSTANCE_ID_SEPARATOR + "'.");
        }
        if(instanceId.contains(INSTANCE_ID_SEPARATOR)) {
            throw new IllegalArgumentException("Instance ID '" + instanceId + " contains instance separator '" + INSTANCE_ID_SEPARATOR + "'.");
        }
        return watchId + INSTANCE_ID_SEPARATOR + instanceId;
    }
}
