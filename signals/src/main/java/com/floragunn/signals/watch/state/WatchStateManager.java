/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.watch.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WatchStateManager {
    private static final Logger log = LogManager.getLogger(WatchStateManager.class);

    private final Map<String, WatchState> watchIdToStateMap = new ConcurrentHashMap<>();
    private final String tenant;
    private final String node;

    public WatchStateManager(String tenant, String node) {
        this.tenant = tenant;
        this.node = node;
    }

    public Map<String, WatchState> reset(Map<String, WatchState> watchIdToStateMap, Set<String> additionalWatchIds) {

        if (log.isDebugEnabled()) {
            log.debug("WatchStateManager.reset(" + watchIdToStateMap.keySet() + ", " + additionalWatchIds + ")");
        }

        this.watchIdToStateMap.putAll(watchIdToStateMap);
        this.watchIdToStateMap.keySet().retainAll(watchIdToStateMap.keySet());

        return checkNodeChanges(watchIdToStateMap, additionalWatchIds);
    }

    public Map<String, WatchState> add(Map<String, WatchState> watchIdToStateMap, Set<String> additionalWatchIds) {
        if (log.isDebugEnabled()) {
            log.debug("WatchStateManager.add(" + watchIdToStateMap.keySet() + ", " + additionalWatchIds + ")");
        }

        this.watchIdToStateMap.putAll(watchIdToStateMap);

        return checkNodeChanges(watchIdToStateMap, additionalWatchIds);
    }

    public WatchState getWatchState(String watchId) {

        if (watchId == null) {
            throw new IllegalArgumentException("watchId is null");
        }

        WatchState watchState = this.watchIdToStateMap.computeIfAbsent(watchId, (String key) -> new WatchState(tenant, node));

        return watchState;
    }

    public WatchState peekWatchState(String watchId) {
        if (watchId == null) {
            throw new IllegalArgumentException("watchId is null");
        }

        return this.watchIdToStateMap.get(watchId);
    }

    public void delete(String watchId) {
        if (log.isDebugEnabled()) {
            log.debug("WatchStateManager.delete(" + watchId + ")");
        }

        watchIdToStateMap.remove(watchId);
    }

    private Map<String, WatchState> checkNodeChanges(Map<String, WatchState> watchIdToStateMap, Set<String> additionalWatchIds) {
        HashMap<String, WatchState> dirtyStates = new HashMap<>();

        for (Map.Entry<String, WatchState> entry : watchIdToStateMap.entrySet()) {
            String id = entry.getKey();
            WatchState state = entry.getValue();

            if (state.getNode() == null) {
                state.setNode(node);
                dirtyStates.put(id, state);
            } else if (!node.equals(state.getNode())) {
                state.setNode(node);
                state.setRefreshBeforeExecuting(true);
                dirtyStates.put(id, state);
            }
        }

        for (String additionalWatchId : additionalWatchIds) {
            if (watchIdToStateMap.containsKey(additionalWatchId)) {
                continue;
            }

            WatchState state = this.watchIdToStateMap.computeIfAbsent(additionalWatchId, (String key) -> new WatchState(tenant));

            if (state.getNode() == null) {
                state.setNode(node);
                dirtyStates.put(additionalWatchId, state);
            } else if (!node.equals(state.getNode())) {
                state.setNode(node);
                state.setRefreshBeforeExecuting(true);
                dirtyStates.put(additionalWatchId, state);
            }
        }

        return dirtyStates;
    }
}
