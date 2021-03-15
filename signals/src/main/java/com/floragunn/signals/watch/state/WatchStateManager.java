package com.floragunn.signals.watch.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class WatchStateManager {
    private final Map<String, WatchState> watchIdToStateMap = new ConcurrentHashMap<>();
    private final String tenant;
    private final String node;

    public WatchStateManager(String tenant, String node) {
        this.tenant = tenant;
        this.node = node;
    }

    public Map<String, WatchState> reset(Map<String, WatchState> watchIdToStateMap, Set<String> additionalWatchIds) {
        this.watchIdToStateMap.putAll(watchIdToStateMap);
        this.watchIdToStateMap.keySet().retainAll(watchIdToStateMap.keySet());

        return checkNodeChanges(watchIdToStateMap, additionalWatchIds);
    }

    public Map<String, WatchState> add(Map<String, WatchState> watchIdToStateMap, Set<String> additionalWatchIds) {
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

    public void delete(String watchId) {
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
