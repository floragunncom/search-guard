package com.floragunn.signals.watch.state;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WatchStateManager {
    private final Map<String, WatchState> watchIdToStateMap = new ConcurrentHashMap<>();
    private final String tenant;
    private final String node;

    public WatchStateManager(String tenant, String node) {
        this.tenant = tenant;
        this.node = node;
    }

    public Map<String, WatchState> reset(Map<String, WatchState> watchIdToStateMap) {
        this.watchIdToStateMap.putAll(watchIdToStateMap);
        this.watchIdToStateMap.keySet().retainAll(this.watchIdToStateMap.keySet());

        return checkNodeChanges(watchIdToStateMap);
    }

    public WatchState getWatchState(String watchId) {

        if (watchId == null) {
            throw new IllegalArgumentException("watchId is null");
        }

        WatchState watchState = this.watchIdToStateMap.computeIfAbsent(watchId, (String key) -> new WatchState(tenant));

        return watchState;
    }

    public void delete(String watchId) {
        watchIdToStateMap.remove(watchId);
    }

    private Map<String, WatchState> checkNodeChanges(Map<String, WatchState> watchIdToStateMap) {
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

        return dirtyStates;
    }
}
