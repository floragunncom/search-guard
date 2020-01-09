package com.floragunn.signals.watch.state;

import java.util.Map;

public interface WatchStateWriter {
    void put(String watchId, WatchState watchState);

    void putAll(Map<String, WatchState> idToStateMap);
}
