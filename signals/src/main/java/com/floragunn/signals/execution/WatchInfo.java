package com.floragunn.signals.execution;

import java.util.Map;

import com.floragunn.signals.support.NestedValueMap;

public class WatchInfo {
    private final String id;

    private final Map<String, Object> metadata;

    public WatchInfo(String id, Map<String, Object> metadata) {
        this.id = id;
        this.metadata = NestedValueMap.createUnmodifieableMap(metadata);
    }
    
    public String getId() {
        return id;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }
}
