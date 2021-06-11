package com.floragunn.signals.watch.state;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.result.Status;
import com.floragunn.signals.watch.severity.SeverityLevel;

public class WatchState implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(WatchState.class);

    private Map<String, ActionState> actions = new ConcurrentHashMap<>();
    private volatile WatchExecutionContextData lastExecutionContextData;
    private final String tenant;
    private volatile Status lastStatus;
    private boolean active = true;
    private String node;
    private boolean refreshBeforeExecuting;
    private transient final Instant creationTime = Instant.now();

    public WatchState(String tenant) {
        this.tenant = tenant;
    }
    
    public WatchState(String tenant, String node) {
        this.tenant = tenant;
        this.node = node;
    }

    public ActionState getActionState(String actionId) {

        if (actionId == null) {
            throw new IllegalArgumentException("watchId is null");
        }

        return this.actions.computeIfAbsent(actionId, (key) -> new ActionState());
    }

    public List<String> ack(String user) {
        Map<String, ActionState> allActionStates = new HashMap<>(actions);

        List<String> ackedActions = new ArrayList<>(allActionStates.size());

        for (Map.Entry<String, ActionState> entry : allActionStates.entrySet()) {
            if (entry.getValue().ackIfPossible(user)) {
                ackedActions.add(entry.getKey());
            }
        }

        return ackedActions;
    }

    public List<String> unack(String user) {
        Map<String, ActionState> allActionStates = new HashMap<>(actions);

        List<String> unackedActions = new ArrayList<>(allActionStates.size());

        for (Map.Entry<String, ActionState> entry : allActionStates.entrySet()) {
            if (entry.getValue().unackIfPossible(user)) {
                unackedActions.add(entry.getKey());
            }
        }

        return unackedActions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("_tenant", tenant);

        builder.startObject("actions");

        for (Map.Entry<String, ActionState> entry : this.actions.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }

        builder.endObject();

        builder.field("last_execution", lastExecutionContextData);

        builder.field("last_status", lastStatus);

        builder.field("node", node);

        //   builder.boolean("active", active);

        builder.endObject();
        return builder;
    }

    public String toJsonString() {
        return Strings.toString(this);
    }

    public static WatchState createFromJson(String tenant, String json) throws IOException {
        return createFrom(tenant, DefaultObjectMapper.readTree(json));
    }

    public static WatchState createFrom(String tenant, JsonNode jsonNode) {
        WatchState result = new WatchState(tenant);

        if (jsonNode.hasNonNull("last_execution")) {
            try {
                result.lastExecutionContextData = WatchExecutionContextData.create(jsonNode.get("last_execution"));
            } catch (Exception e) {
                log.error("Error while parsing watch state from index " + jsonNode, e);
            }
        }

        if (jsonNode.hasNonNull("actions")) {
            Iterator<Entry<String, JsonNode>> actionIter = jsonNode.get("actions").fields();

            while (actionIter.hasNext()) {
                Entry<String, JsonNode> entry = actionIter.next();

                result.actions.put(entry.getKey(), ActionState.createFrom(entry.getValue()));
            }
        }

        if (jsonNode.hasNonNull("last_status")) {
            result.lastStatus = Status.parse(jsonNode.get("last_status"));
        }

        if (jsonNode.hasNonNull("active")) {
            result.active = jsonNode.get("active").asBoolean();
        }

        if (jsonNode.hasNonNull("node")) {
            result.node = jsonNode.get("node").textValue();
        }

        return result;
    }

    public static Map<String, Object> getIndexMapping() {
        NestedValueMap result = new NestedValueMap();

        result.put("dynamic", true);

        result.put(new NestedValueMap.Path("properties", "_tenant", "type"), "text");
        result.put(new NestedValueMap.Path("properties", "_tenant", "analyzer"), "keyword");
        result.put(new NestedValueMap.Path("properties", "last_execution", "type"), "object");
        result.put(new NestedValueMap.Path("properties", "last_execution", "dynamic"), true);
        result.put(new NestedValueMap.Path("properties", "last_execution", "properties", "data", "type"), "object");
        result.put(new NestedValueMap.Path("properties", "last_execution", "properties", "data", "dynamic"), true);
        result.put(new NestedValueMap.Path("properties", "last_execution", "properties", "data", "enabled"), false);

        return result;
    }

    public SeverityLevel getLastSeverityLevel() {
        if (lastExecutionContextData != null && lastExecutionContextData.getSeverity() != null) {
            return lastExecutionContextData.getSeverity().getLevel();
        } else {
            return null;
        }
    }

    public WatchExecutionContextData getLastExecutionContextData() {
        return lastExecutionContextData;
    }

    public void setLastExecutionContextData(WatchExecutionContextData lastExecutionContextData) {
        this.lastExecutionContextData = lastExecutionContextData;
    }

    public String getTenant() {
        return tenant;
    }

    public Status getLastStatus() {
        return lastStatus;
    }

    public void setLastStatus(Status lastStatus) {
        this.lastStatus = lastStatus;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public boolean isRefreshBeforeExecuting() {
        return refreshBeforeExecuting;
    }

    public void setRefreshBeforeExecuting(boolean refreshBeforeExecuting) {
        this.refreshBeforeExecuting = refreshBeforeExecuting;
    }

    public String toString() {
        return Strings.toString(this);
    }

    public Instant getCreationTime() {
        return creationTime;
    }
}
