package com.floragunn.signals.watch.result;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchsupport.json.JacksonTools;
import com.floragunn.signals.execution.WatchExecutionContextData;

public class WatchLog implements ToXContentObject {
    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    public enum ToXContentParams {
        INCLUDE_DATA,
        INCLUDE_RUNTIME_ATTRIBUTES
    }
    
    private String id;

    private String tenant;
    private String watchId;
    private long watchVersion = -1;
    private Status status;

    private Date executionStart;
    private Date executionFinished;
    private ErrorInfo error;
    private String node;

    private Map<String, Object> data;
    private WatchExecutionContextData runtimeAttributes;
    private List<ActionLog> actions;
    private List<ActionLog> resolveActions;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getWatchId() {
        return watchId;
    }

    public void setWatchId(String watchId) {
        this.watchId = watchId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Date getExecutionStart() {
        return executionStart;
    }

    public void setExecutionStart(Date executionStart) {
        this.executionStart = executionStart;
    }

    public Date getExecutionFinished() {
        return executionFinished;
    }

    public void setExecutionFinished(Date executionFinished) {
        this.executionFinished = executionFinished;
    }

    public List<ActionLog> getActions() {
        return actions;
    }

    public void setActions(List<ActionLog> actions) {
        this.actions = actions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("tenant", tenant);
        builder.field("watch_id", watchId);
        
        if (watchVersion > 0) {
            builder.field("watch_version", watchVersion);
        }
        
        builder.field("status", status);

        if (error != null) {
            builder.field("error", error);
        }

        builder.field("execution_start", executionStart != null ? DATE_FORMATTER.format(executionStart.toInstant()) : null);
        builder.field("execution_end", executionFinished != null ? DATE_FORMATTER.format(executionFinished.toInstant()) : null);

        if (params.paramAsBoolean(ToXContentParams.INCLUDE_DATA.name(), false)) {
            builder.field("data", data);
        }

        if (params.paramAsBoolean(ToXContentParams.INCLUDE_RUNTIME_ATTRIBUTES.name(), false)) {
            builder.field("runtime_attributes", runtimeAttributes);
        }
        
        builder.startArray("actions");  
        for (ActionLog actionLog : actions) {
            actionLog.toXContent(builder, params);
        }
        builder.endArray();

        if (resolveActions != null && resolveActions.size() > 0) {
            builder.startArray("resolve_actions");        
            for (ActionLog actionLog : resolveActions) {
                actionLog.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (node != null) {
            builder.field("node", node);
        }

        builder.endObject();

        return builder;
    }

    public static WatchLog parse(String id, String json) throws ParseException {
        try {
            return parse(id, DefaultObjectMapper.readTree(json));
        } catch (IOException e) {
            throw new ParseException(e.getMessage(), -1);
        }
    }

    public static WatchLog parse(String id, JsonNode jsonNode) throws ParseException {
        WatchLog result = new WatchLog();

        result.id = id;

        if (jsonNode.hasNonNull("tenant")) {
            result.tenant = jsonNode.get("tenant").asText();
        }

        if (jsonNode.hasNonNull("watch_id")) {
            result.watchId = jsonNode.get("watch_id").asText();
        }
        
        if (jsonNode.hasNonNull("watch_version")) {
            result.watchVersion = jsonNode.get("watch_version").asLong();
        }

        if (jsonNode.hasNonNull("status")) {
            result.status = Status.parse(jsonNode.get("status"));
        }

        if (jsonNode.hasNonNull("execution_start")) {
            // XXX 
            result.executionStart = Date.from(Instant.from(DATE_FORMATTER.parse(jsonNode.get("execution_start").asText())));
        }

        if (jsonNode.hasNonNull("execution_end")) {
            // XXX 
            result.executionFinished = Date.from(Instant.from(DATE_FORMATTER.parse(jsonNode.get("execution_end").asText())));
        }

        if (jsonNode.hasNonNull("data")) {
            result.data = JacksonTools.toMap(jsonNode.get("data"));
        }
        
        if (jsonNode.hasNonNull("runtime_attributes")) {
            result.runtimeAttributes = WatchExecutionContextData.create(jsonNode.get("runtime_attributes"));
        }

        if (jsonNode.hasNonNull("actions") && jsonNode.get("actions").isArray()) {
            result.actions = ActionLog.parseList((ArrayNode) jsonNode.get("actions"));
        }

        if (jsonNode.hasNonNull("resolve_actions") && jsonNode.get("resolve_actions").isArray()) {
            result.resolveActions = ActionLog.parseList((ArrayNode) jsonNode.get("resolve_actions"));
        }

        if (jsonNode.hasNonNull("node")) {
            result.node = jsonNode.get("node").textValue();
        }

        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public ErrorInfo getError() {
        return error;
    }

    public void setError(ErrorInfo error) {
        this.error = error;
    }

    public List<ActionLog> getResolveActions() {
        return resolveActions;
    }

    public void setResolveActions(List<ActionLog> resolveActions) {
        this.resolveActions = resolveActions;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public WatchExecutionContextData getRuntimeAttributes() {
        return runtimeAttributes;
    }

    public void setRuntimeAttributes(WatchExecutionContextData runtimeAttributes) {
        this.runtimeAttributes = runtimeAttributes;
    }

    public long getWatchVersion() {
        return watchVersion;
    }

    public void setWatchVersion(long watchVersion) {
        this.watchVersion = watchVersion;
    }


}
