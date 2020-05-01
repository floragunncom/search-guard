package com.floragunn.signals.watch.result;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.floragunn.searchsupport.util.JacksonTools;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.watch.common.Ack;
import com.floragunn.signals.watch.result.WatchLog;

public class ActionLog implements ToXContentObject {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    private String name;

    private Status status;
    private ErrorInfo error;

    private Date executionStart;

    private Date executionEnd;
    private Map<String, Object> data;
    private WatchExecutionContextData runtimeAttributes;
    private String request;
    private Ack ack;
    private List<ActionLog> elements;

    public ActionLog() {
    }

    public ActionLog(String name) {
        this.name = name;
    }

    public Status getStatus() {
        return status;
    }

    public Status.Code getStatusCode() {
        if (status != null) {
            return status.getCode();
        } else {
            return null;
        }
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public long getExecutionDurationMs() {
        if (executionStart != null && executionEnd != null) {
            return executionEnd.getTime() - executionStart.getTime();
        } else {
            return -1;
        }
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getExecutionStart() {
        return executionStart;
    }

    public void setExecutionStart(Date executionStart) {
        this.executionStart = executionStart;
    }

    public Date getExecutionEnd() {
        return executionEnd;
    }

    public void setExecutionEnd(Date executionEnd) {
        this.executionEnd = executionEnd;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (name != null) {
            builder.field("name", name);
        }

        builder.field("status", status);

        if (ack != null) {
            builder.field("ack", ack);
        }

        if (error != null) {
            builder.field("error", error);
        }

        builder.field("execution_start", executionStart != null ? DATE_FORMATTER.format(executionStart.toInstant()) : null);
        builder.field("execution_end", executionEnd != null ? DATE_FORMATTER.format(executionEnd.toInstant()) : null);

        if (data != null && params.paramAsBoolean(WatchLog.ToXContentParams.INCLUDE_DATA.name(), false)) {
            builder.field("data", data);
        }

        if (runtimeAttributes != null && params.paramAsBoolean(WatchLog.ToXContentParams.INCLUDE_RUNTIME_ATTRIBUTES.name(), false)) {
            builder.field("runtime_attributes", runtimeAttributes);
        }

        if (request != null) {
            builder.field("request", request);
        }

        if (elements != null) {
            builder.field("elements", elements);
        }

        builder.endObject();
        return builder;
    }

    public static ActionLog parse(JsonNode jsonNode) {
        ActionLog result = new ActionLog(jsonNode.hasNonNull("name") ? jsonNode.get("name").asText() : null);

        if (jsonNode.hasNonNull("status")) {
            result.status = Status.parse(jsonNode.get("status"));
        }

        if (jsonNode.hasNonNull("ack")) {
            result.ack = Ack.create(jsonNode.get("ack"));
        }

        if (jsonNode.hasNonNull("execution_start")) {
            // XXX 
            result.executionStart = Date.from(Instant.from(DATE_FORMATTER.parse(jsonNode.get("execution_start").asText())));
        }

        if (jsonNode.hasNonNull("execution_end")) {
            // XXX 
            result.executionStart = Date.from(Instant.from(DATE_FORMATTER.parse(jsonNode.get("execution_end").asText())));
        }

        if (jsonNode.hasNonNull("request")) {
            result.request = jsonNode.get("request").asText();
        }

        if (jsonNode.hasNonNull("data")) {
            result.data = JacksonTools.toMap(jsonNode.get("data"));
        }

        if (jsonNode.hasNonNull("runtime_attributes")) {
            result.runtimeAttributes = WatchExecutionContextData.create(jsonNode.get("runtime_attributes"));
        }

        if (jsonNode.hasNonNull("elements") && jsonNode.get("elements").isArray()) {
            result.elements = parseList((ArrayNode) jsonNode.get("elements"));
        }

        return result;
    }

    public static List<ActionLog> parseList(ArrayNode arrayNode) {
        List<ActionLog> result = new ArrayList<>(arrayNode.size());

        for (JsonNode actionNode : arrayNode) {
            result.add(ActionLog.parse(actionNode));
        }

        return result;
    }

    @Override
    public String toString() {
        return "ActionLog [name=" + name + ", status=" + status + ", executionStart=" + executionStart + ", executionEnd=" + executionEnd + ", data="
                + data + "]";
    }

    public ErrorInfo getError() {
        return error;
    }

    public void setError(ErrorInfo error) {
        this.error = error;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public Ack getAck() {
        return ack;
    }

    public void setAck(Ack ack) {
        this.ack = ack;
    }

    public List<ActionLog> getElements() {
        return elements;
    }

    public void setElements(List<ActionLog> elements) {
        this.elements = elements;
    }

    public WatchExecutionContextData getRuntimeAttributes() {
        return runtimeAttributes;
    }

    public void setRuntimeAttributes(WatchExecutionContextData runtimeAttributes) {
        this.runtimeAttributes = runtimeAttributes;
    }

}
