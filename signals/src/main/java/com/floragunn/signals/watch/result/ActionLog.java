/*
 * Copyright 2020-2021 floragunn GmbH
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

package com.floragunn.signals.watch.result;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.watch.common.Ack;

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

    public static ActionLog parse(DocNode jsonNode) {
        ActionLog result = new ActionLog(jsonNode.hasNonNull("name") ? jsonNode.getAsString("name") : null);

        if (jsonNode.hasNonNull("status")) {
            result.status = Status.parse(jsonNode.getAsNode("status"));
        }

        if (jsonNode.hasNonNull("ack")) {
            result.ack = Ack.create(jsonNode.getAsNode("ack"));
        }

        if (jsonNode.hasNonNull("execution_start")) {
            // XXX 
            result.executionStart = Date.from(Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("execution_start"))));
        }

        if (jsonNode.hasNonNull("execution_end")) {
            // XXX 
            result.executionStart = Date.from(Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("execution_end"))));
        }

        if (jsonNode.hasNonNull("request")) {
            result.request = jsonNode.getAsString("request");
        }

        if (jsonNode.hasNonNull("data")) {
            result.data = jsonNode.getAsNode("data").toMap();
        }

        if (jsonNode.hasNonNull("runtime_attributes")) {
            result.runtimeAttributes = WatchExecutionContextData.create(jsonNode.getAsNode("runtime_attributes"));
        }

        if (jsonNode.hasNonNull("elements") && jsonNode.get("elements") instanceof Collection) {
            result.elements = parseList((Collection<?>) jsonNode.get("elements"));
        }

        return result;
    }

    public static List<ActionLog> parseList(Collection<?> arrayNode) {
        List<ActionLog> result = new ArrayList<>(arrayNode.size());

        for (Object actionNode : arrayNode) {
            result.add(ActionLog.parse(DocNode.wrap(actionNode)));
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
