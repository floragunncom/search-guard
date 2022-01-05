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
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.signals.execution.WatchExecutionContextData;

public class WatchLog implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(WatchLog.class);

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
            return parse(id, DocNode.parse(Format.JSON).from(json));
        } catch (DocumentParseException e) {
            throw new ParseException(e.getMessage(), -1);
        }
    }

    public static WatchLog parse(String id, DocNode jsonNode) throws ParseException {
        WatchLog result = new WatchLog();

        result.id = id;

        if (jsonNode.hasNonNull("tenant")) {
            result.tenant = jsonNode.getAsString("tenant");
        }

        if (jsonNode.hasNonNull("watch_id")) {
            result.watchId = jsonNode.getAsString("watch_id");
        }
        
        if (jsonNode.hasNonNull("watch_version")) {
            try {
                result.watchVersion = jsonNode.getNumber("watch_version").longValue();
            } catch (ConfigValidationException e) {
                log.error("Error while parsing " + jsonNode, e);
            }
        }

        if (jsonNode.hasNonNull("status")) {
            result.status = Status.parse(jsonNode.getAsNode("status"));
        }

        if (jsonNode.hasNonNull("execution_start")) {
            // XXX 
            result.executionStart = Date.from(Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("execution_start"))));
        }

        if (jsonNode.hasNonNull("execution_end")) {
            // XXX 
            result.executionFinished = Date.from(Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("execution_end"))));
        }

        if (jsonNode.hasNonNull("data")) {
            result.data = jsonNode.getAsNode("data").toMap();
        }
        
        if (jsonNode.hasNonNull("runtime_attributes")) {
            result.runtimeAttributes = WatchExecutionContextData.create(jsonNode.getAsNode("runtime_attributes"));
        }

        if (jsonNode.hasNonNull("actions") && jsonNode.get("actions") instanceof Collection) {
            result.actions = ActionLog.parseList((Collection<?>) jsonNode.get("actions"));
        }

        if (jsonNode.hasNonNull("resolve_actions") && jsonNode.get("resolve_actions") instanceof Collection) {
            result.resolveActions = ActionLog.parseList((Collection<?>) jsonNode.get("resolve_actions"));
        }

        if (jsonNode.hasNonNull("node")) {
            result.node = jsonNode.getAsString("node");
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
