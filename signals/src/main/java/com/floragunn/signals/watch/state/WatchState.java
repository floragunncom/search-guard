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

package com.floragunn.signals.watch.state;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.floragunn.signals.watch.common.Ack;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.signals.NoSuchActionException;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.action.invokers.AlertAction;
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

    public boolean hasAction(String actionId) {
        Preconditions.checkArgument(actionId != null, "action id is missing");
        return this.actions.containsKey(actionId);
    }

    public boolean isActionMissing(String actionId) {
        return ! hasAction(actionId);
    }

    public Map<String, Ack> ack(String user, Watch watch) {
        Map<String, ActionState> allActionStates = new HashMap<>(actions);

        Map<String, Ack> ackedActions = new HashMap<>(allActionStates.size());

        for (Map.Entry<String, ActionState> entry : allActionStates.entrySet()) {
            try {
                AlertAction action = watch.getActionByName(entry.getKey());

                if (!action.isAckEnabled()) {
                    log.debug("Action is not marked as acknowledgeable: " + entry.getKey() + "; skipping.");
                    continue;
                }

                ActionState actionState = entry.getValue();
                if (actionState.ackIfPossible(user)) {
                    Ack acked = actionState.getAcked();
                    ackedActions.put(entry.getKey(), acked);
                }
            } catch (NoSuchActionException e) {
                log.error("Error in ack(): Cannot find action " + entry.getKey(), e);
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

    public static WatchState createFromJson(String tenant, String json) throws DocumentParseException {
        return createFrom(tenant, DocNode.parse(Format.JSON).from(json));
    }

    public static WatchState createFrom(String tenant, DocNode jsonNode) {
        WatchState result = new WatchState(tenant);

        if (jsonNode.hasNonNull("last_execution")) {
            try {
                result.lastExecutionContextData = WatchExecutionContextData.create(jsonNode.getAsNode("last_execution"));
            } catch (Exception e) {
                log.error("Error while parsing watch state from index " + jsonNode, e);
            }
        }

        if (jsonNode.hasNonNull("actions")) {
            for (Map.Entry<String, Object> entry : jsonNode.getAsNode("actions").toMap().entrySet()) {
                result.actions.put(entry.getKey(), ActionState.createFrom(DocNode.wrap(entry.getValue())));
            }
        }

        if (jsonNode.hasNonNull("last_status")) {
            result.lastStatus = Status.parse(jsonNode.getAsNode("last_status"));
        }

        if (jsonNode.hasNonNull("active")) {
            try {
                result.active = jsonNode.getBoolean("active");
            } catch (ConfigValidationException e) {
                log.error("Error while parsing watch state from index " + jsonNode, e);
            }
        }

        if (jsonNode.hasNonNull("node")) {
            result.node = jsonNode.getAsString("node");
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
