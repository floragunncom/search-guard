/*
 * Copyright 2019-2023 floragunn GmbH
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
package com.floragunn.signals.actions.watch.generic.rest;


import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;

import java.util.Objects;

public class WatchInstanceIdRepresentation implements Document<WatchInstanceIdRepresentation> {
    public static final String FIELD_TENANT_ID = "tenant_id";
    public static final String FIELD_WATCH_ID = "watch_id";
    public static final String FIELD_INSTANCE_ID = "instance_id";
    private final String tenantId;
    private final String watchId;
    private final String instanceId;

    public WatchInstanceIdRepresentation(String tenantId, String watchId, String instanceId) {
        this.tenantId = Objects.requireNonNull(tenantId, "tenant id is required");
        this.watchId = Objects.requireNonNull(watchId, "Watch id is required");
        this.instanceId = Objects.requireNonNull(instanceId, "Instance id is required");
    }

    public WatchInstanceIdRepresentation(DocNode docNode) {
        this.tenantId = docNode.getAsString(FIELD_TENANT_ID);
        this.watchId = docNode.getAsString(FIELD_WATCH_ID);
        this.instanceId = docNode.getAsString(FIELD_INSTANCE_ID);
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getWatchId() {
        return watchId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public ImmutableMap<String, Object> toBasicObject() {
        return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId, FIELD_INSTANCE_ID, instanceId);
    }
}
