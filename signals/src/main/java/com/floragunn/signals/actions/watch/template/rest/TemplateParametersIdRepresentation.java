package com.floragunn.signals.actions.watch.template.rest;


import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;

import java.util.Objects;

public class TemplateParametersIdRepresentation implements Document<TemplateParametersIdRepresentation> {
    public static final String FIELD_TENANT_ID = "tenant_id";
    public static final String FIELD_WATCH_ID = "watch_id";
    public static final String FIELD_INSTANCE_ID = "instance_id";
    private final String tenantId;
    private final String watchId;
    private final String instanceId;

    public TemplateParametersIdRepresentation(String tenantId, String watchId, String instanceId) {
        this.tenantId = Objects.requireNonNull(tenantId, "tenant id is required");
        this.watchId = Objects.requireNonNull(watchId, "Watch id is required");
        this.instanceId = Objects.requireNonNull(instanceId, "Instance id is required");
    }

    public TemplateParametersIdRepresentation(DocNode docNode) {
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
    public ImmutableMap toBasicObject() {
        return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId, FIELD_INSTANCE_ID, instanceId);
    }
}
