package com.floragunn.signals.actions.watch.generic.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.indices.IndexMapping.DisabledIndexProperty;
import com.floragunn.searchsupport.indices.IndexMapping.DynamicIndexMapping;
import com.floragunn.searchsupport.indices.IndexMapping.KeywordProperty;

import java.util.Objects;

public class WatchInstanceData implements Document<WatchInstanceData> {
    public static final String FIELD_TENANT_ID = "tenant_id";
    public static final String FIELD_WATCH_ID = "watch_id";
    public static final String FIELD_INSTANCE_ID = "instance_id";
    public static final String FIELD_ENABLED = "enabled";
    public static final String FIELD_PARAMETERS = "parameters";

    //TODO: consider usage multiple field mappings so that parameters are searchable via full text search using
    // - tenant id
    // - watch id
    // - instance id
    public static final DynamicIndexMapping MAPPINGS = new DynamicIndexMapping(new KeywordProperty(FIELD_TENANT_ID),
        new KeywordProperty(FIELD_WATCH_ID), new KeywordProperty(FIELD_INSTANCE_ID), new DisabledIndexProperty(FIELD_PARAMETERS));
    private final String tenantId;
    private final String watchId;
    private final String instanceId;

    private final boolean enabled;

    private final long version;
    private final ImmutableMap<String, Object> parameters;

    public WatchInstanceData(String tenantId, String watchId, String instanceId, boolean enabled, ImmutableMap<String, Object> parameters) {
        this(tenantId, watchId, instanceId, enabled, parameters, -1);
    }

    private WatchInstanceData(String tenantId, String watchId, String instanceId, boolean enabled, ImmutableMap<String, Object> parameters, long version) {
        this.tenantId = Objects.requireNonNull(tenantId);
        this.watchId = watchId;
        this.instanceId = instanceId;
        this.enabled = enabled;
        this.parameters = parameters;
        this.version = version;
    }

    public WatchInstanceData(DocNode node, long version) {
        this.tenantId = node.getAsString(FIELD_TENANT_ID);
        this.watchId = node.getAsString(FIELD_WATCH_ID);
        this.instanceId = node.getAsString(FIELD_INSTANCE_ID);
        this.enabled = isEnabled(node);
        this.parameters = node.getAsNode(FIELD_PARAMETERS).toMap();
        if(version < 0) {
            String currentId = getId();
            throw new IllegalArgumentException("Watch parameter " + currentId +  "version " + version + " must be positive value.");
        }
        this.version = version;
    }

    private static Boolean isEnabled(DocNode node) {
        try {
            return node.getBoolean(FIELD_ENABLED);
        } catch (ConfigValidationException e) {
            throw new RuntimeException("Cannot check if generic watch instance is enabled", e);
        }
    }

    @Override
    public Object toBasicObject() {
        return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId, FIELD_INSTANCE_ID, instanceId,
            FIELD_ENABLED, enabled, FIELD_PARAMETERS, parameters);
    }

    String getId() {
        return createId(tenantId, watchId, instanceId);
    }

    public static String createId(String tenantId, String watchId, String instanceId) {
        Objects.requireNonNull(tenantId, "Tenant id is required");
        Objects.requireNonNull(watchId, "Watch id is required");
        Objects.requireNonNull(instanceId, "Instance id is required.");
        return String.format("ins@%s/%s/%s", tenantId, watchId, instanceId);
    }

    public ImmutableMap<String, Object> getParameters() {
        return parameters;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public long getVersion() {
        return version;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String toString() {
        return "WatchInstanceData{" + "tenantId='" + tenantId + '\'' + ", watchId='" + watchId + '\'' + ", instanceId='" + instanceId + '\''
            + ", version=" + version + '}';
    }
}
