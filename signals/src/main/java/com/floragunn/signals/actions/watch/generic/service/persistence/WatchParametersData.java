package com.floragunn.signals.actions.watch.generic.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.indices.IndexMapping.DisabledIndexProperty;
import com.floragunn.searchsupport.indices.IndexMapping.DynamicIndexMapping;
import com.floragunn.searchsupport.indices.IndexMapping.KeywordProperty;

import java.util.Objects;

public class WatchParametersData implements Document<WatchParametersData> {
    public static final String FIELD_TENANT_ID = "tenant_id";
    public static final String FIELD_WATCH_ID = "watch_id";
    public static final String FIELD_INSTANCE_ID = "instance_id";
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

    private final long version;
    private final ImmutableMap<String, Object> parameters;

    public WatchParametersData(String tenantId, String watchId, String instanceId, ImmutableMap<String, Object> parameters) {
        this(tenantId, watchId, instanceId, parameters, -1);
    }

    private WatchParametersData(String tenantId, String watchId, String instanceId, ImmutableMap<String, Object> parameters, long version) {
        this.tenantId = Objects.requireNonNull(tenantId);
        this.watchId = watchId;
        this.instanceId = instanceId;
        this.parameters = parameters;
        this.version = version;
    }

    public WatchParametersData(DocNode node, long version) {
        this.tenantId = node.getAsString(FIELD_TENANT_ID);
        this.watchId = node.getAsString(FIELD_WATCH_ID);
        this.instanceId = node.getAsString(FIELD_INSTANCE_ID);
        this.parameters = node.getAsNode(FIELD_PARAMETERS).toMap();
        if(version < 0) {
            String currentId = getId();
            throw new IllegalArgumentException("Watch parameter " + currentId +  "version " + version + " must be positive value.");
        }
        this.version = version;
    }

    @Override
    public Object toBasicObject() {
        return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId, FIELD_INSTANCE_ID, instanceId,
            FIELD_PARAMETERS, parameters);
    }

    String getId() {
        return createId(tenantId, watchId, instanceId);
    }

    public static String createId(String tenantId, String watchId, String instanceId) {
        Objects.requireNonNull(tenantId, "Tenant id is required");
        Objects.requireNonNull(watchId, "Watch id is required");
        Objects.requireNonNull(instanceId, "Instance id is required.");
        return String.format("param@%s/%s/%s", tenantId, watchId, instanceId);
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

    @Override
    public String toString() {
        return "WatchParametersData{" + "tenantId='" + tenantId + '\'' + ", watchId='" + watchId + '\'' + ", instanceId='" + instanceId + '\'' + ", version=" + version + '}';
    }
}
