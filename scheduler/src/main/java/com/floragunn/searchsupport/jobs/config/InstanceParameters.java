package com.floragunn.searchsupport.jobs.config;

import com.floragunn.fluent.collections.ImmutableMap;

import java.util.Objects;

public class InstanceParameters {

    private final String instanceId;
    private final ImmutableMap<String, Object> parameters;

    public InstanceParameters(String instanceId, ImmutableMap<String, Object> parameters) {
        this.instanceId = Objects.requireNonNull(instanceId, "Instance id is required");
        this.parameters = Objects.requireNonNull(parameters, "Template instance parameters are required");
    }

    public String getInstanceId() {
        return instanceId;
    }

    public ImmutableMap<String, Object> getParameters() {
        return parameters;
    }
}
