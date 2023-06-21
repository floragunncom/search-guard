package com.floragunn.signals.actions.watch.template.service;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.jobs.config.InstanceParameterLoader;
import com.floragunn.searchsupport.jobs.config.InstanceParameters;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersRepository;

import java.util.List;
import java.util.Objects;

public class WatchInstanceParameterLoader implements InstanceParameterLoader {


    private final String tenantId;

    private final WatchParametersRepository repository;

    public WatchInstanceParameterLoader(String tenantId, WatchParametersRepository repository) {
        this.tenantId = Objects.requireNonNull(tenantId, "Tenant id is required");
        this.repository = Objects.requireNonNull(repository, "Watch parameter repository is required");
    }

    @Override
    public ImmutableList<InstanceParameters> findParameters(String parametersKey) {
        return repository.findByWatchId(tenantId, parametersKey) //
            .map(watchParametersData -> new InstanceParameters(watchParametersData.getInstanceId(), watchParametersData.getParameters()));
    }
}
