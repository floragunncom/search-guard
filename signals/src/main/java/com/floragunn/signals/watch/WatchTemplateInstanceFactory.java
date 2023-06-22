package com.floragunn.signals.watch;

import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.jobs.config.JobTemplateInstanceFactory;
import com.floragunn.signals.actions.watch.template.service.WatchInstanceParameterLoader;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersData;
import com.floragunn.signals.watch.init.WatchInitializationService;

import java.util.Objects;

public class WatchTemplateInstanceFactory implements JobTemplateInstanceFactory<Watch> {

    private final WatchInstanceParameterLoader parameterLoader;
    private final WatchInitializationService initializationService;

    public WatchTemplateInstanceFactory(WatchInstanceParameterLoader parameterLoader, WatchInitializationService initializationService) {
        this.parameterLoader = Objects.requireNonNull(parameterLoader, "Parameter loader is required");
        this.initializationService = Objects.requireNonNull(initializationService, "Watch initialization service is required");
    }

    @Override
    public ImmutableList<Watch> instantiateTemplate(Watch watch) {
        if(watch.hasParameters()) {
            ImmutableList<WatchParametersData> parameters = parameterLoader.findParameters(watch.getId());
            if(parameters.isEmpty()) {
                return ImmutableList.empty();
            }
            return parameters.map(instanceParameters -> createInstanceForParameter(watch, instanceParameters));
        }
        return ImmutableList.of(watch);
    }

    private Watch createInstanceForParameter(Watch watch, WatchParametersData instanceParameters) {
        String templateDefinition = watch.getTemplateDefinition();
        String id = Watch.createInstanceId(watch.getId(), instanceParameters.getInstanceId());
        try {
            Watch watchInstance = Watch.parse(initializationService, watch.getTenant(), id, templateDefinition, -3);
            watchInstance.setInstancesParameters(instanceParameters);
            return watchInstance;
        } catch (ConfigValidationException e) {
            throw new RuntimeException("Cannot parse watch template instance", e);
        }
    }
}
