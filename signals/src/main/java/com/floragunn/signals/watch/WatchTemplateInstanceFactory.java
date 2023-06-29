package com.floragunn.signals.watch;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.jobs.config.JobTemplateInstanceFactory;
import com.floragunn.signals.actions.watch.template.service.WatchInstanceParameterLoader;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersData;
import com.floragunn.signals.watch.init.WatchInitializationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

public class WatchTemplateInstanceFactory implements JobTemplateInstanceFactory<Watch> {

    private static final Logger log = LogManager.getLogger(WatchTemplateInstanceFactory.class);

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
                log.debug("Watch template '{}' has no defined parameters, therefore instances will be not created.", watch.getId());
                return ImmutableList.empty();
            }
            log.debug("Watch '{}' has defined '{}' instances.", watch.getId(), parameters.size());
            return parameters.map(instanceParameters -> createInstanceForParameter(watch, instanceParameters));
        }
        log.debug("Watch '{}' is not a template, instance will not be created", watch.getId());
        return ImmutableList.of(watch);
    }

    private Watch createInstanceForParameter(Watch watch, WatchParametersData instanceParameters) {
        String templateDefinition = watch.getTemplateDefinition();
        String id = Watch.createInstanceId(watch.getId(), instanceParameters.getInstanceId());
        try {
            Watch watchInstance = Watch.parse(initializationService, watch.getTenant(), id, templateDefinition, -3);
            watchInstance.setInstancesParameters(instanceParameters);
            log.debug("Watch '{}' instance with id '{}' created", watch.getId(), instanceParameters.getInstanceId());
            return watchInstance;
        } catch (ConfigValidationException e) {
            String message = "Cannot parse watch '" + watch.getId() + "' template instance '" + instanceParameters.getInstanceId() + "'.";
            throw new RuntimeException(message, e);
        }
    }
}
