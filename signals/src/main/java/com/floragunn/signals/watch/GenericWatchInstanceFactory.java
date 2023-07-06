package com.floragunn.signals.watch;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.jobs.config.GenericJobInstanceFactory;
import com.floragunn.signals.actions.watch.generic.service.WatchInstanceParameterLoader;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchParametersData;
import com.floragunn.signals.watch.init.WatchInitializationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GenericWatchInstanceFactory implements GenericJobInstanceFactory<Watch> {

    private static final Logger log = LogManager.getLogger(GenericWatchInstanceFactory.class);

    private final WatchInstanceParameterLoader parameterLoader;
    private final WatchInitializationService initializationService;

    public GenericWatchInstanceFactory(WatchInstanceParameterLoader parameterLoader, WatchInitializationService initializationService) {
        this.parameterLoader = Objects.requireNonNull(parameterLoader, "Parameter loader is required");
        this.initializationService = Objects.requireNonNull(initializationService, "Watch initialization service is required");
    }

    @Override
    public ImmutableList<Watch> instantiateGeneric(Watch watch) {
        Objects.requireNonNull(watch, "Watch is required");
        log.debug("Try to create watch instances for '{}'.", watch.getId());
        if(watch.hasParameters()) {
            ImmutableList<WatchParametersData> parameters = parameterLoader.findParameters(watch.getId());
            if(parameters.isEmpty()) {
                log.debug("Generic watch '{}' has no defined parameters, therefore instances will be not created.", watch.getId());
                return ImmutableList.empty();
            }
            log.debug("Watch '{}' has defined '{}' instances.", watch.getId(), parameters.size());
            List<Watch> watchInstances = parameters.stream() //
                .filter(WatchParametersData::isEnabled) //
                .map(instanceParameters -> createInstanceForParameter(watch, instanceParameters)) //
                .collect(Collectors.toList());
            log.debug("Watch '{}' has defined '{}' enabled instances.", watch.getId(), watchInstances.size());
            return ImmutableList.of(watchInstances);
        }
        log.debug("Watch '{}' is not generic, instance will not be created", watch.getId());
        return ImmutableList.of(watch);
    }

    private Watch createInstanceForParameter(Watch watch, WatchParametersData instanceParameters) {
        String genericDefinition = watch.getGenericDefinition();
        String id = Watch.createInstanceId(watch.getId(), instanceParameters.getInstanceId());
        try {
            long version = computeVersion(watch.getVersion(), instanceParameters.getVersion());
            Watch watchInstance = Watch.parse(initializationService, watch.getTenant(), id, genericDefinition, version, instanceParameters);
            log.debug("Watch '{}' instance with id '{}' and parameters '{}' created, instance version '{}'", watch.getId(),
                instanceParameters.getInstanceId(), instanceParameters.getParameters(), version);
            return watchInstance;
        } catch (ConfigValidationException e) {
            String message = "Cannot parse generic watch '" + watch.getId() + "' instance '" + instanceParameters.getInstanceId() + "'.";
            throw new RuntimeException(message, e);
        }
    }

    private long computeVersion(long watchVersion, long parametersVersion) {
        log.debug("Watch version '{}', parameters version '{}'.", watchVersion, parametersVersion);
        if(watchVersion < 0) {
            throw new IllegalArgumentException("Watch version " + watchVersion + " is below 0");
        }
        if(parametersVersion < 0) {
            throw new IllegalArgumentException("Parameters version " + parametersVersion + " is below 0");
        }
        watchVersion = watchVersion << 32;
        return watchVersion | parametersVersion;
    }
}
