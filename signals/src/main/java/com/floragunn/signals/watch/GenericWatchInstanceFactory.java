package com.floragunn.signals.watch;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.jobs.cluster.CurrentNodeJobSelector;
import com.floragunn.searchsupport.jobs.config.GenericJobInstanceFactory;
import com.floragunn.signals.actions.watch.generic.service.WatchInstancesLoader;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData;
import com.floragunn.signals.watch.init.WatchInitializationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class GenericWatchInstanceFactory implements GenericJobInstanceFactory<Watch> {

    private static final Logger log = LogManager.getLogger(GenericWatchInstanceFactory.class);

    private final WatchInstancesLoader instancesLoader;
    private final WatchInitializationService initializationService;

    private final CurrentNodeJobSelector currentNodeJobSelector;

    public GenericWatchInstanceFactory(WatchInstancesLoader instancesLoader, WatchInitializationService initializationService,
        CurrentNodeJobSelector currentNodeJobSelector) {
        this.instancesLoader = Objects.requireNonNull(instancesLoader, "Parameter loader is required");
        this.initializationService = Objects.requireNonNull(initializationService, "Watch initialization service is required");
        this.currentNodeJobSelector = Objects.requireNonNull(currentNodeJobSelector, "Job distributor is required");
        log.info("Generic watch instance factory created with usage of current node job selector '{}'", currentNodeJobSelector);
    }

    @Override
    public ImmutableList<Watch> instantiateGeneric(Watch watch) {
        Objects.requireNonNull(watch, "Watch is required");
        log.debug("Try to create watch instances for '{}'.", watch.getId());
        if(watch.hasParameters()) {
            ImmutableList<WatchInstanceData> parameters = instancesLoader.findInstances(watch.getId());
            if(parameters.isEmpty()) {
                log.debug("Generic watch '{}' has no defined parameters, therefore instances will be not created.", watch.getId());
                return ImmutableList.empty();
            }
            log.debug("Watch '{}' has defined '{}' instances.", watch.getId(), parameters.size());
            List<Watch> watchInstances = parameters.stream() //
                .filter(WatchInstanceData::isEnabled) //
                .map(instanceParameters -> createInstanceForParameter(watch, instanceParameters)) //
                // decide if job instance should be executed on current node
                .filter(currentNodeJobSelector::isJobSelected)
                .collect(Collectors.toList());
            log.debug("Watch '{}' has defined '{}' enabled instances.", watch.getId(), watchInstances.size());
            return ImmutableList.of(watchInstances);
        }
        log.debug("Watch '{}' is not generic, instance will not be created", watch.getId());
        return ImmutableList.of(watch);
    }

    public Optional<Watch> instantiateOne(Watch genericWatch, String instanceId) {
        Objects.requireNonNull(instanceId, "Watch instance id is required");
        if(genericWatch.isExecutable()) {
            throw new IllegalArgumentException("Cannot instantiate non generic watch " + genericWatch.getId());
        }
        String watchId = genericWatch.getGenericWatchIdOrWatchId();
        return instancesLoader.findOne(watchId, instanceId) //
            .map(instanceParameters -> createInstanceForParameter(genericWatch, instanceParameters));
    }

    private Watch createInstanceForParameter(Watch watch, WatchInstanceData instanceParameters) {
        String genericDefinition = watch.getGenericDefinition();
        // TODO this field should be always not null for generic watch. Add some validation for this, possibly in the Watch class
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
