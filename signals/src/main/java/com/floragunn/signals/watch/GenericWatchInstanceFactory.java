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

import java.io.IOException;
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

    /**
     * Create instances of generic {@link Watch}. In case of non-generic {@link Watch}s the {@link Watch} is returned.
     *
     * @param watch which is generic or not, please see method {@link Watch#isGenericJobConfig()}
     * @return instances of generic {@link Watch} or in case of non-generic {@link Watch} the {@link Watch} itself
     * (inside one element {@link ImmutableList}).
     */
    @Override
    public ImmutableList<Watch> instantiateGeneric(Watch watch) {
        Objects.requireNonNull(watch, "Watch is required");
        log.debug("Try to create watch instances for '{}'.", watch.getId());
        if(watch.isGenericJobConfig()) {
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
        return instancesLoader.findOne(genericWatch.getId(), instanceId) //
            .map(instanceParameters -> createInstanceForParameter(genericWatch, instanceParameters));
    }

    private Watch createInstanceForParameter(Watch watch, WatchInstanceData instanceParameters) {
        if(!watch.isGenericJobConfig()) {
            String message = "Instances can be created only for generic watches. Watch '" + watch.getId() + "' is not generic";
            throw new IllegalArgumentException(message);
        }

        try {
            Watch watchInstance = watch.createInstance(instanceParameters);
            log.debug("Watch '{}' instance with id '{}' and parameters '{}' created, instance version '{}'", watch.getId(),
                instanceParameters.getInstanceId(), instanceParameters.getParameters(), watchInstance.getVersion());
            return watchInstance;
        } catch (CloneNotSupportedException | IOException | ConfigValidationException e) {
            String message = "Cannot clone generic watch '" + watch.getId() + "' instance '" + instanceParameters.getInstanceId() + "'.";
            throw new RuntimeException(message, e);
        }
    }
}
