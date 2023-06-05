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
package com.floragunn.signals.actions.watch.generic.service;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import com.floragunn.signals.actions.watch.generic.rest.UpsertManyGenericWatchInstancesAction.UpsertManyGenericWatchInstancesRequest;
import com.floragunn.signals.actions.watch.generic.rest.UpsertOneGenericWatchInstanceAction.UpsertOneGenericWatchInstanceRequest;
import com.floragunn.signals.actions.watch.generic.rest.DeleteWatchInstanceAction.DeleteWatchInstanceRequest;
import com.floragunn.signals.actions.watch.generic.rest.GetAllWatchInstancesAction.GetAllWatchInstancesRequest;
import com.floragunn.signals.actions.watch.generic.rest.GetWatchInstanceAction.GetWatchInstanceParametersRequest;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstancesRepository;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchStateRepository;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.common.Instances;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;

public class GenericWatchService {

    private static final Logger log = LogManager.getLogger(GenericWatchService.class);

    private final static ImmutableList<Class<?>> ALLOWED_PARAMETER_TYPES = ImmutableList.of(String.class, Number.class, Boolean.class,
        Date.class);

    private final Signals signals;
    private final WatchInstancesRepository instancesRepository;
    private final WatchStateRepository stateRepository;
    private final SchedulerConfigUpdateNotifier notifier;


    GenericWatchService(Signals signals, WatchInstancesRepository instancesRepository, WatchStateRepository stateRepository,
        SchedulerConfigUpdateNotifier notifier) {
        this.signals = Objects.requireNonNull(signals, "Signals module is required");
        this.instancesRepository = Objects.requireNonNull(instancesRepository);
        this.stateRepository = Objects.requireNonNull(stateRepository, "Watch state repository is required");
        this.notifier = Objects.requireNonNull(notifier, "Scheduler config update notifier is required.");
    }

    public StandardResponse upsert(UpsertOneGenericWatchInstanceRequest request) throws ConfigValidationException {
        Objects.requireNonNull(request, "Create one watch request is required");
        Instances instances = instanceConfiguration(request.getTenantId(), request.getWatchId());
        if(instances.isEnabled()) {
            ValidationErrors validationErrors = new ValidationErrors();
            validateParameters(validationErrors, instances, request.getInstanceId(), request.getParameters());
            validateInstanceId(validationErrors, request.getTenantId(), request.getWatchId(), request.getInstanceId());
            validationErrors.throwExceptionForPresentErrors();
            int responseCode = instancesRepository.findOneById(request.getTenantId(), request.getWatchId(), request.getInstanceId()) //
                .map(ignore -> 200) //
                .orElse(201); //
            instancesRepository.store(toWatchInstanceData(request));
            notifier.send(request.getTenantId(), () -> log.debug("Scheduler configuration updated after watch instance upserted."));
            return new StandardResponse(responseCode);
        } else {
            // Watch does not exist therefore it is not possible to create none existing watch instance
            return new StandardResponse(404)//
                .error("Generic watch with id '" + request.getWatchId() + "' does not exist.");
        }
    }

    private void validateParameters(ValidationErrors validationErrors, Instances instances, String instanceId,
        ImmutableMap<String, Object> parameters) throws ConfigValidationException {
        List<ValidationError> errorList = prepateValidationErrorList(instances, instanceId, parameters);
        validationErrors.add(errorList);
        validationErrors.throwExceptionForPresentErrors();
    }

    private List<ValidationError> prepateValidationErrorList(Instances instances, String instanceId, Map<String, Object> parameters) {
        Set<String> predefinedParameters = new HashSet<>(instances.getParams());
        Set<String> actualParameters = parameters.keySet();
        List<ValidationError> errorList = new ArrayList<>();
        errorList.addAll(validateNotAllowedParameters(instanceId, predefinedParameters, actualParameters));
        errorList.addAll(validateMissingParameters(instanceId, predefinedParameters, actualParameters));
        errorList.addAll(validateParametersValueTypes(instanceId, parameters));
        return errorList;
    }

    private static List<ValidationError> validateParametersValueTypes(String instanceId, Map<String, Object> parameters) {
        ArrayList<ValidationError> validationErrors = new ArrayList<>();
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            String patch = instanceId + "." + entry.getKey();
            validationErrors.addAll(validateParameterType(patch, entry.getValue(), true));
        }
        return validationErrors;
    }

    private static List<ValidationError> validateParameterType(String patch, Object value, boolean listAllowed) {
        if(value == null) {
            return Collections.emptyList();
        }
        List<ValidationError> errors = new ArrayList<>();
        if(listAllowed && (value instanceof List)) {
            List<?> listToValidate = (List<?>) value;
            for(int i = 0; i < listToValidate.size(); ++i) {
                errors.addAll(validateParameterType(patch + "[" + i + "]", listToValidate.get(i), false));
            }
        } else {
            boolean notAllowedParameterType = ! ALLOWED_PARAMETER_TYPES.stream()//
                .map(clazz -> clazz.isInstance(value))//
                .reduce(false, (one, two) -> one || two);
            if(notAllowedParameterType) {
                errors.add(new ValidationError(patch, "Forbidden parameter value type " + value.getClass().getSimpleName()));
            }
        }
        return errors;
    }

    private static ImmutableList<ValidationError> validateMissingParameters(String instanceId, Set<String> predefinedParameters,
        Set<String> actualParameters) {
        Set<String> missingParameters = new HashSet<>(predefinedParameters);
        missingParameters.removeAll(actualParameters);
        if(!missingParameters.isEmpty()) {
            String missing = missingParameters.stream().map(name -> String.format("'%s'", name)).collect(Collectors.joining(", "));
            String message = "Watch instance does not contain required parameters: [" + missing + "]";
            return ImmutableList.of(new ValidationError(instanceId, message));
        }
        return ImmutableList.empty();
    }

    private static ImmutableList<ValidationError> validateNotAllowedParameters(String instanceId, Set<String> predefinedParameters,
        Set<String> actualParameters) {
        String requiredParameters = predefinedParameters.stream() //
            .map(name -> String.format("'%s'", name)) //
            .collect(Collectors.joining(", "));
        List<String> notAllowedParameters = actualParameters.stream() //
            .filter(name -> !predefinedParameters.contains(name)) //
            .collect(Collectors.toList());
        if(!notAllowedParameters.isEmpty()) {
            String incorrectParameters = notAllowedParameters.stream() //
                .map(name -> String.format("'%s'", name)) //
                .collect(Collectors.joining(","));
            String message = "Incorrect parameter names: [" + incorrectParameters + "]. Valid parameter names: [" + requiredParameters + "]";
            return ImmutableList.of(new ValidationError(instanceId, message));
        }
        return ImmutableList.empty();
    }

    private void validateInstanceId(ValidationErrors validationErrors, String tenant, String watchId, String...instanceIds) {
        List<String> instanceIdsForFurtherValidation = new ArrayList<>();
        for(String currentInstanceId : instanceIds) {
            if (!Instances.isValidParameterName(currentInstanceId)) {
                validationErrors.add(new ValidationError(currentInstanceId, "Watch instance id is incorrect."));
            } else {
                instanceIdsForFurtherValidation.add(currentInstanceId);
            }
        }
        try {
            SignalsTenant signalsTenant = signals.getTenant(tenant);
            String[] fullInstanceIds = instanceIdsForFurtherValidation.stream() //
                .map(id -> Watch.createInstanceId(watchId, id)) //
                .toArray(String[]::new);
            ImmutableMap<String, Boolean> existingWatches = signalsTenant.watchesExist(fullInstanceIds);
            for(Map.Entry<String, Boolean> entry : existingWatches.entrySet()) {
                if(entry.getValue()) {
                    validationErrors.add(new ValidationError(entry.getKey(), "Non generic watch with the same ID already exists."));
                }
            }
        } catch (NoSuchTenantException | SignalsUnavailableException e) {
            validationErrors.add(new ValidationError(tenant, "Cannot load tenant."));
        }
    }

    private static WatchInstanceData toWatchInstanceData(UpsertOneGenericWatchInstanceRequest request) {
        return new WatchInstanceData(request.getTenantId(), request.getWatchId(), request.getInstanceId(), true, request.getParameters());
    }

    public StandardResponse getWatchInstanceParameters(GetWatchInstanceParametersRequest request) {
        Objects.requireNonNull(request, "Get generic watch parameters request is required");
        return instancesRepository.findOneById(request.getTenantId(), request.getWatchId(), request.getInstanceId())//
            .map(watchParameters -> new StandardResponse(200).data(watchParameters.getParameters()))//
            .orElseGet(() -> new StandardResponse(404));
    }

    public StandardResponse deleteWatchInstance(DeleteWatchInstanceRequest request) {
        Objects.requireNonNull(request, "Delete watch instance request is required");
        boolean deleted = instancesRepository.delete(request.getTenantId(), request.getWatchId(), request.getInstanceId());
        if(deleted) {
            ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse deleteResponse) {
                    log.debug("Generic watch '{}' instance '{}' state deleted for tenant '{}'", request.getWatchId(),
                        request.getInstanceId(), request.getTenantId());
                }

                @Override
                public void onFailure(Exception e) {
                    log.warn("Generic watch '{}' instance '{}' state was not deleted for tenant '{}'", request.getWatchId(),
                        request.getInstanceId(), request.getTenantId());
                }
            };
            ImmutableList<String> instances = ImmutableList.of(request.getInstanceId());
            notifier.send(request.getTenantId(),
                () -> stateRepository.deleteInstanceState(request.getTenantId(), request.getWatchId(), instances, listener));

            return new StandardResponse(200);
        } else {
            return new StandardResponse(404);
        }
    }

    public StandardResponse upsertManyInstances(UpsertManyGenericWatchInstancesRequest request) throws ConfigValidationException {
        Objects.requireNonNull(request, "Create watch instances request is required");
        Instances instances = instanceConfiguration(request.getTenantId(), request.getWatchId());
        if(instances.isEnabled()) {
            ValidationErrors validationErrors = new ValidationErrors();
            validateManyInstancesParameters(validationErrors, request, instances);
            validateInstanceId(validationErrors, request.getTenantId(), request.getWatchId(), request.instanceIds().toArray(new String[0]));
            validationErrors.throwExceptionForPresentErrors();
            Set<String> existingInstanceIds = findUpdatedWatchesIds(request);
            boolean update = !existingInstanceIds.isEmpty();
            WatchInstanceData[] watchInstanceData = request.toCreateOneWatchInstanceRequest().stream()//
                .map(GenericWatchService::toWatchInstanceData).toArray(WatchInstanceData[]::new);
            instancesRepository.store(watchInstanceData);
            notifier.send(request.getTenantId(), () -> log.debug("Scheduler configuration updated after watches instance upserted."));
            return new StandardResponse(update ? 200 : 201);
        } else {
            return new StandardResponse(404).message("No such watch template.");
        }
    }

    private void validateManyInstancesParameters(ValidationErrors validationErrors,
        UpsertManyGenericWatchInstancesRequest upsertManyGenericWatchInstancesRequest, Instances instances) {
        List<ValidationError> errorList = upsertManyGenericWatchInstancesRequest.toCreateOneWatchInstanceRequest() //
                .stream() //
                .flatMap(request -> prepateValidationErrorList(instances, request.getInstanceId(), request.getParameters()).stream()) //
                .collect(Collectors.toList());
        validationErrors.add(errorList);
    }

    private ImmutableSet<String> findUpdatedWatchesIds(UpsertManyGenericWatchInstancesRequest request) {
        Set<String> existingInstanceIds = instancesRepository.findByWatchId(request.getTenantId(), request.getWatchId())//
                .stream()//
                .map(WatchInstanceData::getInstanceId)//
                .collect(Collectors.toSet());
        existingInstanceIds.retainAll(request.instanceIds());
        return ImmutableSet.of(existingInstanceIds);
    }

    public StandardResponse findAllInstances(GetAllWatchInstancesRequest request) {
        Objects.requireNonNull(request, "Request is required");
        Map<String, ImmutableMap<String, Object>> watchInstances = instancesRepository //
            .findByWatchId(request.getTenantId(), request.getWatchId())//
            .stream()//
            .collect(Collectors.toMap(WatchInstanceData::getInstanceId, WatchInstanceData::getParameters));
        if(watchInstances.isEmpty()) {
            return new StandardResponse(404);
        }
        return new StandardResponse(200).data(ImmutableMap.of(watchInstances));
    }

    private Instances instanceConfiguration(String tenantName, String watchId) {
        try {
            SignalsTenant tenant = signals.getTenant(tenantName);
            return tenant.findGenericWatchInstanceConfig(watchId).orElse(Instances.EMPTY);
        } catch (SignalsUnavailableException | NoSuchTenantException e) {
            log.warn("Cannot find tenant '{}',", tenantName, e);
            return Instances.EMPTY;
        }
    }


    public void deleteAllInstanceParametersWithState(String tenantId, String watchId) {
        Objects.requireNonNull(tenantId, "Tenant id is required");
        Objects.requireNonNull(watchId, "Watch id is required");
        ImmutableList<String> deletedInstancesIds = instancesRepository.deleteByWatchId(tenantId, watchId);
        if(!deletedInstancesIds.isEmpty()) {
            stateRepository.deleteInstanceState(tenantId, watchId, deletedInstancesIds, new ActionListener<BulkResponse>() {
                @Override public void onResponse(BulkResponse bulkItemResponses) {
                    log.debug("State related to generic watches deleted");
                }

                @Override public void onFailure(Exception e) {
                    log.warn("Cannot delete state related to generic watches", e);
                }
            });
        }
    }

    public StandardResponse switchEnabledFlag(String tenantId, String watchId, String instanceId, boolean enable) {
        Objects.requireNonNull(tenantId, "Tenant ID is required");
        Objects.requireNonNull(watchId, "Watch ID is required");
        Objects.requireNonNull(instanceId, "Instance ID is required");
        if(instancesRepository.updateEnabledFlag(tenantId, watchId, instanceId, enable)) {
            notifier.send(tenantId, () -> log.debug("Scheduler configuration updated after watch instance enabled '{}'.", enable));
            log.debug("Watch '{}' instance '{}' defined for tenant '{}' has updated value of flag enabled to '{}'.", watchId,
                instanceId, tenantId, enable);
            return new StandardResponse(SC_OK);
        }
        return new StandardResponse(SC_NOT_FOUND) //
            .error("Watch '" + watchId + "' instance '" + instanceId + "' for tenant '" + tenantId + "' does not exist.");
    }
}
