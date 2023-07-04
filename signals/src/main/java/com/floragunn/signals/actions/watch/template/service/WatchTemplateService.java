package com.floragunn.signals.actions.watch.template.service;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import com.floragunn.signals.actions.watch.template.rest.CreateManyWatchInstancesAction.CreateManyWatchInstances;
import com.floragunn.signals.actions.watch.template.rest.CreateOrUpdateOneWatchInstanceAction.CreateOrUpdateOneWatchInstanceRequest;
import com.floragunn.signals.actions.watch.template.rest.DeleteWatchInstanceAction.DeleteWatchInstanceRequest;
import com.floragunn.signals.actions.watch.template.rest.GetAllWatchInstancesAction.GetAllWatchInstancesRequest;
import com.floragunn.signals.actions.watch.template.rest.GetWatchInstanceParametersAction.GetWatchInstanceParametersRequest;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersData;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersRepository;
import com.floragunn.signals.watch.common.Instances;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class WatchTemplateService {

    private static final Logger log = LogManager.getLogger(WatchTemplateService.class);

    private final Signals signals;
    private final WatchParametersRepository parametersRepository;
    private final SchedulerConfigUpdateNotifier notifier;


    WatchTemplateService(Signals signals, WatchParametersRepository parametersRepository, SchedulerConfigUpdateNotifier notifier) {
        this.signals = Objects.requireNonNull(signals, "Signals module is required");
        this.parametersRepository = Objects.requireNonNull(parametersRepository);
        this.notifier = Objects.requireNonNull(notifier, "Scheduler config update notifier is required.");
    }

    public StandardResponse createOrReplace(CreateOrUpdateOneWatchInstanceRequest request) throws ConfigValidationException {
        Objects.requireNonNull(request, "Create one watch request is required");
        Instances instances = instanceConfiguration(request.getTenantId(), request.getWatchId());
        if(instances.isEnabled()) {
            validateParameters(instances, request.getInstanceId(), request.getParameters());
            int responseCode = parametersRepository.findOneById(request.getTenantId(), request.getWatchId(), request.getInstanceId()) //
                .map(ignore -> 200) //
                .orElse(201); //
            parametersRepository.store(toWatchParameterData(request));
            notifier.send();
            return new StandardResponse(responseCode);
        } else {
            // Watch does not exist therefore it is not possible to create none existing watch instance
            return new StandardResponse(404).error("Watch template with id " + request.getWatchId() + " does not exist.");
        }
    }

    private void validateParameters(Instances instances, String instanceId, ImmutableMap<String, Object> parameters)
        throws ConfigValidationException {
        Set<String> predefinedParameters = new HashSet<>(instances.getParams());
        String requiredParameters = predefinedParameters.stream() //
            .map(name -> String.format("'%s'", name)) //
            .collect(Collectors.joining(", "));
        ImmutableSet<String> actualParameters = parameters.keySet();
        ValidationErrors validationErrors = new ValidationErrors();
        if(! Instances.isValidParameterName(instanceId)) {
            validationErrors.add(new ValidationError("instance.id", "Watch instance id is incorrect."));
        }
        List<String> notAllowedParameters = actualParameters.stream() //
            .filter(name -> !predefinedParameters.contains(name)) //
            .collect(Collectors.toList());
        if(!notAllowedParameters.isEmpty()) {
            String incorrectParameters = notAllowedParameters.stream() //
                .map(name -> String.format("'%s'", name)) //
                .collect(Collectors.joining(","));
            String message = "Incorrect parameter names: [" + incorrectParameters + "]. Valid parameter names: [" + requiredParameters + "]";
            validationErrors.add(new ValidationError(instanceId, message));
        }
        Set<String> missingParameters = new HashSet<>(predefinedParameters);
        missingParameters.removeAll(actualParameters);
        if(!missingParameters.isEmpty()) {
            String missing = missingParameters.stream().map(name -> String.format("'%s'", name)).collect(Collectors.joining(", "));
            String message = "Watch instance does not contain required parameters: [" + missing + "]";
            validationErrors.add(new ValidationError(instanceId, message));
        }
        validationErrors.throwExceptionForPresentErrors();
    }

    private static WatchParametersData toWatchParameterData(CreateOrUpdateOneWatchInstanceRequest request) {
        return new WatchParametersData(request.getTenantId(), request.getWatchId(), request.getInstanceId(), request.getParameters());
    }

    public StandardResponse getTemplateParameters(GetWatchInstanceParametersRequest request) {
        Objects.requireNonNull(request, "Get template parameters request is required");
        return parametersRepository.findOneById(request.getTenantId(), request.getWatchId(), request.getInstanceId())//
            .map(watchParameters -> new StandardResponse(200).data(watchParameters.getParameters()))//
            .orElseGet(() -> new StandardResponse(404));
    }

    public StandardResponse deleteWatchInstance(DeleteWatchInstanceRequest request) {
        Objects.requireNonNull(request, "Delete watch instance request is required");
        boolean deleted = parametersRepository.delete(request.getTenantId(), request.getWatchId(), request.getInstanceId());
        if(deleted) {
            notifier.send();
            return new StandardResponse(200);
        } else {
            return new StandardResponse(404);
        }
    }

    public StandardResponse createManyInstances(CreateManyWatchInstances request) throws ConfigValidationException {
        Objects.requireNonNull(request, "Create watch instances request is required");
        Instances instances = instanceConfiguration(request.getTenantId(), request.getWatchId());
        if(instances.isEnabled()) {
            for(CreateOrUpdateOneWatchInstanceRequest singleRequest : request.toCreateOneWatchInstanceRequest()) {
                validateParameters(instances, singleRequest.getInstanceId(), singleRequest.getParameters());
            }
            Set<String> existingInstanceIds = findUpdatedWatchesIds(request);
            boolean update = !existingInstanceIds.isEmpty();
            WatchParametersData[] watchParametersData = request.toCreateOneWatchInstanceRequest().stream()//
                .map(WatchTemplateService::toWatchParameterData).toArray(WatchParametersData[]::new);
            parametersRepository.store(watchParametersData);
            notifier.send();
            return new StandardResponse(update ? 200 : 201);
        } else {
            return new StandardResponse(404).message("No such watch template.");
        }
    }

    private ImmutableSet<String> findUpdatedWatchesIds(CreateManyWatchInstances request) {
        Set<String> existingInstanceIds = parametersRepository.findByWatchId(request.getTenantId(), request.getWatchId())//
                .stream()//
                .map(WatchParametersData::getInstanceId)//
                .collect(Collectors.toSet());
        existingInstanceIds.retainAll(request.instanceIds());
        return ImmutableSet.of(existingInstanceIds);
    }

    public StandardResponse findAllInstances(GetAllWatchInstancesRequest request) {
        Objects.requireNonNull(request, "Request is required");
        Map<String, ImmutableMap<String, Object>> watchInstances = parametersRepository //
            .findByWatchId(request.getTenantId(), request.getWatchId())//
            .stream()//
            .collect(Collectors.toMap(WatchParametersData::getInstanceId, WatchParametersData::getParameters));
        if(watchInstances.isEmpty()) {
            return new StandardResponse(404);
        }
        return new StandardResponse(200).data(ImmutableMap.of(watchInstances));
    }

    private Instances instanceConfiguration(String tenantName, String watchId) {
        try {
            SignalsTenant tenant = signals.getTenant(tenantName);
            return tenant.findGenericWatchInstanceConfig(watchId);
        } catch (SignalsUnavailableException | NoSuchTenantException e) {
            log.warn("Cannot find tenant '{}',", tenantName, e);
            return Instances.EMPTY;
        }
    }


    public void deleteAllInstanceParameters(String tenantId, String watchId) {
        Objects.requireNonNull(tenantId, "Tenant id is required");
        Objects.requireNonNull(watchId, "Watch id is required");
        parametersRepository.deleteByWatchId(tenantId, watchId);
    }
}
