package com.floragunn.signals.actions.watch.template.service;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import com.floragunn.signals.actions.watch.template.rest.CreateManyWatchInstancesAction.CreateManyWatchInstances;
import com.floragunn.signals.actions.watch.template.rest.CreateOneWatchInstanceAction.CreateOneWatchInstanceRequest;
import com.floragunn.signals.actions.watch.template.rest.DeleteWatchInstanceAction.DeleteWatchInstanceRequest;
import com.floragunn.signals.actions.watch.template.rest.GetAllWatchInstancesAction.GetAllWatchInstancesRequest;
import com.floragunn.signals.actions.watch.template.rest.GetWatchInstanceParametersAction.GetWatchInstanceParametersRequest;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersData;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class WatchTemplateService {

    private static final Logger log = LogManager.getLogger(WatchTemplateService.class);

    private final Signals signals;
    private final WatchParametersRepository parametersRepository;

    public WatchTemplateService(Signals signals, WatchParametersRepository parametersRepository) {
        this.signals = Objects.requireNonNull(signals, "Signals module is required");
        this.parametersRepository = Objects.requireNonNull(parametersRepository);
    }

    public StandardResponse createOrReplace(CreateOneWatchInstanceRequest request) throws ConfigValidationException {
        Objects.requireNonNull(request, "Create one watch request is required");
        if(templateExists(request.getTenantId(), request.getWatchId())) {
            int responseCode = parametersRepository.findOneById(request.getTenantId(), request.getWatchId(), request.getInstanceId()) //
                .map(ignore -> 200) //
                .orElse(201); //
            parametersRepository.store(toWatchParameterData(request));
            return new StandardResponse(responseCode);
        } else {
            return new StandardResponse(404).error("Watch template with id " + request.getWatchId() + " does not exist.");
        }
    }

    private static WatchParametersData toWatchParameterData(CreateOneWatchInstanceRequest request) {
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
        return new StandardResponse(deleted ? 200 : 404);
    }

    public StandardResponse createManyInstances(CreateManyWatchInstances request) throws ConfigValidationException {
        Objects.requireNonNull(request, "Create watch instances request is required");
        if(templateExists(request.getTenantId(), request.getWatchId())) {
            Set<String> existingInstanceIds = findUpdatedWatchesIds(request);
            boolean update = !existingInstanceIds.isEmpty();
            WatchParametersData[] watchParametersData = request.toCreateOneWatchInstanceRequest().stream()//
                .map(WatchTemplateService::toWatchParameterData).toArray(WatchParametersData[]::new);
            parametersRepository.store(watchParametersData);
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

    private boolean templateExists(String tenantName, String watchId) {
        try {
            SignalsTenant tenant = signals.getTenant(tenantName);
            return tenant.watchTemplateExist(watchId);
        } catch (SignalsUnavailableException | NoSuchTenantException e) {
            log.warn("Cannot find tenant '{}',", tenantName);
            return false;
        }
    }
}
