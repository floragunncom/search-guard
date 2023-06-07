package com.floragunn.signals.actions.watch.template.service;

import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.actions.watch.template.rest.CreateOneWatchInstanceAction.CreateOneWatchInstanceRequest;
import com.floragunn.signals.actions.watch.template.rest.DeleteWatchInstanceAction.DeleteWatchInstanceRequest;
import com.floragunn.signals.actions.watch.template.rest.GetWatchInstanceParametersAction.GetWatchInstanceParametersRequest;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersData;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersRepository;

import java.util.Objects;

public class WatchTemplateService {

    private final WatchParametersRepository parametersRepository;

    public WatchTemplateService(WatchParametersRepository parametersRepository) {
        this.parametersRepository = Objects.requireNonNull(parametersRepository);
    }

    public StandardResponse createOrUpdate(CreateOneWatchInstanceRequest request) {
        Objects.requireNonNull(request, "Create one watch request is required");
        //TODO check if parameters exists
        WatchParametersData watchParametersData = new WatchParametersData(request.getTenantId(), request.getWatchId(),
            request.getInstanceId(), request.getParameters());

        parametersRepository.store(watchParametersData);
        return new StandardResponse(201);
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
}
