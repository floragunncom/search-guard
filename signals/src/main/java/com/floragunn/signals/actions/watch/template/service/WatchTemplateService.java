package com.floragunn.signals.actions.watch.template.service;

import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.actions.watch.template.rest.CreateOneWatchInstanceAction.CreateOneWatchInstanceRequest;
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
        //TODO check if parameters exists
        WatchParametersData watchParametersData = new WatchParametersData(request.getTenantId(), request.getWatchId(),
            request.getInstanceId(), request.getParameters());

        parametersRepository.store(watchParametersData);
        return new StandardResponse(201);
    }

    public StandardResponse getTemplateParameters(GetWatchInstanceParametersRequest request) {
        return new StandardResponse(303).message("Not implemented");
    }
}
