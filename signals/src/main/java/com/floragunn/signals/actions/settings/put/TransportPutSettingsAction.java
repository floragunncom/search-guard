package com.floragunn.signals.actions.settings.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.signals.Signals;

public class TransportPutSettingsAction extends HandledTransportAction<PutSettingsRequest, PutSettingsResponse> {

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportPutSettingsAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(PutSettingsAction.NAME, transportService, actionFilters, PutSettingsRequest::new);

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, PutSettingsRequest request, ActionListener<PutSettingsResponse> listener) {

        this.threadPool.generic().submit(() -> {
            try {
                signals.getSignalsSettings().update(client, request.getKey(), request.getValue());

                listener.onResponse(new PutSettingsResponse(Result.UPDATED, RestStatus.OK, null, null));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

}