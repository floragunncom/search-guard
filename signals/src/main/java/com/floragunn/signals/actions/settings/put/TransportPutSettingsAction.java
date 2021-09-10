package com.floragunn.signals.actions.settings.put;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.signals.Signals;

public class TransportPutSettingsAction extends HandledTransportAction<PutSettingsRequest, PutSettingsResponse> {
    private static final Logger log = LogManager.getLogger(TransportPutSettingsAction.class);

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
                if (log.isDebugEnabled()) {
                    log.debug("Settings update " + request);
                }

                signals.getSignalsSettings().update(client, request.getKey(), getParsedValue(request));

                listener.onResponse(new PutSettingsResponse(Result.UPDATED, RestStatus.OK, null, null));
            } catch (ConfigValidationException e) {
                listener.onResponse(new PutSettingsResponse(Result.NOOP, RestStatus.BAD_REQUEST, e.getMessage(), e.toJsonString()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private Object getParsedValue(PutSettingsRequest request) throws ConfigValidationException {
        if (request.isValueJson()) {
            try {
               return DefaultObjectMapper.objectMapper.readValue(request.getValue(), Object.class);
            } catch (IOException e) {
                throw new ConfigValidationException(new ValidationError(request.getKey(), e.getMessage()).cause(e));
            }
        } else {
            return request.getValue();
        }
    }

}