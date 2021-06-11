package com.floragunn.signals.actions.settings.put;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidationError;
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
                listener.onResponse(new PutSettingsResponse(Result.NOOP, RestStatus.BAD_REQUEST, e.getMessage(), Strings.toString(e)));
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