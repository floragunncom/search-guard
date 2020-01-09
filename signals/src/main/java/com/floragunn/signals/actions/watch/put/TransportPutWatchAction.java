package com.floragunn.signals.actions.watch.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;

public class TransportPutWatchAction extends HandledTransportAction<PutWatchRequest, PutWatchResponse> {
    private static final Logger log = LogManager.getLogger(TransportPutWatchAction.class);

    private final Signals signals;

    private final ThreadPool threadPool;

    @Inject
    public TransportPutWatchAction(Signals signals, TransportService transportService, ScriptService scriptService, ThreadPool threadPool,
            ActionFilters actionFilters) {
        super(PutWatchAction.NAME, transportService, actionFilters, PutWatchRequest::new);

        this.signals = signals;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, PutWatchRequest request, ActionListener<PutWatchResponse> listener) {

        ThreadContext threadContext = threadPool.getThreadContext();

        User user = threadContext.getTransient(ConfigConstants.SG_USER);

        if (user == null) {
            listener.onResponse(
                    new PutWatchResponse(request.getWatchId(), -1, Result.NOOP, RestStatus.UNAUTHORIZED, "Request did not contain user", null));
            return;
        }

        SignalsTenant signalsTenant = signals.getTenant(user);

        if (signalsTenant == null) {
            listener.onResponse(new PutWatchResponse(request.getWatchId(), -1, Result.NOT_FOUND, RestStatus.NOT_FOUND,
                    "No such tenant: " + user.getRequestedTenant(), null));
            return;
        }

        threadPool.generic().submit(() -> {
            try {
                IndexResponse response = signalsTenant.addWatch(request.getWatchId(), request.getBody().utf8ToString(), user);

                listener.onResponse(
                        new PutWatchResponse(request.getWatchId(), response.getVersion(), response.getResult(), response.status(), null, null));
            } catch (ConfigValidationException e) {
                log.info("Invalid watch supplied to PUT " + request.getWatchId() + ":\n" + e.toString(), e);
                listener.onResponse(new PutWatchResponse(request.getWatchId(), -1, Result.NOOP, RestStatus.BAD_REQUEST,
                        "Watch is invalid: " + e.getMessage(), e.getValidationErrors().toJson()));
                return;

            } catch (Exception e) {
                log.error("Error while saving watch: ", e);
                listener.onFailure(e);
                return;

            }
        });
    }
}