package com.floragunn.signals.actions.watch.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.ScriptService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;

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

        try {
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                throw new Exception("Request did not contain user");
            }

            SignalsTenant signalsTenant = signals.getTenant(user);
                        
            threadPool.generic().submit(threadPool.getThreadContext().preserveContext(() -> {
                try {
                    DiagnosticContext.fixupLoggingContext(threadContext);
                    
                    IndexResponse response = signalsTenant.addWatch(request.getWatchId(), request.getBody().utf8ToString(), user);

                    listener.onResponse(
                            new PutWatchResponse(request.getWatchId(), response.getVersion(), response.getResult(), response.status(), null, null));
                } catch (ConfigValidationException e) {
                    log.info("Invalid watch supplied to PUT " + request.getWatchId() + ":\n" + e.toString(), e);
                    listener.onResponse(new PutWatchResponse(request.getWatchId(), -1, Result.NOOP, RestStatus.BAD_REQUEST,
                            "Watch is invalid: " + e.getMessage(), e.getValidationErrors().toJsonString()));
                } catch (Exception e) {
                    log.error("Error while saving watch: ", e);
                    listener.onFailure(e);
                }
            }));
        } catch (NoSuchTenantException e) {
            listener.onResponse(new PutWatchResponse(request.getWatchId(), -1, Result.NOT_FOUND, RestStatus.NOT_FOUND, e.getMessage(), null));
        } catch (SignalsUnavailableException e) {
            listener.onFailure(e.toOpenSearchException());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}