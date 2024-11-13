package com.floragunn.signals.actions.watch.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;

import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;

public class TransportPutWatchAction extends HandledTransportAction<PutWatchRequest, PutWatchResponse> {
    private static final Logger log = LogManager.getLogger(TransportPutWatchAction.class);

    private final Signals signals;

    private final ThreadPool threadPool;

    @Inject
    public TransportPutWatchAction(Signals signals, TransportService transportService, ScriptService scriptService, ThreadPool threadPool,
            ActionFilters actionFilters) {
        super(PutWatchAction.NAME, transportService, actionFilters, PutWatchRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));

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

                    DocWriteResponse response = signalsTenant.addWatch(request.getWatchId(), request.getBody().utf8ToString(), user, STRICT);

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
            listener.onFailure(e.toElasticsearchException());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}