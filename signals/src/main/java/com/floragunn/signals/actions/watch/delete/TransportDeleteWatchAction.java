package com.floragunn.signals.actions.watch.delete;

import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigContext;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;

public class TransportDeleteWatchAction extends HandledTransportAction<DeleteWatchRequest, DeleteWatchResponse> {
    private static final Logger log = LogManager.getLogger(TransportDeleteWatchAction.class);

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportDeleteWatchAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(DeleteWatchAction.NAME, transportService, actionFilters, DeleteWatchRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, DeleteWatchRequest request, ActionListener<DeleteWatchResponse> listener) {
        try {
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                listener.onResponse(
                        new DeleteWatchResponse(request.getWatchId(), -1, Result.NOOP, RestStatus.UNAUTHORIZED, "Request did not contain user"));
                return;
            }

            SignalsTenant signalsTenant = signals.getTenant(user);
            Supplier<StoredContext> callerContext = threadContext.newRestorableContext(false);

            try (StoredContext ctx = PrivilegedConfigContext.initPrivilegedContext(threadContext)) {
                String idInIndex = signalsTenant.getWatchIdForConfigIndex(request.getWatchId());

                client.prepareDelete().setIndex(signalsTenant.getConfigIndexName()).setId(idInIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .execute(new ActionListener<DeleteResponse>() {
                            @Override
                            public void onResponse(DeleteResponse response) {

                                if (response.getResult() == Result.DELETED) {
                                    SchedulerConfigUpdateAction.send(client, signalsTenant.getScopedName());
                                }

                                client.prepareDelete().setIndex(signalsTenant.getSettings().getStaticSettings().getIndexNames().getWatchesState())
                                            .setId(idInIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).execute(new ActionListener<DeleteResponse>() {

                                                @Override
                                                public void onResponse(DeleteResponse response) {
                                                    if (log.isDebugEnabled()) {
                                                        log.debug("Result of deleting state " + idInIndex + "\n" + Strings.toString(response));
                                                    }
                                                }

                                                @Override
                                                public void onFailure(Exception e) {
                                                    log.error("Error while deleting state " + idInIndex, e);
                                                }

                                            });

                                try (StoredContext ctx = callerContext.get()) {
                                    listener.onResponse(new DeleteWatchResponse(request.getWatchId(), response.getVersion(), response.getResult(),
                                            response.status(), null));
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                try (StoredContext ctx = callerContext.get()) {
                                    listener.onFailure(e);
                                }
                            }
                        });
            }
        } catch (NoSuchTenantException e) {
            listener.onResponse(new DeleteWatchResponse(request.getWatchId(), -1, Result.NOT_FOUND, RestStatus.NOT_FOUND, e.getMessage()));
        } catch (SignalsUnavailableException e) {
            listener.onFailure(e.toElasticsearchException());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
