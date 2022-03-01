package com.floragunn.signals.actions.watch.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
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
        super(DeleteWatchAction.NAME, transportService, actionFilters, DeleteWatchRequest::new);

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
            Object originalRemoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Object originalOrigin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

            try (StoredContext ctx = threadContext.stashContext()) {

                threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                threadContext.putTransient(ConfigConstants.SG_USER, user);
                threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, originalRemoteAddress);
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, originalOrigin);

                String idInIndex = signalsTenant.getWatchIdForConfigIndex(request.getWatchId());

                client.prepareDelete(signalsTenant.getConfigIndexName(), null, idInIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .execute(new ActionListener<DeleteResponse>() {
                            @Override
                            public void onResponse(DeleteResponse response) {

                                if (response.getResult() == Result.DELETED) {
                                    SchedulerConfigUpdateAction.send(client, signalsTenant.getScopedName());
                                }

                                try (StoredContext ctx = threadContext.stashContext()) {

                                    threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                                    threadContext.putTransient(ConfigConstants.SG_USER, user);
                                    threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, originalRemoteAddress);
                                    threadContext.putTransient(ConfigConstants.SG_ORIGIN, originalOrigin);

                                    client.prepareDelete(signalsTenant.getSettings().getStaticSettings().getIndexNames().getWatchesState(), null,
                                            idInIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).execute(new ActionListener<DeleteResponse>() {

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
                                }
                                
                                listener.onResponse(new DeleteWatchResponse(request.getWatchId(), response.getVersion(), response.getResult(),
                                        response.status(), null));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
            }
        } catch (NoSuchTenantException e) {
            listener.onResponse(new DeleteWatchResponse(request.getWatchId(), -1, Result.NOT_FOUND, RestStatus.NOT_FOUND, e.getMessage()));
        } catch (SignalsUnavailableException e) {
            listener.onFailure(e.toOpenSearchException());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}