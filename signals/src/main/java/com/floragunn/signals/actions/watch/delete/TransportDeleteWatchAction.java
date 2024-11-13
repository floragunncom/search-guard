package com.floragunn.signals.actions.watch.delete;

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
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;

import java.util.List;
import java.util.Map;

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
            Object originalRemoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Object originalOrigin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);
            final Map<String, List<String>> originalResponseHeaders = threadContext.getResponseHeaders();


            try (StoredContext ctx = threadContext.stashContext()) {

                threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                threadContext.putTransient(ConfigConstants.SG_USER, user);
                threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, originalRemoteAddress);
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, originalOrigin);

                originalResponseHeaders.entrySet().forEach(
                        h ->  h.getValue().forEach(v -> threadContext.addResponseHeader(h.getKey(), v))
                );

                String idInIndex = signalsTenant.getWatchIdForConfigIndex(request.getWatchId());

                client.prepareDelete().setIndex(signalsTenant.getConfigIndexName()).setId(idInIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
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

                                    originalResponseHeaders.entrySet().forEach(
                                            h ->  h.getValue().forEach(v -> threadContext.addResponseHeader(h.getKey(), v))
                                    );

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
            listener.onFailure(e.toElasticsearchException());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}