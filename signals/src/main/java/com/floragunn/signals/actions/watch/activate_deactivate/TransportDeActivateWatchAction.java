package com.floragunn.signals.actions.watch.activate_deactivate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;

import java.util.List;
import java.util.Map;

public class TransportDeActivateWatchAction extends HandledTransportAction<DeActivateWatchRequest, DeActivateWatchResponse> {
    private static final Logger log = LogManager.getLogger(TransportDeActivateWatchAction.class);

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportDeActivateWatchAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(DeActivateWatchAction.NAME, transportService, actionFilters, DeActivateWatchRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, DeActivateWatchRequest request, ActionListener<DeActivateWatchResponse> listener) {
        try (XContentBuilder watchContentBuilder = XContentFactory.jsonBuilder()) {

            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                listener.onResponse(
                        new DeActivateWatchResponse(request.getWatchId(), -1, Result.NOOP, RestStatus.UNAUTHORIZED, "Request did not contain user"));
                return;
            }

            SignalsTenant signalsTenant = signals.getTenant(user);

            if (signalsTenant == null) {
                listener.onResponse(new DeActivateWatchResponse(request.getWatchId(), -1, Result.NOT_FOUND, RestStatus.NOT_FOUND,
                        "No such tenant: " + user.getRequestedTenant()));
                return;
            }

            Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);
            final Map<String, List<String>> originalResponseHeaders = threadContext.getResponseHeaders();

            try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {

                threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                threadContext.putTransient(ConfigConstants.SG_USER, user);
                threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

                originalResponseHeaders.entrySet().forEach(
                        h ->  h.getValue().forEach(v -> threadContext.addResponseHeader(h.getKey(), v))
                );

                UpdateRequest updateRequest = new UpdateRequest(signalsTenant.getConfigIndexName(),
                        signalsTenant.getWatchIdForConfigIndex(request.getWatchId()));
                updateRequest.doc("active", request.isActivate());
                updateRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

                client.update(updateRequest, new ActionListener<UpdateResponse>() {

                    @Override
                    public void onResponse(UpdateResponse response) {
                        if (log.isDebugEnabled()) {
                            log.debug("Got response " + response + " for " + updateRequest);
                        }

                        if (response.getResult() == UpdateResponse.Result.UPDATED) {
                            SchedulerConfigUpdateAction.send(client, signalsTenant.getScopedName());

                            listener.onResponse(new DeActivateWatchResponse(request.getWatchId(), response.getVersion(), response.getResult(),
                                    RestStatus.OK, null));
                        } else if (response.getResult() == UpdateResponse.Result.NOOP) {
                            // Nothing changed

                            listener.onResponse(new DeActivateWatchResponse(request.getWatchId(), response.getVersion(), response.getResult(),
                                    RestStatus.OK, null));
                        } else if (response.getResult() == UpdateResponse.Result.NOT_FOUND) {
                            listener.onResponse(new DeActivateWatchResponse(request.getWatchId(), response.getVersion(),
                                    UpdateResponse.Result.NOT_FOUND, RestStatus.NOT_FOUND, "No such watch: " + request.getWatchId()));
                        } else {
                            log.error("Unexpected result " + response + " in " + response + " for " + updateRequest);
                            listener.onResponse(new DeActivateWatchResponse(request.getWatchId(), response.getVersion(), response.getResult(),
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    "Unexpected result " + response.getResult() + " in " + response + " for " + updateRequest));

                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                });
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}