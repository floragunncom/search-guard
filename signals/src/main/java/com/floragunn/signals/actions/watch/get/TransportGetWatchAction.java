package com.floragunn.signals.actions.watch.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import com.floragunn.signals.watch.Watch;

import java.util.List;
import java.util.Map;

public class TransportGetWatchAction extends HandledTransportAction<GetWatchRequest, GetWatchResponse> {

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportGetWatchAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(GetWatchAction.NAME, transportService, actionFilters, GetWatchRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, GetWatchRequest request, ActionListener<GetWatchResponse> listener) {

        try {
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);
            Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);
            SignalsTenant signalsTenant = signals.getTenant(user);
            final Map<String, List<String>> originalResponseHeaders = threadContext.getResponseHeaders();

            try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {

                threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                threadContext.putTransient(ConfigConstants.SG_USER, user);
                threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

                originalResponseHeaders.entrySet().forEach(
                        h ->  h.getValue().forEach(v -> threadContext.addResponseHeader(h.getKey(), v))
                );

                client.prepareGet().setIndex(signalsTenant.getConfigIndexName()).setId(signalsTenant.getWatchIdForConfigIndex(request.getWatchId()))
                        .setFetchSource(Strings.EMPTY_ARRAY, Watch.HiddenAttributes.asArray()).execute(new ActionListener<GetResponse>() {

                            @Override
                            public void onResponse(GetResponse response) {
                                listener.onResponse(new GetWatchResponse(signalsTenant.getName(), response));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }

                        });
            }
        } catch (NoSuchTenantException e) {
            listener.onResponse(new GetWatchResponse(e.getTenant(), request.getWatchId(), false));
        } catch (SignalsUnavailableException e) {
            listener.onFailure(e.toElasticsearchException());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}