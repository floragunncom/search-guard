package com.floragunn.signals.actions.account.search;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;

public class TransportSearchAccountAction extends HandledTransportAction<SearchAccountRequest, SearchAccountResponse> {

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportSearchAccountAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(SearchAccountAction.NAME, transportService, actionFilters, SearchAccountRequest::new);

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, SearchAccountRequest request, ActionListener<SearchAccountResponse> listener) {
        try {
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                throw new Exception("No user set");
            }

            SignalsTenant signalsTenant = signals.getTenant(user);

            if (signalsTenant == null) {
                throw new Exception("Unknown tenant: " + user.getRequestedTenant());
            }

            Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

            try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {

                threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                threadContext.putTransient(ConfigConstants.SG_USER, user);
                threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

                SearchRequest searchRequest = new SearchRequest(this.signals.getSignalsSettings().getStaticSettings().getIndexNames().getAccounts());

                if (request.getScroll() != null) {
                    searchRequest.scroll(request.getScroll());
                }

                SearchSourceBuilder searchSourceBuilder = request.getSearchSourceBuilder();

                if (searchSourceBuilder == null) {
                    searchSourceBuilder = new SearchSourceBuilder();
                    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
                }

                if (request.getFrom() != -1) {
                    searchSourceBuilder.from(request.getFrom());
                }

                if (request.getSize() != -1) {
                    searchSourceBuilder.size(request.getSize());
                }

                searchRequest.source(searchSourceBuilder);

                client.execute(SearchAction.INSTANCE, searchRequest, new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse response) {

                        listener.onResponse(new SearchAccountResponse(response));
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