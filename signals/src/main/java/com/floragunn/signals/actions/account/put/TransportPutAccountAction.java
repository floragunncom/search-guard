package com.floragunn.signals.actions.account.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.accounts.Account;
import com.floragunn.signals.actions.account.config_update.DestinationConfigUpdateAction;

public class TransportPutAccountAction extends HandledTransportAction<PutAccountRequest, PutAccountResponse> {

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportPutAccountAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(PutAccountAction.NAME, transportService, actionFilters, PutAccountRequest::new);

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, PutAccountRequest request, ActionListener<PutAccountResponse> listener) {
        String scopedId = request.getAccountType() + "/" + request.getAccountId();

        try {

            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                listener.onResponse(new PutAccountResponse(scopedId, -1, Result.NOOP, RestStatus.UNAUTHORIZED, "Request did not contain user", null));
                return;
            }

            Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

            Account account = Account.parse(request.getAccountType(), request.getAccountId(), request.getBody().utf8ToString());

            try (StoredContext ctx = threadPool.getThreadContext().stashContext(); XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {

                threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                threadContext.putTransient(ConfigConstants.SG_USER, user);
                threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

                account.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

                client.prepareIndex(this.signals.getSignalsSettings().getStaticSettings().getIndexNames().getAccounts(), null, scopedId)
                        .setSource(xContentBuilder).setRefreshPolicy(RefreshPolicy.IMMEDIATE).execute(new ActionListener<IndexResponse>() {
                            @Override
                            public void onResponse(IndexResponse response) {
                                if (response.getResult() == Result.CREATED || response.getResult() == Result.UPDATED) {
                                    DestinationConfigUpdateAction.send(client);
                                }

                                listener.onResponse(
                                        new PutAccountResponse(scopedId, response.getVersion(), response.getResult(), response.status(), null, null));

                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
            }
        } catch (ConfigValidationException e) {
            listener.onResponse(
                    new PutAccountResponse(scopedId, -1, Result.NOOP, RestStatus.BAD_REQUEST, e.getMessage(), e.getValidationErrors().toJson()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}