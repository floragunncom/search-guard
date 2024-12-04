package com.floragunn.signals.actions.account.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.signals.Signals;
import com.floragunn.signals.accounts.Account;
import com.floragunn.signals.accounts.NoSuchAccountException;
import com.floragunn.signals.actions.account.config_update.DestinationConfigUpdateAction;
import com.floragunn.signals.actions.account.delete.DeleteAccountResponse.Result;

import java.util.List;
import java.util.Map;

public class TransportDeleteAccountAction extends HandledTransportAction<DeleteAccountRequest, DeleteAccountResponse> {

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportDeleteAccountAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(DeleteAccountAction.NAME, transportService, actionFilters, DeleteAccountRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, DeleteAccountRequest request, ActionListener<DeleteAccountResponse> listener) {
        try {
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                listener.onFailure(new Exception("Request did not contain user"));
                return;
            }

            Account account = signals.getAccountRegistry().lookupAccount(request.getAccountId(), request.getAccountType());

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

                account.isInUse(client, signals.getSignalsSettings(), new ActionListener<Boolean>() {

                    @Override
                    public void onResponse(Boolean response) {
                        if (response.booleanValue()) {
                            listener.onResponse(new DeleteAccountResponse(account.getScopedId(), -1, Result.IN_USE, RestStatus.CONFLICT,
                                    "The account is still in use"));
                        } else {
                            client.prepareDelete().setIndex(signals.getSignalsSettings().getStaticSettings().getIndexNames().getAccounts())
                                    .setId(account.getScopedId()).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                                    .execute(new ActionListener<DeleteResponse>() {
                                        @Override
                                        public void onResponse(DeleteResponse response) {
                                            if (response.getResult() == DocWriteResponse.Result.DELETED) {
                                                DestinationConfigUpdateAction.send(client);
                                                listener.onResponse(new DeleteAccountResponse(account.getScopedId(), response.getVersion(),
                                                        Result.DELETED, response.status(), null));
                                            } else {
                                                listener.onResponse(new DeleteAccountResponse(account.getScopedId(), response.getVersion(),
                                                        Result.NOT_FOUND, response.status(), null));
                                            }
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            listener.onFailure(e);
                                        }
                                    });
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                });

            }
        } catch (NoSuchAccountException e) {
            listener.onResponse(new DeleteAccountResponse(request.getAccountId(), -1, Result.NOT_FOUND, RestStatus.NOT_FOUND, e.getMessage()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}