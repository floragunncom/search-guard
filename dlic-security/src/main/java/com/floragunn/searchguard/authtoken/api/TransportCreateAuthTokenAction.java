package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;

public class TransportCreateAuthTokenAction extends HandledTransportAction<CreateAuthTokenRequest, CreateAuthTokenResponse> {

    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportCreateAuthTokenAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters, Client client) {
        super(CreateAuthTokenAction.NAME, transportService, actionFilters, CreateAuthTokenRequest::new);

        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, CreateAuthTokenRequest request, ActionListener<CreateAuthTokenResponse> listener) {
        try {
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                listener.onFailure(new Exception("Request did not contain user"));
                return;
            }

            Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

            try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {

                threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                threadContext.putTransient(ConfigConstants.SG_USER, user);
                threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}