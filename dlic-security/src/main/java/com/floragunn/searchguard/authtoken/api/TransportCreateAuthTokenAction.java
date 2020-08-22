package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.authtoken.AuthTokenService;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;

public class TransportCreateAuthTokenAction extends HandledTransportAction<CreateAuthTokenRequest, CreateAuthTokenResponse> {

    private final AuthTokenService authTokenService;
    private final ThreadPool threadPool;

    @Inject
    public TransportCreateAuthTokenAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            AuthTokenService authTokenService) {
        super(CreateAuthTokenAction.NAME, transportService, actionFilters, CreateAuthTokenRequest::new);

        this.authTokenService = authTokenService;
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

            String jwt = authTokenService.createJwt(user, request);

            listener.onResponse(new CreateAuthTokenResponse(jwt));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}