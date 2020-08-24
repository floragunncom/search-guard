package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.authtoken.AuthToken;
import com.floragunn.searchguard.authtoken.AuthTokenService;
import com.floragunn.searchguard.authtoken.NoSuchAuthTokenException;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;

public class TransportGetAuthTokenAction extends AbstractTransportAuthTokenAction<GetAuthTokenRequest, GetAuthTokenResponse> {

    private final AuthTokenService authTokenService;
    private final ThreadPool threadPool;

    @Inject
    public TransportGetAuthTokenAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            AuthTokenService authTokenService, PrivilegesEvaluator privilegesEvaluator) {
        super(GetAuthTokenAction.NAME, transportService, actionFilters, GetAuthTokenRequest::new, privilegesEvaluator);

        this.authTokenService = authTokenService;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, GetAuthTokenRequest request, ActionListener<GetAuthTokenResponse> listener) {

        ThreadContext threadContext = threadPool.getThreadContext();

        User user = threadContext.getTransient(ConfigConstants.SG_USER);

        if (user == null) {
            listener.onFailure(new Exception("Request did not contain user"));
            return;
        }

        threadPool.generic().submit(() -> {
            try {
                AuthToken authToken = authTokenService.getByIdFromIndex(request.getAuthTokenId());

                if (!isAllowedToAccessAll(user) && !user.getName().equals(authToken.getUserName())) {
                    throw new NoSuchAuthTokenException(request.getAuthTokenId());
                }

                listener.onResponse(new GetAuthTokenResponse(authToken));
            } catch (NoSuchAuthTokenException e) {
                listener.onResponse(new GetAuthTokenResponse(RestStatus.NOT_FOUND, "No such auth token: " + request.getAuthTokenId()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });

    }
}