/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

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

public class TransportRevokeAuthTokenAction extends AbstractTransportAuthTokenAction<RevokeAuthTokenRequest, RevokeAuthTokenResponse> {

    private final AuthTokenService authTokenService;
    private final ThreadPool threadPool;

    @Inject
    public TransportRevokeAuthTokenAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            AuthTokenService authTokenService, PrivilegesEvaluator privilegesEvaluator) {
        super(RevokeAuthTokenAction.NAME, transportService, actionFilters, RevokeAuthTokenRequest::new, privilegesEvaluator);

        this.authTokenService = authTokenService;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, RevokeAuthTokenRequest request, ActionListener<RevokeAuthTokenResponse> listener) {

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

                String status = authTokenService.revoke(user, request.getAuthTokenId());

                listener.onResponse(new RevokeAuthTokenResponse(status));
            } catch (NoSuchAuthTokenException e) {
                listener.onResponse(new RevokeAuthTokenResponse(RestStatus.NOT_FOUND, "No such auth token: " + request.getAuthTokenId()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });

    }
}