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

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authtoken.AuthTokenService;
import com.floragunn.searchguard.authtoken.TokenCreationException;
import com.floragunn.searchguard.user.User;

public class TransportCreateAuthTokenAction extends HandledTransportAction<CreateAuthTokenRequest, CreateAuthTokenResponse> {

    private final AuthTokenService authTokenService;
    private final AuthInfoService authInfoService;
    private final ThreadPool threadPool;

    @Inject
    public TransportCreateAuthTokenAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            AuthTokenService authTokenService, AuthInfoService authInfoService) {
        super(CreateAuthTokenAction.NAME, transportService, actionFilters, CreateAuthTokenRequest::new);

        this.authTokenService = authTokenService;
        this.threadPool = threadPool;
        this.authInfoService = authInfoService;
    }

    @Override
    protected final void doExecute(Task task, CreateAuthTokenRequest request, ActionListener<CreateAuthTokenResponse> listener) {

        User user = authInfoService.getCurrentUser();

        threadPool.generic().submit(() -> {
            try {
                listener.onResponse(authTokenService.createJwt(user, request));
            } catch (TokenCreationException e) {
                listener.onFailure(new OpenSearchStatusException(e.getMessage(), e.getRestStatus(), e));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });

    }
}