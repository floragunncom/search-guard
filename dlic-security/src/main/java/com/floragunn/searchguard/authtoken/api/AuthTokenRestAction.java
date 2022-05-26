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

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.rest.action.RestToXContentListener;

import com.floragunn.searchsupport.action.Responses;
import com.google.common.collect.ImmutableList;

public class AuthTokenRestAction extends BaseRestHandler {
    private static final Logger log = LogManager.getLogger(AuthTokenRestAction.class);

    public AuthTokenRestAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(POST, "/_searchguard/authtoken"), new Route(GET, "/_searchguard/authtoken/{id}"),
                new Route(DELETE, "/_searchguard/authtoken/{id}"));

    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        if (request.method() == POST) {
            return handlePost(request, client);
        } else if (request.method() == GET) {
            return handleGet(request.param("id"), client);
        } else if (request.method() == DELETE) {
            return handleDelete(request.param("id"), client);
        } else {
            return (RestChannel channel) -> Responses.sendError(channel, RestStatus.METHOD_NOT_ALLOWED, "Method not allowed: " + request.method());
        }
    }

    private RestChannelConsumer handlePost(RestRequest request, NodeClient client) {
        try {
            CreateAuthTokenRequest authTokenRequest = CreateAuthTokenRequest.parse(request.requiredContent(), request.getXContentType());

            return channel -> client.execute(CreateAuthTokenAction.INSTANCE, authTokenRequest,
                    new RestToXContentListener<CreateAuthTokenResponse>(channel));
        } catch (Exception e) {
            log.warn("Error while handling request", e);
            return channel -> Responses.sendError(channel, e);
        }
    }

    private RestChannelConsumer handleDelete(String id, NodeClient client) {
        try {
            return channel -> client.execute(RevokeAuthTokenAction.INSTANCE, new RevokeAuthTokenRequest(id),
                    new RestToXContentListener<RevokeAuthTokenResponse>(channel));
        } catch (Exception e) {
            return channel -> Responses.sendError(channel, e);
        }
    }

    private RestChannelConsumer handleGet(String id, NodeClient client) {

        try {
            return channel -> client.execute(GetAuthTokenAction.INSTANCE, new GetAuthTokenRequest(id),
                    new RestStatusToXContentListener<GetAuthTokenResponse>(channel));
        } catch (Exception e) {
            return channel -> Responses.sendError(channel, e);
        }
    }

    @Override
    public String getName() {
        return "Search Guard Auth Token";
    }
}
