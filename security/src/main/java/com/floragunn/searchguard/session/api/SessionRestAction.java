/*
 * Copyright 2021 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.session.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.session.SessionService;
import com.floragunn.searchsupport.client.rest.Responses;
import com.google.common.collect.ImmutableList;

public class SessionRestAction extends BaseRestHandler {
    private static final Logger log = LogManager.getLogger(SessionRestAction.class);
    private SessionService sessionService;

    public SessionRestAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(POST, "/_searchguard/auth/session"), new Route(GET, "/_searchguard/auth/session"),
                new Route(DELETE, "/_searchguard/auth/session"));

    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        if (request.method() == POST) {
            return handlePost(request, request.requiredContent(), request.getXContentType(), client);
        } else if (request.method() == GET) {
            return handleGet(request, client);
        } else if (request.method() == DELETE) {
            return handleDelete(request, client);
        } else {
            return (RestChannel channel) -> Responses.sendError(channel, RestStatus.METHOD_NOT_ALLOWED, "Method not allowed: " + request.method());
        }
    }

    private RestChannelConsumer handlePost(RestRequest request, BytesReference body, XContentType xContentType, NodeClient client) {

        return (RestChannel channel) -> {

            try {
                Map<String, Object> requestBody = DocReader.format(Format.getByContentType(xContentType.mediaType()))
                        .readObject(BytesReference.toBytes(body));

                sessionService.createSession(requestBody, request, (response) -> {
                    Responses.send(channel, RestStatus.CREATED, response);
                }, (authFailureAuthzResult) -> {
                    Responses.send(channel, authFailureAuthzResult.getRestStatus(), authFailureAuthzResult);
                }, (e) -> {
                    log.error("Error while handling request", e);
                    Responses.sendError(channel, e);
                });

            } catch (Exception e) {
                log.warn("Error while handling request", e);
                Responses.sendError(channel, e);
            }
        };
    }

    private RestChannelConsumer handleDelete(RestRequest request, NodeClient client) {
        return (RestChannel channel) -> {

            try {
                client.execute(DeleteSessionAction.INSTANCE, new DeleteSessionAction.Request(),
                        new RestToXContentListener<DeleteSessionAction.Response>(channel));
            } catch (Exception e) {
                Responses.sendError(channel, e);
            }
        };
    }

    private RestChannelConsumer handleGet(RestRequest request, NodeClient client) {
        return (RestChannel channel) -> {
            Responses.sendError(channel, RestStatus.BAD_REQUEST, "");
        };
    }

    @Override
    public String getName() {
        return "Search Guard Session";
    }

    public SessionService getSessionService() {
        return sessionService;
    }

    public void setSessionService(SessionService sessionService) {
        this.sessionService = sessionService;
    }
}
