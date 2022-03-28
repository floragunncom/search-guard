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

package com.floragunn.searchguard.authc.session.backend;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.rest.Responses;

public class SessionApi {

    public static class DeleteAction extends Action<EmptyRequest, StandardResponse> {

        public static final DeleteAction INSTANCE = new DeleteAction();
        public static final String NAME = "cluster:admin:searchguard:session/_own/delete";

        protected DeleteAction() {
            super(NAME, EmptyRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<EmptyRequest, StandardResponse> {
            private static final Logger log = LogManager.getLogger(Handler.class);

            private final SessionService sessionService;
            private final AuthInfoService authInfoService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, SessionService sessionService, AuthInfoService authInfoService,
                    PrivilegesEvaluator privilegesEvaluator) {
                super(DeleteAction.INSTANCE, handlerDependencies);

                this.sessionService = sessionService;
                this.authInfoService = authInfoService;
            }

            @Override
            protected final CompletableFuture<StandardResponse> doExecute(EmptyRequest request) {
                User user = authInfoService.getCurrentUser();

                return supplyAsync(() -> {
                    String authTokenId = null; // request.getAuthTokenId();

                    try {

                        if (authTokenId == null && SessionService.USER_TYPE.equals(user.getType())) {
                            authTokenId = String.valueOf(user.getSpecialAuthzConfig());
                        }

                        if (authTokenId == null) {
                            return new StandardResponse(400, new StandardResponse.Error("User has no active session"));
                        }

                        SessionToken authToken = sessionService.getByIdFromIndex(authTokenId);

                        if (!user.getName().equals(authToken.getUserName())) {
                            throw new NoSuchSessionException(authTokenId);
                        }

                        String status = sessionService.delete(user, authToken);

                        return new StandardResponse(200, status);
                    } catch (NoSuchSessionException e) {
                        return new StandardResponse(404, new StandardResponse.Error("No such auth token: " + authTokenId));
                    } catch (SessionUpdateException e) {
                        log.error("Error while updating " + authTokenId, e);
                        return new StandardResponse(500, new StandardResponse.Error(e.getMessage()));
                    }
                });

            }
        }

    }

    public static class Rest extends RestApi {
        private static final Logger log = LogManager.getLogger(Rest.class);
        private SessionService sessionService;

        public Rest() {
            handlesGet("/_searchguard/auth/session").with((r, c) -> handleGet(r, c));
            handlesPost("/_searchguard/auth/session").with((r, c) -> handlePost(r, c));
            handlesDelete("/_searchguard/auth/session").with(DeleteAction.INSTANCE);
        }

        private RestChannelConsumer handleGet(RestRequest request, NodeClient client) {
            return (RestChannel channel) -> {               
                try {
                    User user = client.threadPool().getThreadContext().getTransient(ConfigConstants.SG_USER);
                    
                    if (user != null) {
                        Responses.send(channel, RestStatus.OK, DocNode.of("sso_logout_url", sessionService.getSsoLogoutUrl(user)));
                    } else {
                        Responses.sendError(channel, RestStatus.NOT_FOUND, "No session");
                    }                   
                } catch (Exception e) {
                    log.warn("Error while handling request", e);
                    Responses.sendError(channel, e);
                }
            };
        }
        
        private RestChannelConsumer handlePost(RestRequest request, NodeClient client) {

            BytesReference body = request.requiredContent();
            XContentType xContentType = request.getXContentType();

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

        @Override
        public String getName() {
            return "/_searchguard/auth/session";
        }

        public SessionService getSessionService() {
            return sessionService;
        }

        public void setSessionService(SessionService sessionService) {
            this.sessionService = sessionService;
        }
    }
}
