/*
 * Copyright 2021-2022 floragunn GmbH
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.Action.UnparsedMessage;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.cstate.metrics.Meter;

public class SessionApi {

    public static class GetExtendedInfoAction extends Action<EmptyRequest, GetExtendedInfoAction.Response> {

        public static final GetExtendedInfoAction INSTANCE = new GetExtendedInfoAction();
        public static final String NAME = "cluster:admin:searchguard:session/_own/get/extended";

        protected GetExtendedInfoAction() {
            super(NAME, EmptyRequest::new, Response::new);
        }

        public static class Response extends Action.Response {

            private String userName;
            private String userSubName;
            private String userType;
            private List<String> userRoles;
            private List<String> userSearchGuardRoles;
            private Map<String, Object> userAttributes;
            private String userRequestedTenant;
            private String ssoLogoutUrl;

            public Response(User user, String ssoLogoutUrl) {
                this.userName = user.getName();
                this.userSubName = user.getSubName();
                this.userType = user.getType();
                this.userRoles = ImmutableList.of(user.getRoles());
                this.userSearchGuardRoles = ImmutableList.of(user.getSearchGuardRoles());
                this.userAttributes = user.getStructuredAttributes();
                this.userRequestedTenant = user.getRequestedTenant();

                this.ssoLogoutUrl = ssoLogoutUrl;
            }

            public Response(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                DocNode docNode = message.requiredDocNode();
                DocNode userNode = docNode.getAsNode("user");

                this.userName = userNode.getAsString("name");
                this.userSubName = userNode.getAsString("sub_name");
                this.userType = userNode.getAsString("type");
                this.userRoles = userNode.getAsListOfStrings("backend_roles");
                this.userSearchGuardRoles = userNode.getAsListOfStrings("search_guard_roles");
                this.userAttributes = userNode.getAsNode("attributes").toMap();
                this.userRequestedTenant = userNode.getAsString("requested_tenant");

                this.ssoLogoutUrl = docNode.getAsString("sso_logout_url");
            }

            @Override
            public Object toBasicObject() {
                return OrderedImmutableMap.of("user",
                        OrderedImmutableMap.of("name", userName, "sub_name", userSubName, "type", userType, "backend_roles", userRoles,
                                "search_guard_roles", userSearchGuardRoles).with("attributes", userAttributes)
                                .with("requested_tenant", userRequestedTenant),
                        "sso_logout_url", this.ssoLogoutUrl);
            }

        }

        public static class Handler extends Action.Handler<EmptyRequest, Response> {
            private final SessionService sessionService;
            private final AuthInfoService authInfoService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, SessionService sessionService, AuthInfoService authInfoService) {
                super(GetExtendedInfoAction.INSTANCE, handlerDependencies);

                this.sessionService = sessionService;
                this.authInfoService = authInfoService;
            }

            @Override
            protected final CompletableFuture<Response> doExecute(EmptyRequest request) {
                User user = authInfoService.getCurrentUser();

                if (user == null) {
                    CompletableFuture<Response> result = new CompletableFuture<Response>();
                    result.completeExceptionally(new Exception("No user present"));
                    return result;
                }

                return CompletableFuture.completedFuture(new Response(user, sessionService.getSsoLogoutUrl(user)));
            }
        }
    }

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

                        SessionToken authToken = sessionService.getByIdFromIndex(authTokenId, Meter.NO_OP);

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

    /**
     * Note: This is only used for /_searchguard/auth/session/with_header endpoint. In most cases, sessions will be started with /_searchguard/auth/session, which
     * uses no transport action.
     */
    public static class CreateAction extends Action<EmptyRequest, StartSessionResponse> {

        public static final CreateAction INSTANCE = new CreateAction();
        public static final String NAME = "cluster:admin:searchguard:session/create";

        protected CreateAction() {
            super(NAME, EmptyRequest::new, StartSessionResponse::new);
        }

        public static class Handler extends Action.Handler<EmptyRequest, StartSessionResponse> {
            private static final Logger log = LogManager.getLogger(Handler.class);

            private final SessionService sessionService;
            private final AuthInfoService authInfoService;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, SessionService sessionService, AuthInfoService authInfoService,
                    PrivilegesEvaluator privilegesEvaluator) {
                super(CreateAction.INSTANCE, handlerDependencies);

                this.sessionService = sessionService;
                this.authInfoService = authInfoService;
            }

            @Override
            protected final CompletableFuture<StartSessionResponse> doExecute(EmptyRequest request) {
                User user = authInfoService.getCurrentUser();

                if (user == null) {
                    log.error("Cannot create session: No user found in thread context");
                    CompletableFuture<StartSessionResponse> result = new CompletableFuture<>();
                    result.completeExceptionally(new Exception("Invalid authentication"));
                    return result;
                }

                return sessionService.createSession(user);
            }
        }
    }

    public static class StartSessionResponse extends Action.Response {
        private String token;
        private String redirectUri;

        public StartSessionResponse(String token, String redirectUri) {
            this.token = token;
            this.redirectUri = redirectUri;
        }

        public StartSessionResponse(UnparsedMessage message) throws ConfigValidationException {
            super(message);
            DocNode docNode = message.requiredDocNode();

            this.token = docNode.getAsString("token");
            this.redirectUri = docNode.getAsString("redirect_uri");
        }

        public String getToken() {
            return token;
        }

        public String getRedirectUri() {
            return redirectUri;
        }

        @Override
        public Object toBasicObject() {
            return OrderedImmutableMap.of("token", token, "redirect_uri", redirectUri);
        }
    }

    public static class Rest extends RestApi {
        private static final Logger log = LogManager.getLogger(Rest.class);
        private SessionService sessionService;

        public Rest() {
            handlesGet("/_searchguard/auth/session/extended").with(GetExtendedInfoAction.INSTANCE);
            handlesGet("/_searchguard/auth/session").with((r, c) -> handleGet(r, c));
            handlesPost("/_searchguard/auth/session/with_header").with(CreateAction.INSTANCE);
            handlesPost("/_searchguard/auth/session").with((r, c) -> handlePost(r, c));
            handlesDelete("/_searchguard/auth/session").with(DeleteAction.INSTANCE);
        }

        private RestChannelConsumer handleGet(RestRequest request, NodeClient client) {
            return (RestChannel channel) -> {
                try {
                    User user = client.threadPool().getThreadContext().getTransient(ConfigConstants.SG_USER);

                    if (user != null) {
                        channel.sendResponse(new RestResponse(RestStatus.OK, "application/json", DocNode.of("sso_logout_url", sessionService.getSsoLogoutUrl(user)).toJsonString()));
                    } else {
                        channel.sendResponse(new StandardResponse(404, new StandardResponse.Error("No session")).toRestResponse());
                    }
                } catch (Exception e) {
                    log.warn("Error while handling request", e);
                    channel.sendResponse(StandardResponse.internalServerError().toRestResponse());
                }
            };
        }

        private RestChannelConsumer handlePost(RestRequest request, NodeClient client) {
            ReleasableBytesReference body = request.requiredContent();
            XContentType xContentType = request.getXContentType();

            body.mustIncRef();

            return (RestChannel channel) -> {
                try {
                    Map<String, Object> requestBody = DocReader.format(Format.getByContentType(xContentType.mediaType()))
                            .readObject(BytesReference.toBytes(body));

                    sessionService.authenticateAndCreateSession(requestBody, request, (response) -> {
                        channel.sendResponse(new RestResponse(RestStatus.CREATED, "application/json", response.toJsonString()));
                    }, (authFailureAuthzResult) -> {
                        channel.sendResponse(new RestResponse(authFailureAuthzResult.getRestStatus(), "application/json",
                                authFailureAuthzResult.toJsonString()));
                    }, (e) -> {
                        log.error("Error while handling request", e);
                        channel.sendResponse(StandardResponse.internalServerError().toRestResponse());
                    });

                } catch (Exception e) {
                    log.warn("Error while handling request", e);
                    channel.sendResponse(StandardResponse.internalServerError().toRestResponse());
                } finally {
                    body.decRef();
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
