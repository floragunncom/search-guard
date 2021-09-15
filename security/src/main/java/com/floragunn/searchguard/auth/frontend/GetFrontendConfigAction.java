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

package com.floragunn.searchguard.auth.frontend;

import static org.opensearch.rest.RestRequest.Method.GET;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.auth.BackendRegistry;
import com.floragunn.searchguard.auth.frontend.ActivatedFrontendConfig.AuthMethod;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.sgconf.impl.v7.FrontendConfig;
import com.floragunn.searchsupport.client.rest.Responses;
import com.google.common.collect.ImmutableList;

public class GetFrontendConfigAction extends ActionType<GetFrontendConfigAction.Response> {
    protected final static Logger log = LogManager.getLogger(GetFrontendConfigAction.class);

    public static final GetFrontendConfigAction INSTANCE = new GetFrontendConfigAction();
    public static final String NAME = "cluster:admin:searchguard:auth/frontend/config/get";

    protected GetFrontendConfigAction() {
        super(NAME, in -> new GetFrontendConfigAction.Response(in));
    }

    public static class Request extends ActionRequest {
        private final String nextURL;
        private final String configId;
        private final String frontendBaseUrl;

        public Request(String configId, String nextURL, String frontendBaseUrl) {
            super();
            this.configId = configId;
            this.nextURL = nextURL;
            this.frontendBaseUrl = frontendBaseUrl;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.configId = in.readOptionalString();
            this.nextURL = in.readOptionalString();
            this.frontendBaseUrl = in.readOptionalString();
            @SuppressWarnings("unused")
            String reserved1 = in.readOptionalString();
            @SuppressWarnings("unused")
            String reserved2 = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(1);
            out.writeOptionalString(configId);
            out.writeOptionalString(nextURL);
            out.writeOptionalString(null);
            out.writeOptionalString(null);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getNextURL() {
            return nextURL;
        }

        public String getConfigId() {
            return configId;
        }
        
        public String getFrontendBaseUrl() {
            return frontendBaseUrl;
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private List<AuthMethod> authMethods;
        private String loginPageJson;

        public Response() {
        }

        public Response(List<AuthMethod> authMethods, FrontendConfig.LoginPage loginPage) {
            this.authMethods = authMethods;
            this.loginPageJson = loginPage.toJsonString();

        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.loginPageJson = in.readString();
            this.authMethods = in.readList(AuthMethod::new);
            @SuppressWarnings("unused")
            String reserved1 = in.readOptionalString();
            @SuppressWarnings("unused")
            String reserved2 = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(loginPageJson);
            out.writeList(authMethods);
            out.writeOptionalString(null);
            out.writeOptionalString(null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

            builder.startObject();

            builder.field("auth_methods", authMethods);

            builder.rawField("login_page", new ByteArrayInputStream(loginPageJson.getBytes("UTF-8")), XContentType.JSON);

            builder.endObject();

            return builder;
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private BackendRegistry backendRegistry;

        @Inject
        public TransportAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                BackendRegistry backendRegistry) {
            super(GetFrontendConfigAction.NAME, transportService, actionFilters, Request::new);

            this.backendRegistry = backendRegistry;

        }

        @Override
        protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
            try {
                String configId = request.getConfigId() != null ? request.getConfigId() : "default";

                FrontendConfig frontendConfig = backendRegistry.getFrontendConfig().getCEntry(configId);

                if (frontendConfig == null) {
                    listener.onFailure(new OpenSearchStatusException("No such frontend config: " + configId, RestStatus.NOT_FOUND));
                    return;
                }

                List<FrontendConfig.Authcz> authMethods = frontendConfig.getAuthcz();
                List<AuthMethod> result = new ArrayList<>(authMethods.size());

                for (FrontendConfig.Authcz authMethod : authMethods) {
                    if (!authMethod.isEnabled()) {
                        continue;
                    }
                    
                    // Normalize the type to replace deprecated type ids by their replacement (openid -> oidc)
                    String type = authMethod.getAuthenticationFrontend() != null ? authMethod.getAuthenticationFrontend().getType()
                            : authMethod.getType();

                    ActivatedFrontendConfig.AuthMethod activatedAuthMethod = new ActivatedFrontendConfig.AuthMethod(type, authMethod.getLabel(),
                            authMethod.getId(), true, authMethod.isUnavailable(), authMethod.getMessage());

                    if (authMethod.getAuthenticationFrontend() instanceof ApiAuthenticationFrontend) {
                        try {
                            activatedAuthMethod = ((ApiAuthenticationFrontend) authMethod.getAuthenticationFrontend())
                                    .activateFrontendConfig(activatedAuthMethod, request);
                        } catch (Exception e) {
                            log.error("Error while activating " + authMethod, e);
                            activatedAuthMethod = activatedAuthMethod.unavailable(
                                    "This auth method is temporarily unavailable. Please try again later or contact your administrator.");
                        }
                    }

                    result.add(activatedAuthMethod);
                }

                listener.onResponse(new Response(result, frontendConfig.getLoginPage()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    public static class RestAction extends BaseRestHandler {

        public RestAction() {
            super();
        }

        @Override
        public List<Route> routes() {
            return ImmutableList.of(new Route(GET, "/_searchguard/auth/config"));

        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

            if (request.method() == GET) {
                return handleGet(client, request.param("config_id"), request.param("next_url"), request.param("frontend_base_url"));
            } else {
                return (RestChannel channel) -> Responses.sendError(channel, RestStatus.METHOD_NOT_ALLOWED,
                        "Method not allowed: " + request.method());
            }
        }

        private RestChannelConsumer handleGet(NodeClient client, String configId, String nextUrl, String frontendBaseUrl) {
            return (RestChannel channel) -> {

                try {
                    client.execute(GetFrontendConfigAction.INSTANCE, new Request(configId, nextUrl, frontendBaseUrl),
                            new RestStatusToXContentListener<Response>(channel));
                } catch (Exception e) {
                    Responses.sendError(channel, e);
                }
            };
        }

        @Override
        public String getName() {
            return "Search Guard Frontend Auth Config";
        }
    }

}
