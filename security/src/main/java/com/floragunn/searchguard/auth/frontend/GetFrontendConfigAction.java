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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.auth.AuthenticatorUnavailableException;
import com.floragunn.searchguard.auth.BackendRegistry;
import com.floragunn.searchguard.auth.frontend.ActivatedFrontendConfig.AuthMethod;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.sgconf.impl.v7.FrontendConfig;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.google.common.collect.ImmutableMap;

public class GetFrontendConfigAction extends Action<GetFrontendConfigAction.Request, GetFrontendConfigAction.Response> {
    public static final GetFrontendConfigAction INSTANCE = new GetFrontendConfigAction();
    public static final String NAME = "cluster:admin:searchguard:auth/frontend/config/get";

    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/auth/config")
            .with(GetFrontendConfigAction.INSTANCE,
                    (params, body) -> new Request(params.get("config_id"), params.get("next_url"), params.get("frontend_base_url")))//
            .handlesPost("/_searchguard/auth/config")//
            .with(GetFrontendConfigAction.INSTANCE)//
            .name("Search Guard Frontend Auth Config");

    protected final static Logger log = LogManager.getLogger(GetFrontendConfigAction.class);

    protected GetFrontendConfigAction() {
        super(NAME, Request::new, Response::new);
    }

    public static class Request extends Action.Request {
        private final String nextURL;
        private final String configId;
        private final String frontendBaseUrl;

        public Request(String configId, String nextURL, String frontendBaseUrl) {
            super();
            this.configId = configId;
            this.nextURL = nextURL;
            this.frontendBaseUrl = frontendBaseUrl;
        }

        public Request(UnparsedMessage message) throws ConfigValidationException {
            DocNode docNode = message.requiredDocNode();
            this.configId = docNode.getAsString("config_id");
            this.nextURL = docNode.getAsString("next_url");
            this.frontendBaseUrl = docNode.getAsString("frontend_base_url");
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("config_id", configId, "next_url", nextURL, "frontend_base_url", frontendBaseUrl);
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

    public static class Response extends Action.Response {

        private List<AuthMethod> authMethods;
        private FrontendConfig.LoginPage loginPage;

        public Response() {
        }

        public Response(List<AuthMethod> authMethods, FrontendConfig.LoginPage loginPage) {
            this.authMethods = authMethods;
            this.loginPage = loginPage;

        }

        public Response(UnparsedMessage message) throws ConfigValidationException {
            super(message);
            DocNode docNode = message.requiredDocNode();
            this.loginPage = FrontendConfig.LoginPage.parse(docNode.getAsNode("login_page"));
            this.authMethods = docNode.getAsListFromNodes("auth_methods", AuthMethod::new);
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("auth_methods", authMethods, "login_page", loginPage != null ? loginPage.toBasicObject() : null);
        }

    }

    public static class Handler extends Action.Handler<Request, Response> {

        private BackendRegistry backendRegistry;

        @Inject
        public Handler(HandlerDependencies handlerDependencies, BackendRegistry backendRegistry) {
            super(GetFrontendConfigAction.INSTANCE, handlerDependencies);
            this.backendRegistry = backendRegistry;
        }

        @Override
        protected CompletableFuture<Response> doExecute(Request request) {
            String configId = request.getConfigId() != null ? request.getConfigId() : "default";

            FrontendConfig frontendConfig = backendRegistry.getFrontendConfig().getCEntry(configId);

            if (frontendConfig == null) {
                throw notFound("No such frontend config: " + configId);
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
                        authMethod.getId(), true, authMethod.isUnavailable(), authMethod.isCaptureUrlFragment(), null, authMethod.getMessage());

                if (authMethod.getAuthenticationFrontend() instanceof ApiAuthenticationFrontend) {
                    try {
                        activatedAuthMethod = ((ApiAuthenticationFrontend) authMethod.getAuthenticationFrontend())
                                .activateFrontendConfig(activatedAuthMethod, request);
                    } catch (AuthenticatorUnavailableException e) {
                        log.error("Error while activating " + authMethod + "\n" + e.getDetails(), e);
                        String messageTitle = "Temporarily Unavailable";
                        String messageBody = "Please try again later or contact your administrator.";
                        Map<String, Object> details = null;

                        if (backendRegistry.isDebugEnabled()) {
                            messageTitle = e.getMessageTitle();
                            messageBody = e.getMessageBody();
                            details = e.getDetails();
                        }

                        activatedAuthMethod = activatedAuthMethod.unavailable(messageTitle, messageBody, details);

                    } catch (Exception e) {
                        log.error("Error while activating " + authMethod, e);
                        Map<String, Object> details = null;
                        String messageTitle = "Temporarily Unavailable";
                        String messageBody = "Please try again later or contact your administrator.";

                        if (backendRegistry.isDebugEnabled()) {
                            messageTitle = "Unexpected error while " + type + " login";
                            messageBody = e.toString();
                        }

                        activatedAuthMethod = activatedAuthMethod.unavailable(messageTitle, messageBody, details);
                    }
                }

                result.add(activatedAuthMethod);
            }

            return CompletableFuture.completedFuture(new Response(result, frontendConfig.getLoginPage()));
        }
    }

}
