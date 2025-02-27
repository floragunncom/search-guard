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

package com.floragunn.searchguard.authc.session;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.injection.guice.Inject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig.AuthMethod;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.google.common.collect.ImmutableMap;

public class GetActivatedFrontendConfigAction extends Action<GetActivatedFrontendConfigAction.Request, GetActivatedFrontendConfigAction.Response> {
    public static final GetActivatedFrontendConfigAction INSTANCE = new GetActivatedFrontendConfigAction();
    public static final String NAME = "cluster:admin:searchguard:auth/frontend/config/get";

    public static final RestApi REST_API = new RestApi()//
            .handlesGet("/_searchguard/auth/config")
            .with(GetActivatedFrontendConfigAction.INSTANCE,
                    (params, body) -> new Request(params.get("config_id"), params.get("next_url"), params.get("frontend_base_url")))//
            .handlesPost("/_searchguard/auth/config")//
            .with(GetActivatedFrontendConfigAction.INSTANCE)//
            .name("Search Guard Frontend Auth Config");

    protected final static Logger log = LogManager.getLogger(GetActivatedFrontendConfigAction.class);

    protected GetActivatedFrontendConfigAction() {
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
        private FrontendAuthcConfig.LoginPage loginPage;

        public Response() {
        }

        public Response(List<AuthMethod> authMethods, FrontendAuthcConfig.LoginPage loginPage) {
            this.authMethods = authMethods;
            this.loginPage = loginPage;

        }

        public Response(UnparsedMessage message) throws ConfigValidationException {
            super(message);
            DocNode docNode = message.requiredDocNode();
            this.loginPage = FrontendAuthcConfig.LoginPage.parse(docNode.getAsNode("login_page"), null);
            this.authMethods = docNode.getAsListFromNodes("auth_methods", AuthMethod::new);
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("auth_methods", authMethods, "login_page", loginPage != null ? loginPage.toBasicObject() : null);
        }

    }

    public static class Handler extends Action.Handler<Request, Response> {

        private ConfigurationRepository configRepository;

        @Inject
        public Handler(HandlerDependencies handlerDependencies, ConfigurationRepository configRepository) {
            super(GetActivatedFrontendConfigAction.INSTANCE, handlerDependencies);
            this.configRepository = configRepository;
        }

        @Override
        protected CompletableFuture<Response> doExecute(Request request) {
            String configId = request.getConfigId() != null ? request.getConfigId() : "default";

            FrontendAuthcConfig frontendConfig = configRepository.getConfiguration(CType.FRONTEND_AUTHC).getCEntry(configId);

            if (frontendConfig == null) {
                throw notFound("No such frontend config: " + configId);
            }

            List<FrontendAuthcConfig.FrontendAuthenticationDomain> authMethods = frontendConfig.getAuthDomains();
            List<AuthMethod> result = new ArrayList<>(authMethods.size());

            for (FrontendAuthcConfig.FrontendAuthenticationDomain authMethod : authMethods) {
                if (!authMethod.isEnabled()) {
                    continue;
                }

                // Normalize the type to replace deprecated type ids by their replacement (openid -> oidc)
                String type = authMethod.getAuthenticationFrontend() != null ? authMethod.getAuthenticationFrontend().getType()
                        : authMethod.getType();

                ActivatedFrontendConfig.AuthMethod activatedAuthMethod = new ActivatedFrontendConfig.AuthMethod(type, authMethod.getLabel(),
                        authMethod.getId(), true, authMethod.isUnavailable(), authMethod.isCaptureUrlFragment(), authMethod.isAutoSelect(), null, authMethod.getMessage());

                if (authMethod.getAuthenticationFrontend() instanceof ApiAuthenticationFrontend) {
                    try {
                        activatedAuthMethod = ((ApiAuthenticationFrontend) authMethod.getAuthenticationFrontend())
                                .activateFrontendConfig(activatedAuthMethod, request);
                    } catch (AuthenticatorUnavailableException e) {
                        log.error("Error while activating " + authMethod + "\n" + e.getDetails(), e);
                        String messageTitle = "Temporarily Unavailable";
                        String messageBody = "Please try again later or contact your administrator.";
                        Map<String, Object> details = null;

                        if (frontendConfig.isDebug()) {
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

                        if (frontendConfig.isDebug()) {
                            messageTitle = "Unexpected error while " + type + " login";
                            messageBody = e.toString();
                            StringWriter stringWriter = new StringWriter();
                            e.printStackTrace(new PrintWriter(stringWriter));                            
                            details = ImmutableMap.of("exception", ImmutableList.ofArray(stringWriter.toString().split("\n")));
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
