/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.rest.authenticators;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.support.HTTPHelper;
import com.floragunn.searchguard.user.AuthCredentials;

public class BasicAuthenticator implements HTTPAuthenticator, ApiAuthenticationFrontend {
    private static final Logger log = LogManager.getLogger(BasicAuthenticator.class);
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "basic").initialized();
    private final boolean challenge;

    public BasicAuthenticator(DocNode docNode, ConfigurationRepository.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        this.challenge = vNode.get("challenge").withDefault(true).asBoolean();

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();
    }

    @Override
    public AuthCredentials extractCredentials(RestRequest request, ThreadContext threadContext) {

        final boolean forceLogin = request.paramAsBoolean("force_login", false);

        if (forceLogin) {
            return null;
        }

        final String authorizationHeader = request.header("Authorization");

        AuthCredentials.Builder credsBuilder = HTTPHelper.extractCredentials(authorizationHeader, log);

        if (credsBuilder != null) {
            return credsBuilder.authenticatorType(getType()).build();
        } else {
            return null;
        }
    }

    @Override
    public String getType() {
        return "basic";
    }

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request) throws ElasticsearchSecurityException, ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        if (request.get("user") == null) {
            validationErrors.add(new MissingAttribute("user", null));
        }

        if (request.get("password") == null) {
            validationErrors.add(new MissingAttribute("password", null));
        }

        validationErrors.throwExceptionForPresentErrors();

        return AuthCredentials.forUser(String.valueOf(request.get("user")))
                .password(String.valueOf(request.get("password")).getBytes(StandardCharsets.UTF_8)).complete().build();
    }

    @Override
    public String getChallenge(AuthCredentials credentials) {
        if (challenge) {
            return "Basic realm=\"Search Guard\"";
        } else {
            return null;
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
