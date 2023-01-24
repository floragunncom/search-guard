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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.ComponentState;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BasicAuthenticationFrontend implements HttpAuthenticationFrontend, ApiAuthenticationFrontend {
    public static String TYPE = "basic";
    private static final Logger log = LogManager.getLogger(BasicAuthenticationFrontend.class);
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", TYPE).initialized();
    private final boolean challenge;

    public BasicAuthenticationFrontend(DocNode docNode, ConfigurationRepository.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        this.challenge = vNode.get("challenge").withDefault(true).asBoolean();

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public AuthCredentials extractCredentials(RequestMetaData<?> request) {
        AuthCredentials.Builder credsBuilder = decodeBasicCredentials(request.getAuthorizationByScheme("basic"));

        if (credsBuilder != null) {
            return credsBuilder.authenticatorType(getType()).build();
        } else {
            return null;
        }
    }

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request) throws ConfigValidationException {
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

    private AuthCredentials.Builder decodeBasicCredentials(String basicAuthorization) {
        if (basicAuthorization == null) {
            return null;
        }

        String decodedBasicHeader = new String(Base64.getDecoder().decode(basicAuthorization), StandardCharsets.UTF_8);

        int firstColonIndex = decodedBasicHeader.indexOf(':');
        if (firstColonIndex == -1) {
            log.debug("Invalid 'Authorization' header; no colon found.");
            return null;
        }

        String username = decodedBasicHeader.substring(0, firstColonIndex);
        String password = decodedBasicHeader.length() - 1 != firstColonIndex ? decodedBasicHeader.substring(firstColonIndex + 1) : "";

        return AuthCredentials.forUser(username).password(password.getBytes(StandardCharsets.UTF_8)).complete();
    }
}
