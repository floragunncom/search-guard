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
import java.nio.file.Path;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.support.HTTPHelper;
import com.floragunn.searchguard.user.AuthCredentials;

public class BasicAuthenticator implements HTTPAuthenticator, ApiAuthenticationFrontend {

    protected final Logger log = LogManager.getLogger(this.getClass());

    public BasicAuthenticator(final Settings settings, final Path configPath) {
    
    }

    @Override
    public AuthCredentials extractCredentials(final RestRequest request, ThreadContext threadContext) {

        final boolean forceLogin = request.paramAsBoolean("force_login", false);
        
        if(forceLogin) {
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
    public boolean reRequestAuthentication(final RestChannel channel, AuthCredentials creds) {
        final BytesRestResponse wwwAuthenticateResponse = new BytesRestResponse(RestStatus.UNAUTHORIZED, "Unauthorized");
        wwwAuthenticateResponse.addHeader("WWW-Authenticate", "Basic realm=\"Search Guard\"");
        channel.sendResponse(wwwAuthenticateResponse);
        return true;
    }

    @Override
    public String getType() {
        return "basic";
    }

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request) throws ElasticsearchSecurityException, ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        
        if (request.get("user") == null) {
            validationErrors.add(new MissingAttribute("user", (JsonNode) null));
        }
        
        if (request.get("password") == null) {
            validationErrors.add(new MissingAttribute("password", (JsonNode) null));
        }
        
        validationErrors.throwExceptionForPresentErrors();
        
        return AuthCredentials.forUser(String.valueOf(request.get("user"))).password(String.valueOf(request.get("password")).getBytes(StandardCharsets.UTF_8)).complete().build();
    }

    @Override
    public String getChallenge(AuthCredentials credentials) {
        return "Basic realm=\"Search Guard\"";
    }
}
