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

package com.floragunn.searchguard.legacy.auth;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.legacy.LegacyHTTPAuthenticator;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.legacy.LegacyComponentFactory;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.ComponentState;

public class HTTPBasicAuthenticator implements LegacyHTTPAuthenticator, ApiAuthenticationFrontend {

    protected final static Logger log = LogManager.getLogger(HTTPBasicAuthenticator.class);
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "basic", HTTPBasicAuthenticator.class)
            .initialized();
    
    public HTTPBasicAuthenticator(final Settings settings, final Path configPath) {

    }

    @Override
    public AuthCredentials extractCredentials(final RestRequest request, ThreadContext threadContext) {

        // replaced by below code. Usage of code request.paramAsBoolean("force_login", false); consumes parameter and this
        // leads to an assertion error:
        // https://github.com/elastic/elasticsearch/commit/5a5b14dde62b1c23bb0ce121584162485ba1989b
        //        final boolean forceLogin = request.paramAsBoolean("force_login", false);
        final boolean forceLogin = Optional.ofNullable(request.params())
                .map(params -> params.get("force_login"))
                .map(Boolean::parseBoolean)
                .orElse(false);


        if (forceLogin) {
            return null;
        }

        final String authorizationHeader = request.header("Authorization");

        AuthCredentials.Builder credsBuilder = extractCredentials(authorizationHeader);

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
    public String getChallenge(AuthCredentials credentials) {
        return "Basic realm=\"Search Guard\"";
    }

    public static TypedComponent.Info<LegacyHTTPAuthenticator> INFO = new TypedComponent.Info<LegacyHTTPAuthenticator>() {

        @Override
        public Class<LegacyHTTPAuthenticator> getType() {
            return LegacyHTTPAuthenticator.class;
        }

        @Override
        public String getName() {
            return "basic";
        }

        @Override
        public Factory<LegacyHTTPAuthenticator> getFactory() {
            return LegacyComponentFactory.adapt(HTTPBasicAuthenticator::new);
        }
    };

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request)
            throws CredentialsException, ConfigValidationException, AuthenticatorUnavailableException {
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
    public ComponentState getComponentState() {
        return componentState;
    }
    
    private static AuthCredentials.Builder extractCredentials(String authorizationHeader) {

        if (authorizationHeader != null) {
            if (!authorizationHeader.trim().toLowerCase().startsWith("basic ")) {
                log.debug("No 'Basic Authorization' header, send 401 and 'WWW-Authenticate Basic'");
                return null;
            } else {

                final String decodedBasicHeader = new String(Base64.getDecoder().decode(authorizationHeader.split(" ")[1]),
                        StandardCharsets.UTF_8);

                //username:password
                //special case
                //username must not contain a :, but password is allowed to do so
                //   username:pass:word
                //blank password
                //   username:
                
                final int firstColonIndex = decodedBasicHeader.indexOf(':');

                String username = null;
                String password = null;

                if (firstColonIndex > 0) {
                    username = decodedBasicHeader.substring(0, firstColonIndex);
                    
                    if(decodedBasicHeader.length() - 1 != firstColonIndex) {
                        password = decodedBasicHeader.substring(firstColonIndex + 1);
                    } else {
                        //blank password
                        password="";
                    }
                }

                if (username == null || password == null) {
                    log.debug("Invalid 'Authorization' header, send 401 and 'WWW-Authenticate Basic'");
                    return null;
                } else {
                    return AuthCredentials.forUser(username).password(password.getBytes(StandardCharsets.UTF_8)).complete();
                }
            }
        } else {
            return null;
        }
    }
}
