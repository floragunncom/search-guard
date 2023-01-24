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

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authc.AuthenticationFrontend;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import java.net.URI;
import java.util.Map;

public interface ApiAuthenticationFrontend extends AuthenticationFrontend {

    AuthCredentials extractCredentials(Map<String, Object> request)
            throws CredentialsException, ConfigValidationException, AuthenticatorUnavailableException;

    default ActivatedFrontendConfig.AuthMethod activateFrontendConfig(ActivatedFrontendConfig.AuthMethod frontendConfig,
            GetActivatedFrontendConfigAction.Request request) throws AuthenticatorUnavailableException {
        return frontendConfig;
    }

    default String getLogoutUrl(User user) throws AuthenticatorUnavailableException {
        return null;
    }

    public static class ActivateFrontendConfigRequest {
        private final URI nextURI;

        private ActivateFrontendConfigRequest(URI nextURI) {
            this.nextURI = nextURI;
        }

        public URI getNextURI() {
            return nextURI;
        }
    }
}
