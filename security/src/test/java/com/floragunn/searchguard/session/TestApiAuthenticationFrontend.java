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

package com.floragunn.searchguard.session;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.auth.AuthenticationFrontend;
import com.floragunn.searchguard.auth.AuthenticatorUnavailableException;
import com.floragunn.searchguard.auth.CredentialsException;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.user.AuthCredentials;

public class TestApiAuthenticationFrontend implements ApiAuthenticationFrontend {

    public TestApiAuthenticationFrontend(Map<String, Object> config, AuthenticationFrontend.Context context) throws ConfigValidationException {

    }

    @Override
    public String getType() {
        return "test";
    }

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request)
            throws CredentialsException, ConfigValidationException, AuthenticatorUnavailableException {
        if (!"indeed".equals(request.get("secret"))) {
            return null;
        }

        if (request.get("user") == null) {
            throw new ConfigValidationException(new MissingAttribute("user"));
        }

        String user = request.get("user").toString();

        Collection<String> roles;

        if (request.get("roles") instanceof Collection) {
            roles = ((Collection<?>) request.get("roles")).stream().map((e) -> e.toString()).collect(Collectors.toList());
        } else if (request.get("roles") != null) {
            roles = Arrays.asList(request.get("roles").toString());
        } else {
            roles = Collections.emptyList();
        }

        return AuthCredentials.forUser(user).backendRoles(roles).build();
    }

}
