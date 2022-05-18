/*
 * Copyright 2022 floragunn GmbH
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
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.ComponentState;

public class HttpTrustedOriginAuthenticationFrontend implements HttpAuthenticationFrontend {

    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "trusted_origin").initialized();

    public HttpTrustedOriginAuthenticationFrontend(DocNode docNode, ConfigurationRepository.Context context) {
    }

    public AuthCredentials extractCredentials(RequestMetaData<?> request) throws CredentialsException {
        if (request.isTrustedProxy()) {
            return AuthCredentials.forUser("n/a").authenticatorType(getType()).complete().build();
        } else {
            throw new CredentialsException(new AuthcResult.DebugInfo("trusted_origin", false, "Connecting IP is not trusted",
                    ImmutableMap.of("direct_ip_address", String.valueOf(request.getDirectIpAddress()))));
        }
    }

    @Override
    public String getType() {
        return "trusted_origin";
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}