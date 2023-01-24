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

import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.ComponentState;

public class AnonymousAuthenticationFrontend implements HttpAuthenticationFrontend {
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "anonymous").initialized();

    @Override
    public String getType() {
        return "anonymous";
    }

    @Override
    public AuthCredentials extractCredentials(RequestMetaData<?> request) throws AuthenticatorUnavailableException, CredentialsException {
        return AuthCredentials.forUser("anonymous").build();
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
