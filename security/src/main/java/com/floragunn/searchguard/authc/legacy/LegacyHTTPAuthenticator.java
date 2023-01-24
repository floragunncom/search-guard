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
package com.floragunn.searchguard.authc.legacy;

import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.user.AuthCredentials;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

public interface LegacyHTTPAuthenticator extends HttpAuthenticationFrontend {
    AuthCredentials extractCredentials(RestRequest request, ThreadContext threadContext);

    default AuthCredentials extractCredentials(RequestMetaData<?> request) throws AuthenticatorUnavailableException, CredentialsException {
        LegacyRestRequestMetaData restRequestMetaData = (LegacyRestRequestMetaData) request;
        return extractCredentials(restRequestMetaData.getRequest(), restRequestMetaData.getThreadContext());
    }

    default boolean reRequestAuthentication(RestChannel channel, AuthCredentials credentials) {
        return false;
    }

    default boolean handleMetaRequest(RestRequest restRequest, RestChannel restChannel, String generalRequestPathComponent,
            String specificRequestPathComponent, ThreadContext threadContext) {
        return false;
    }
}
