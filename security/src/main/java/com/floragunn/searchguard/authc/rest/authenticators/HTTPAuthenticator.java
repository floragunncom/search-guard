/*
 * Copyright 2015-2021 floragunn GmbH
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

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.searchguard.authc.AuthenticationFrontend;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.user.AuthCredentials;

/**
 * Search Guard custom HTTP authenticators need to implement this interface.
 * <p/>
 * A HTTP authenticator extracts {@link AuthCredentials} from a {@link RestRequest}
 * <p/>
 * 
 * Implementation classes must provide a public constructor
 * <p/>
 * {@code public MyHTTPAuthenticator(org.elasticsearch.common.settings.Settings settings, java.nio.file.Path configPath)}
 * <p/>
 * The constructor should not throw any exception in case of an initialization problem.
 * Instead catch all exceptions and log a appropriate error message. A logger can be instantiated like:
 * <p/>
 * {@code private final Logger log = LogManager.getLogger(this.getClass());}
 * <p/>
 * <b>Custom authenticators is a commercial feature. To make them work you need to obtain a license here:
 * https://floragunn.com
 * </b>
 */
public interface HTTPAuthenticator extends AuthenticationFrontend {
    
    /**
     * Extract {@link AuthCredentials} from {@link RestRequest}
     * 
     * @param request The rest request
     * @param context The current thread context
     * @return The authentication credentials (complete or incomplete) or null when no credentials are found in the request
     * <p>
     * When the credentials could be fully extracted from the request {@code .markComplete()} must be called on the {@link AuthCredentials} which are returned.
     * If the authentication flow needs another roundtrip with the request originator do not mark it as complete.
     * @throws AuthenticatorUnavailableException
     * @throws CredentialsException
     */
    AuthCredentials extractCredentials(RestRequest request, ThreadContext context) throws AuthenticatorUnavailableException, CredentialsException;
    
    default String getChallenge(AuthCredentials credentials) {
        return null;
    }
    
    default boolean handleMetaRequest(RestRequest restRequest, RestChannel restChannel, String generalRequestPathComponent, String specificRequestPathComponent, ThreadContext threadContext) {
        return false;
    }
}
