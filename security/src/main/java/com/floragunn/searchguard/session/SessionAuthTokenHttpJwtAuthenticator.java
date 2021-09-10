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

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtConstants;
import org.apache.cxf.rs.security.jose.jwt.JwtException;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.SpecialPermission;
import org.opensearch.common.Strings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;

import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.user.AuthCredentials;

public class SessionAuthTokenHttpJwtAuthenticator implements HTTPAuthenticator {
    private final static Logger log = LogManager.getLogger(SessionAuthTokenHttpJwtAuthenticator.class);

    private final SessionService sessionService;
    private final String jwtHeaderName;
    private final String subjectKey;

    public SessionAuthTokenHttpJwtAuthenticator(SessionService sessionService) {
        this.sessionService = sessionService;
        this.jwtHeaderName = "Authorization";
        this.subjectKey = JwtConstants.CLAIM_SUBJECT;
    }

    @Override
    public String getType() {
        return "session";
    }

    @Override
    public AuthCredentials extractCredentials(RestRequest request, ThreadContext context) throws OpenSearchSecurityException {

        // TODO check whether this is really necessary
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<AuthCredentials>) () -> extractCredentials0(request));
    }

    private AuthCredentials extractCredentials0(RestRequest request) throws OpenSearchSecurityException {

        String encodedJwt = getJwtTokenString(request);

        if (Strings.isNullOrEmpty(encodedJwt)) {
            return null;
        }

        try {
            JwtToken jwt = sessionService.getVerifiedJwtToken(encodedJwt);
            JwtClaims claims = jwt.getClaims();

            String subject = extractSubject(claims);

            if (subject == null) {
                log.error("No subject found in JWT token: " + claims);
                return null;
            }

            return AuthCredentials.forUser(subject).claims(claims.asMap()).complete().build();

        } catch (JwtException e) {
            log.info("JWT is invalid (" + this.getType() + ")", e);
            return null;
        }

    }

    protected String getJwtTokenString(RestRequest request) {
        String authzHeader = request.header(jwtHeaderName);

        if (authzHeader == null) {
            return null;
        }

        authzHeader = authzHeader.trim();

        int separatorIndex = authzHeader.indexOf(' ');

        if (separatorIndex == -1) {
            log.info("Illegal Authorization header: " + authzHeader);
            return null;
        }

        String scheme = authzHeader.substring(0, separatorIndex);

        if (!scheme.equalsIgnoreCase("bearer")) {
            if (log.isDebugEnabled()) {
                log.debug("Unsupported authentication scheme " + scheme);
            }
            return null;
        }

        return authzHeader.substring(separatorIndex + 1).trim();
    }

    protected String extractSubject(JwtClaims claims) {
        String subject = claims.getSubject();

        if (subjectKey != null) {
            Object subjectObject = claims.getClaim(subjectKey);

            if (subjectObject == null) {
                log.warn("Failed to get subject from JWT claims, check if subject_key '{}' is correct.", subjectKey);
                return null;
            }

            // We expect a String. If we find something else, convert to String but issue a
            // warning
            if (!(subjectObject instanceof String)) {
                log.warn("Expected type String for roles in the JWT for subject_key {}, but value was '{}' ({}). Will convert this value to String.",
                        subjectKey, subjectObject, subjectObject.getClass());
                subject = String.valueOf(subjectObject);
            } else {
                subject = (String) subjectObject;
            }
        }

        return subject;
    }

    @Override
    public boolean reRequestAuthentication(RestChannel channel, AuthCredentials authCredentials) {
        final BytesRestResponse wwwAuthenticateResponse = new BytesRestResponse(RestStatus.UNAUTHORIZED, "");
        wwwAuthenticateResponse.addHeader("WWW-Authenticate", "Bearer realm=\"Search Guard\"");
        channel.sendResponse(wwwAuthenticateResponse);
        return true;
    }

}