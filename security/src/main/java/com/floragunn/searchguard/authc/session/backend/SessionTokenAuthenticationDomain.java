/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.session.backend;

import java.util.concurrent.CompletableFuture;

import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtConstants;
import org.apache.cxf.rs.security.jose.jwt.JwtException;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.authc.AuthenticationDebugLogger;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class SessionTokenAuthenticationDomain implements AuthenticationDomain<HTTPAuthenticator> {
    private final static Logger log = LogManager.getLogger(SessionTokenAuthenticationDomain.class);

    private final SessionService sessionService;
    private final SessionAuthenticator authenticator;
    private final ComponentState componentState = new ComponentState(0, "auth_domain", "session").initialized();

    SessionTokenAuthenticationDomain(SessionService sessionService) {
        this.sessionService = sessionService;
        this.authenticator = new SessionAuthenticator(sessionService);
        this.componentState.addPart(this.authenticator.getComponentState());
    }

    @Override
    public HTTPAuthenticator getFrontend() {
        return authenticator;
    }

    @Override
    public String getId() {
        return "session";
    }

    @Override
    public boolean accept(RequestMetaData<?> request) {
        return true;
    }

    @Override
    public boolean accept(AuthCredentials authCredentials) {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return sessionService.isEnabled();
    }

    public static class SessionAuthenticator implements HTTPAuthenticator {

        private final SessionService sessionService;
        private final String jwtHeaderName;
        private final String subjectKey;
        private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "session").initialized();

        public SessionAuthenticator(SessionService sessionService) {
            this.sessionService = sessionService;
            this.jwtHeaderName = "Authorization";
            this.subjectKey = JwtConstants.CLAIM_SUBJECT;
        }

        @Override
        public String getType() {
            return "session";
        }

        @Override
        public AuthCredentials extractCredentials(RestRequest request, ThreadContext context) throws ElasticsearchSecurityException {
            String encodedJwt = getJwtTokenString(request);

            if (Strings.isNullOrEmpty(encodedJwt)) {
                return null;
            }

            try {
                JwtToken jwt = sessionService.getVerifiedJwtToken(encodedJwt);

                if (jwt == null) {
                    return null;
                }

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
                    log.warn(
                            "Expected type String for roles in the JWT for subject_key {}, but value was '{}' ({}). Will convert this value to String.",
                            subjectKey, subjectObject, subjectObject.getClass());
                    subject = String.valueOf(subjectObject);
                } else {
                    subject = (String) subjectObject;
                }
            }

            return subject;
        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }
    }

    @Override
    public String getType() {
        return "session";
    }

    @Override
    public CompletableFuture<User> authenticate(AuthCredentials credentials, AuthenticationDebugLogger debug) throws AuthenticatorUnavailableException, CredentialsException {
        try {
            CompletableFuture<User> result = new CompletableFuture<User>();

            sessionService.getByClaims(credentials.getClaims(), (sessionToken) -> {
                if (sessionToken.isRevoked()) {
                    result.completeExceptionally(new ElasticsearchSecurityException(
                            "Session " + sessionToken.getId() + " has been expired or deleted", RestStatus.UNAUTHORIZED));
                } else {
                    sessionService.checkExpiryAndTrackAccess(sessionToken, (ok) -> {
                        if (ok) {
                            result.complete(User.forUser(sessionToken.getUserName()).type(SessionService.USER_TYPE)
                                    .backendRoles(sessionToken.getBase().getBackendRoles())
                                    .searchGuardRoles(sessionToken.getBase().getSearchGuardRoles()).specialAuthzConfig(sessionToken.getId())
                                    .attributes(sessionToken.getBase().getAttributes()).authzComplete().build());
                        } else {
                            result.completeExceptionally(new ElasticsearchSecurityException("Session " + sessionToken.getId() + " has been expired",
                                    RestStatus.UNAUTHORIZED));
                        }
                    }, (e) -> {
                        result.completeExceptionally(e);
                    });
                }

            }, (noSuchAuthTokenException) -> {
                result.complete(null);
            }, (e) -> {
                result.completeExceptionally(e);
            });

            return result;

        } catch (InvalidTokenException e) {
            log.info("Got InvalidTokenException for " + credentials, e);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<User> impersonate(User originalUser, AuthCredentials authCredentials)
            throws AuthenticatorUnavailableException, CredentialsException {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean cacheUser() {
        return false;
    }

    @Override
    public String toString() {
        return "session";
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
