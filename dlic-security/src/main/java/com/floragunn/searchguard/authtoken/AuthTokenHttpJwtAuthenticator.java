/*
 * Copyright 2020-2022 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.authtoken;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtConstants;
import org.apache.cxf.rs.security.jose.jwt.JwtException;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.user.AuthCredentials;

public class AuthTokenHttpJwtAuthenticator implements HTTPAuthenticator {
    private final static Logger log = LogManager.getLogger(AuthTokenHttpJwtAuthenticator.class);

    private final AuthTokenService authTokenService;
    private final String jwtHeaderName;
    private final String subjectKey;
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "sg_auth_token",
            AuthTokenHttpJwtAuthenticator.class).initialized();

    public AuthTokenHttpJwtAuthenticator(AuthTokenService authTokenService) {
        this.authTokenService = authTokenService;
        this.jwtHeaderName = "Authorization";
        this.subjectKey = JwtConstants.CLAIM_SUBJECT;
    }

    public String getType() {
        return "sg_auth_token";
    }

    @Override
    public AuthCredentials extractCredentials(RestRequest request, ThreadContext context) throws ElasticsearchSecurityException {

        // TODO check whether this is really necessary
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<AuthCredentials>) () -> extractCredentials0(request));
    }

    private AuthCredentials extractCredentials0(RestRequest request) throws ElasticsearchSecurityException {

        String encodedJwt = getJwtTokenString(request);

        if (Strings.isNullOrEmpty(encodedJwt)) {
            return null;
        }

        try {
            JwtToken jwt = authTokenService.getVerifiedJwtToken(encodedJwt);

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
    public ComponentState getComponentState() {
        return componentState;
    }

}
