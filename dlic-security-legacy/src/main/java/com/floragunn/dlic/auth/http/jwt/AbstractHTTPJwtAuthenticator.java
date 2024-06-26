/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.jwt;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.dlic.util.Roles;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.legacy.LegacyHTTPAuthenticator;
import com.floragunn.searchguard.enterprise.auth.oidc.BadCredentialsException;
import com.floragunn.searchguard.enterprise.auth.oidc.JwtVerifier;
import com.floragunn.searchguard.enterprise.auth.oidc.KeyProvider;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;

public abstract class AbstractHTTPJwtAuthenticator implements LegacyHTTPAuthenticator {
    private final static Logger log = LogManager.getLogger(AbstractHTTPJwtAuthenticator.class);

    private static final String BEARER = "bearer ";

    private KeyProvider keyProvider;
    private JwtVerifier jwtVerifier;
    private final String jwtHeaderName;
    private final String jwtUrlParameter;
    private final String subjectKey;
    private final Pattern subjectPattern;
    private final String rolesKey;
    private final String jsonSubjectPath;
    private final String jsonRolesPath;
    private Configuration jsonPathConfig;
    private Map<String, JsonPath> attributeMapping;

    protected AbstractHTTPJwtAuthenticator(Settings settings, Path configPath) {
        jwtUrlParameter = settings.get("jwt_url_parameter");
        jwtHeaderName = settings.get("jwt_header", "Authorization");
        rolesKey = settings.get("roles_key");
        subjectKey = settings.get("subject_key");
        jsonRolesPath = settings.get("roles_path");
        jsonSubjectPath = settings.get("subject_path");
        subjectPattern = getSubjectPattern(settings);

        try {
            this.keyProvider = this.initKeyProvider(settings, configPath);
            jwtVerifier = new JwtVerifier(keyProvider, null, null);
        } catch (Exception e) {
            log.error("Error creating JWT authenticator: " + e + ". JWT authentication will not work", e);
        }

        if ((subjectKey != null && jsonSubjectPath != null) || (rolesKey != null && jsonRolesPath != null)) {
            throw new IllegalStateException("Both, subject_key and subject_path or roles_key and roles_path have simultaneously provided."
                    + " Please provide only one combination.");
        }
        jsonPathConfig = BasicJsonPathDefaultConfiguration.builder().options(Option.ALWAYS_RETURN_LIST).build();
        attributeMapping = Attributes.getAttributeMapping(settings.getAsSettings("map_claims_to_user_attrs"));
    }

    @Override
    public AuthCredentials extractCredentials(RestRequest request, ThreadContext context) throws ElasticsearchSecurityException {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<AuthCredentials>) () -> extractCredentials0(request));
    }

    private AuthCredentials extractCredentials0(final RestRequest request) throws ElasticsearchSecurityException {

        String jwtString = getJwtTokenString(request);

        if (Strings.isNullOrEmpty(jwtString)) {
            return null;
        }

        JwtToken jwt;

        try {
            jwt = jwtVerifier.getVerifiedJwtToken(jwtString);
        } catch (AuthenticatorUnavailableException e) {
            log.info(e);
            throw new ElasticsearchSecurityException(e.getMessage(), RestStatus.SERVICE_UNAVAILABLE);
        } catch (BadCredentialsException e) {
            log.info("Extracting JWT token from " + jwtString + " failed", e);
            return null;
        }

        JwtClaims claims = jwt.getClaims();

        if (log.isTraceEnabled()) {
            log.trace("Claims from JWT: " + claims);
        }

        final String subject = extractSubject(claims);

        if (subject == null) {
            log.error("No subject found in JWT token");
            return null;
        }

        final String[] roles = extractRoles(claims);

        if (log.isTraceEnabled()) {
            log.trace("From JWT:\nSubject: " + subject + "\nRoles: " + Arrays.asList(roles));
        }

        return AuthCredentials.forUser(subject).authenticatorType(getType()).backendRoles(roles).attributesByJsonPath(attributeMapping, claims)
                .prefixOldAttributes("attr.jwt.", claims.asMap()).complete().build();
    }

    protected String getJwtTokenString(RestRequest request) {
        String jwtToken = request.header(jwtHeaderName);

        if (jwtToken != null && jwtToken.toLowerCase().startsWith("basic ")) {
            jwtToken = null;
        }

        if (jwtUrlParameter != null) {
            if (jwtToken == null || jwtToken.isEmpty()) {
                jwtToken = request.param(jwtUrlParameter);
            } else {
                // just consume to avoid "contains unrecognized para)meter"
                request.param(jwtUrlParameter);
            }
        }

        if (jwtToken == null) {
            return null;
        }

        int index;

        if ((index = jwtToken.toLowerCase().indexOf(BEARER)) > -1) { // detect Bearer
            jwtToken = jwtToken.substring(index + BEARER.length());
        }

        return jwtToken;
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
        } else if (jsonSubjectPath != null) {
            try {
                Object subjectObject = JsonPath.using(BasicJsonPathDefaultConfiguration.defaultConfiguration()).parse(claims.asMap()).read(jsonSubjectPath);

                if (subjectObject == null) {
                    log.error("The subject is null: " + jsonSubjectPath);
                    return null;
                }

                if (subjectObject instanceof Collection) {
                    Collection<?> subjectCollection = (Collection<?>) subjectObject;

                    if (subjectCollection.size() == 0) {
                        log.error("The subject is empty: " + jsonSubjectPath);
                        return null;
                    }

                    if (subjectCollection.size() > 1) {
                        log.error("More than one subject was found. Failing authentication: " + subjectObject + "; " + jsonSubjectPath);
                        return null;
                    }

                    subject = String.valueOf(subjectCollection.iterator().next());

                } else {
                    subject = String.valueOf(subjectObject);
                }

            } catch (PathNotFoundException e) {
                log.error("The provided JSON path {} could not be found ", jsonSubjectPath);
                return null;
            }
        }

        if (subject != null && subjectPattern != null) {
            Matcher matcher = subjectPattern.matcher(subject);

            if (!matcher.matches()) {
                log.warn("Subject " + subject + " does not match subject_pattern " + subjectPattern);
                return null;
            }

            if (matcher.groupCount() == 1) {
                subject = matcher.group(1);
            } else if (matcher.groupCount() > 1) {
                StringBuilder subjectBuilder = new StringBuilder();

                for (int i = 1; i <= matcher.groupCount(); i++) {
                    if (matcher.group(i) != null) {
                        subjectBuilder.append(matcher.group(i));
                    }
                }

                if (subjectBuilder.length() != 0) {
                    subject = subjectBuilder.toString();
                } else {
                    subject = null;
                }
            }
        }

        return subject;
    }

    protected String[] extractRoles(JwtClaims claims) {
        if (rolesKey == null && jsonRolesPath == null) {
            return new String[0];
        }

        if (jsonRolesPath != null) {
            try {
                return Roles.split(JsonPath.using(jsonPathConfig).parse(claims.asMap()).read(jsonRolesPath));
            } catch (PathNotFoundException e) {
                log.error("The provided JSON path {} could not be found ", jsonRolesPath);
                return new String[0];
            }
        }

        Object rolesObject = claims.getClaim(rolesKey);

        if (rolesObject == null) {
            log.warn("Failed to get roles from JWT claims with roles_key '{}'. Check if this key is correct and available in the JWT payload.",
                    rolesKey);
            return new String[0];
        }

        return Roles.split(rolesObject);
    }

    protected abstract KeyProvider initKeyProvider(Settings settings, Path configPath) throws Exception;

    @Override
    public String getChallenge(AuthCredentials credentials) {
        return "Bearer realm=\"Search Guard\"";
    }

    private static Pattern getSubjectPattern(Settings settings) {
        String patternString = settings.get("subject_pattern");

        if (patternString == null) {
            return null;
        }

        try {
            return Pattern.compile(patternString);
        } catch (PatternSyntaxException e) {
            log.error("Invalid regular expression for subject_pattern: " + patternString, e);
            return null;
        }
    }
}
