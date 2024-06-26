/*
 * Copyright 2015-2019 floragunn GmbH
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

package com.floragunn.searchguard.legacy.auth;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.legacy.LegacyHTTPAuthenticator;
import com.floragunn.searchguard.legacy.LegacyComponentFactory;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.ComponentState;

public class HTTPProxyAuthenticator2 implements LegacyHTTPAuthenticator {
    
    private static final String ATTR_PROXY_PREFIX = "attr.proxy2.";
    private static final String ATTR_PROXY_USERNAME = ATTR_PROXY_PREFIX+"username";

    private static final String AUTHENTICATION_MODE = "auth_mode";
    private static final String USER_HEADER = "user_header";
    private static final String ROLES_HEADER = "roles_header";
    private static final String ROLES_SEPARATOR = "roles_separator";
    private static final String ALLOWED_DN_S = "allowed_dn_s";
    private static final String ATTRIBUTE_HEADERS = "attribute_headers";

    private static final String BOTH_MODE = "both";
    private static final String CERTIFICATE_MODE = "cert";
    private static final String IP_MODE = "ip";
    private static final String EITHER_MODE = "either";

    private static final String DEFAULT_MODE = BOTH_MODE;

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final Settings settings;
    private Map<String, String> attributeMapping;
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "proxy2", HTTPProxyAuthenticator2.class)
            .initialized();
    
    public HTTPProxyAuthenticator2(Settings settings, final Path configPath) {
        this.settings = settings;
        attributeMapping = Attributes.getFlatAttributeMapping(settings.getAsSettings("map_headers_to_user_attrs"));
    }

    public AuthCredentials extractCredentials(final RestRequest restRequest, final ThreadContext threadContext) {

        String authMode = settings.get(AUTHENTICATION_MODE);

        if (Strings.isNullOrEmpty(authMode)) {
            authMode = DEFAULT_MODE;
            if (log.isWarnEnabled()) {
                log.warn("Authentication mode not configured using default mode '{}'", DEFAULT_MODE);
            }
        } else if (!authMode.equals(BOTH_MODE) && !authMode.equals(CERTIFICATE_MODE) && !authMode.equals(IP_MODE) && !authMode.equals(EITHER_MODE)) {
            authMode = DEFAULT_MODE;
            if (log.isWarnEnabled()) {
                log.warn("Unknown authentication mode set. Using default authentication mode '{}'.", DEFAULT_MODE);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Authentication mode: '{}'", authMode);
        }

        if (authMode.equals(EITHER_MODE)) {
            if (!xffDone(threadContext) && !certificateDone(threadContext)) {
                if (log.isTraceEnabled()) {
                    log.trace("Authentication failed.");
                }
                return null;
            }
        } else if (authMode.equals(BOTH_MODE)) {
            if (!xffDone(threadContext) || !certificateDone(threadContext)) {
                return null;
            }
        } else if (authMode.equals(CERTIFICATE_MODE)) {
            if (!certificateDone(threadContext)) {
                return null;
            }
        } else if (authMode.equals(IP_MODE)) {
            if (!xffDone(threadContext)) {
                return null;
            }
        }

        final String userHeader = settings.get(USER_HEADER);
        final String rolesHeader = settings.get(ROLES_HEADER);
        final String rolesSeparator = settings.get(ROLES_SEPARATOR, ",");

        if (log.isDebugEnabled()) {
            log.debug("headers {}", restRequest.getHeaders());
            log.debug("userHeader {}, value {}", userHeader, userHeader == null ? null : restRequest.header(userHeader));
            log.debug("rolesHeader {}, value {}", rolesHeader, rolesHeader == null ? null : restRequest.header(rolesHeader));
        }

        if (Strings.isNullOrEmpty(userHeader) || Strings.isNullOrEmpty(restRequest.header(userHeader))) {
            if (log.isTraceEnabled()) {
                log.trace("No '{}' header, send 401", userHeader);
            }
            return null;
        }

        String[] backendRoles = null;

        if (!Strings.isNullOrEmpty(rolesHeader) && !Strings.isNullOrEmpty(restRequest.header(rolesHeader))) {
            backendRoles = restRequest.header(rolesHeader).split(rolesSeparator);
        }
        
        AuthCredentials.Builder authCredentialsBuilder = AuthCredentials.forUser(restRequest.header(userHeader)).authenticatorType(getType())
                .backendRoles(backendRoles).complete();

        addAdditionalAttributes(authCredentialsBuilder, restRequest);
        addAdditionalOldAttributes(authCredentialsBuilder, restRequest);

        return authCredentialsBuilder.build();
    }

    private boolean xffDone(ThreadContext threadContext) {
        if (threadContext.getTransient(ConfigConstants.SG_XFF_DONE) != Boolean.TRUE) {
            if (log.isTraceEnabled()) {
                log.trace("XFF not done, send 401");
            }
            return false;
        }
        return true;
    }

    private boolean certificateDone(ThreadContext threadContext) {
        final String principal = threadContext.getTransient(ConfigConstants.SG_SSL_PRINCIPAL);

        if (Strings.isNullOrEmpty(principal)) {
            if (log.isTraceEnabled()) {
                log.trace("No CLIENT CERT, send 401.");
            }
            return false;
        }
        
        final List<String> allowedDNs = settings.getAsList(ALLOWED_DN_S);
        
        if (allowedDNs.isEmpty()) {
            if (log.isWarnEnabled()) {
                log.warn("No trusted DNs configured, send 401.");
            }
            return false;
        }
        if (!WildcardMatcher.matchAny(allowedDNs, principal)) {
            if (log.isTraceEnabled()) {
                log.trace("DN: {} not trusted, send 401.", principal);
            }
            return false;
        }
        return true;
    }

    private void addAdditionalAttributes(final AuthCredentials.Builder credentials, final RestRequest restRequest) {

        for (Map.Entry<String, String> entry : attributeMapping.entrySet()) {
            String sourceAttributeName = entry.getValue();
            String targetAttributeName = entry.getKey();
            String attributeValue = restRequest.header(sourceAttributeName);

            if (attributeValue != null) {
                credentials.attribute(targetAttributeName, attributeValue);

            }
        }

    }
    
    private void addAdditionalOldAttributes(final AuthCredentials.Builder credentials, final RestRequest restRequest) {
        
        credentials.oldAttribute(ATTR_PROXY_USERNAME, credentials.getUserName());
        
        if (settings.getAsList(ATTRIBUTE_HEADERS).isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("No additional attributes configured.");
            }
            return;
        }

        for (String attributeHeaderName : settings.getAsList(ATTRIBUTE_HEADERS)) {
            if (log.isDebugEnabled()) {
                log.debug("Configured attribute header name: {}", attributeHeaderName);
            }
            if (!Strings.isNullOrEmpty(attributeHeaderName) && !Strings.isNullOrEmpty(restRequest.header(attributeHeaderName))) {
                String attributeValue = restRequest.header(attributeHeaderName);
                credentials.oldAttribute(ATTR_PROXY_PREFIX+attributeHeaderName, attributeValue);

                if (log.isDebugEnabled()) {
                    log.debug("attributeHeader {}, value {}", attributeHeaderName, attributeValue);
                }
            } else if (log.isTraceEnabled()) {
                log.trace("No additional attributes send.");
            }
        }
    }

    @Override
    public String getType() {
        return "proxy2";
    }
    
    public static TypedComponent.Info<LegacyHTTPAuthenticator> INFO = new TypedComponent.Info<LegacyHTTPAuthenticator>() {

        @Override
        public Class<LegacyHTTPAuthenticator> getType() {
            return LegacyHTTPAuthenticator.class;
        }

        @Override
        public String getName() {
            return "proxy2";
        }

        @Override
        public Factory<LegacyHTTPAuthenticator> getFactory() {
            return LegacyComponentFactory.adapt(HTTPProxyAuthenticator2::new);
        }
    };

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}