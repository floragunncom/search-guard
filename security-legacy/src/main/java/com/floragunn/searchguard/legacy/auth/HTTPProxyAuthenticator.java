/*
 * Copyright 2015-2017 floragunn GmbH
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.legacy.LegacyHTTPAuthenticator;
import com.floragunn.searchguard.legacy.LegacyComponentFactory;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.AuthCredentials;

public class HTTPProxyAuthenticator implements LegacyHTTPAuthenticator {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private volatile Settings settings;
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "proxy", HTTPProxyAuthenticator.class)
            .initialized();
    
    public HTTPProxyAuthenticator(Settings settings, final Path configPath) {
        super();
        this.settings = settings;
    }

    @Override
    public AuthCredentials extractCredentials(final RestRequest request, ThreadContext context) {
    	
        if(context.getTransient(ConfigConstants.SG_XFF_DONE) !=  Boolean.TRUE) {
            throw new ElasticsearchSecurityException("xff not done");
        }
        
        final String userHeader = settings.get("user_header");
        final String rolesHeader = settings.get("roles_header");
        final String rolesSeparator = settings.get("roles_separator", ",");
        
        if(log.isDebugEnabled()) {
            log.debug("headers {}", request.getHeaders());
            log.debug("userHeader {}, value {}", userHeader, userHeader == null?null:request.header(userHeader));
            log.debug("rolesHeader {}, value {}", rolesHeader, rolesHeader == null?null:request.header(rolesHeader));
        }

        if (!Strings.isNullOrEmpty(userHeader) && !Strings.isNullOrEmpty((String) request.header(userHeader))) {

            String[] backendRoles = null;

            if (!Strings.isNullOrEmpty(rolesHeader) && !Strings.isNullOrEmpty((String) request.header(rolesHeader))) {
                backendRoles = ((String) request.header(rolesHeader)).split(rolesSeparator);
            }
            return AuthCredentials.forUser((String) request.header(userHeader)).authenticatorType(getType()).backendRoles(backendRoles).complete().build();
        } else {
            if(log.isTraceEnabled()) {
                log.trace("No '{}' header, send 401", userHeader);
            }
            return null;
        }
    }

    @Override
    public String getType() {
        return "proxy";
    }
    
    public static TypedComponent.Info<LegacyHTTPAuthenticator> INFO = new TypedComponent.Info<LegacyHTTPAuthenticator>() {

        @Override
        public Class<LegacyHTTPAuthenticator> getType() {
            return LegacyHTTPAuthenticator.class;
        }

        @Override
        public String getName() {
            return "proxy";
        }

        @Override
        public Factory<LegacyHTTPAuthenticator> getFactory() {
            return LegacyComponentFactory.adapt(HTTPProxyAuthenticator::new);
        }
    };

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
