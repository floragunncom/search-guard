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
package com.floragunn.searchguard.authc.rest.authenticators;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpClientCertAuthenticationFrontend implements HttpAuthenticationFrontend {

    private static final Logger log = LogManager.getLogger(HttpClientCertAuthenticationFrontend.class);
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "clientcert").initialized();

    public HttpClientCertAuthenticationFrontend(DocNode settings, ConfigurationRepository.Context context) {
    }

    @Override
    public AuthCredentials extractCredentials(RequestMetaData<?> request) {

        String principal = request.getClientCertSubject();

        if (Strings.isNullOrEmpty(principal)) {
            log.debug("No client cert provided");
            return null;
        }

        try {
            LdapName rfc2253dn = new LdapName(principal);
            String username = principal.trim();

            return AuthCredentials.forUser(username).userMappingAttribute("clientcert", ImmutableMap.of("subject", dnToMap(rfc2253dn))).build();
        } catch (InvalidNameException e) {
            log.error("Client cert had no properly formed DN (was: {})", principal, e);
            return null;
        }
    }

    @Override
    public String getType() {
        return "clientcert";
    }

    private Map<String, ?> dnToMap(LdapName ldapName) {
        Map<String, List<Object>> result = new HashMap<>();

        for (Rdn rdn : ldapName.getRdns()) {
            result.computeIfAbsent(rdn.getType(), (k) -> new ArrayList<>(1)).add(rdn.getValue());
        }

        return result;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
