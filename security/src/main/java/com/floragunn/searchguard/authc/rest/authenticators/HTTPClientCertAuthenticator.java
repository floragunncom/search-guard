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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.util.ImmutableMap;
import com.google.common.base.Strings;

public class HTTPClientCertAuthenticator implements HTTPAuthenticator {

    private static final Logger log = LogManager.getLogger(HTTPClientCertAuthenticator.class);

    public HTTPClientCertAuthenticator(DocNode settings, ConfigurationRepository.Context context) {
    }

    @Override
    public AuthCredentials extractCredentials(final RestRequest request, final ThreadContext threadContext) {

        String principal = threadContext.getTransient(ConfigConstants.SG_SSL_PRINCIPAL);

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
    public boolean reRequestAuthentication(final RestChannel channel, AuthCredentials creds) {
        return false;
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
}
