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

package com.floragunn.searchguard.configuration;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class AdminDNs {

    protected final Logger log = LogManager.getLogger(AdminDNs.class);
    private final Set<LdapName> adminDn = new HashSet<LdapName>();
    private final ListMultimap<String, String> allowedRestImpersonations = ArrayListMultimap.<String, String> create();
    
    public AdminDNs(final Settings settings) {

        final List<String> adminDnsA = settings.getAsList(ConfigConstants.SEARCHGUARD_AUTHCZ_ADMIN_DN, Collections.emptyList());

        for (String dn : adminDnsA) {
            try {
                log.debug("{} is registered as an admin dn", dn);
                adminDn.add(new LdapName(dn));
            } catch (final InvalidNameException e) {
                log.error("Unable to parse admin dn {}", dn, e);
            }
        }
       
        log.debug("Loaded {} admin DN's {}",adminDn.size(),  adminDn);
               
        final Settings impersonationUsersRest = settings.getByPrefix(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+".");

        for (String user:impersonationUsersRest.keySet()) {
            allowedRestImpersonations.putAll(user, settings.getAsList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+"."+user));
        }
        
        log.debug("Loaded {} impersonation users for REST {}",allowedRestImpersonations.size(), allowedRestImpersonations);
    }

    public boolean isAdmin(User user) {
        if (isAdminDN(user.getName())) {
            return true;
        }

        return false;
    }
    
    public boolean isAdminDN(String dn) {
        
        if(dn == null) return false;
                
        try {
            return isAdminDN(new LdapName(dn));
        } catch (InvalidNameException e) {
           return false;
        }
    }

    private boolean isAdminDN(LdapName dn) {
        if(dn == null) return false;
        
        boolean isAdmin = adminDn.contains(dn);
        
        if (log.isTraceEnabled()) {
            log.trace("Is principal {} an admin cert? {}", dn.toString(), isAdmin);
        }
        
        return isAdmin;
    }
   
    public boolean isRestImpersonationAllowed(final String originalUser, final String impersonated) {
        if(originalUser == null) {
            return false;    
        }
        return WildcardMatcher.matchAny(this.allowedRestImpersonations.get(originalUser), impersonated);
    }
}
