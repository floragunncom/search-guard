/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.ldap.util;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.naming.ldap.LdapName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.DefaultConnectionFactory;
import org.ldaptive.DerefAliases;
import org.ldaptive.FilterTemplate;
import org.ldaptive.LdapEntry;
import org.ldaptive.LdapException;
import org.ldaptive.ReturnAttributes;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.SearchScope;
import org.ldaptive.referral.FollowSearchReferralHandler;

public class LdapHelper {

    protected static final Logger log = LogManager.getLogger(LdapHelper.class);

    private static FilterTemplate ALL = new FilterTemplate("(objectClass=*)");

    public static List<LdapEntry> search(final ConnectionConfig connectionConfig, final String baseDn, FilterTemplate filter,
            final SearchScope searchScope) throws LdapException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {

            if(log.isDebugEnabled())
            log.debug("baseDn {}", baseDn);

            return AccessController.doPrivileged((PrivilegedExceptionAction<List<LdapEntry>>) () -> {
                SearchOperation search = new SearchOperation(new DefaultConnectionFactory(connectionConfig));
                search.setSearchResultHandlers(new FollowSearchReferralHandler());
                SearchResponse r =
                    search.execute(SearchRequest.builder()
                        .dn(baseDn)
                        .aliases(DerefAliases.ALWAYS)
                        .filter(filter)
                        .scope(searchScope)
                        .returnAttributes(ReturnAttributes.ALL.value())
                        .build());
                return new ArrayList<>(r.getEntries());
            });
        } catch (PrivilegedActionException e) {
            if (e.getException() instanceof LdapException) {
                throw (LdapException) e.getException();
            } else if (e.getException() instanceof RuntimeException) {
                throw (RuntimeException) e.getException();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static LdapEntry lookup(ConnectionConfig connectionConfig, LdapName roleDn) throws LdapException {
        return lookup(connectionConfig, roleDn.toString());
    }

    public static LdapEntry lookup(final ConnectionConfig connectionConfig, final String unescapedDn) throws LdapException {

        final List<LdapEntry> entries = search(connectionConfig, unescapedDn, ALL, SearchScope.OBJECT);

        if (entries.size() == 1) {
            return entries.get(0);
        } else {
            return null;
        }
    }
}
