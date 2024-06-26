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

package com.floragunn.dlic.auth.ldap2;

import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.dlic.auth.ldap.LdapUser;
import com.floragunn.dlic.auth.ldap.util.ConfigConstants;
import com.floragunn.dlic.auth.ldap.util.Utils;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator.SSLConfigException;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.legacy.LegacyAuthorizationBackend;
import com.floragunn.searchguard.legacy.LegacyComponentFactory;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.HashMultimap;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;

public class LDAPAuthorizationBackend2 implements LegacyAuthorizationBackend, AutoCloseable {

    static final int ZERO_PLACEHOLDER = 0;
    static final int ONE_PLACEHOLDER = 1;
    static final int TWO_PLACEHOLDER = 2;
    static final String DEFAULT_ROLEBASE = "";
    static final String DEFAULT_ROLESEARCH = "(member={0})";
    static final String DEFAULT_ROLENAME = "name";
    static final String DEFAULT_USERROLENAME = "memberOf";

    protected static final Logger log = LogManager.getLogger(LDAPAuthorizationBackend2.class);
    private final Settings settings;
    private final List<Map.Entry<String, Settings>> roleBaseSettings;
    private final LDAPConnectionManager lcm;

    public LDAPAuthorizationBackend2(final Settings settings, final Path configPath) {
        this.settings = settings;
        this.roleBaseSettings = getRoleSearchSettings(settings);
        try {
            this.lcm = new LDAPConnectionManager(settings, configPath);
        } catch (LDAPException | SSLConfigException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Map.Entry<String, Settings>> getRoleSearchSettings(Settings settings) {
        Map<String, Settings> groupedSettings = settings.getGroups(ConfigConstants.LDAP_AUTHZ_ROLES, true);

        if (!groupedSettings.isEmpty()) {
            // New style settings
            return Utils.getOrderedBaseSettings(groupedSettings);
        } else {
            // Old style settings
            return convertOldStyleSettingsToNewStyle(settings);
        }
    }

    private static List<Map.Entry<String, Settings>> convertOldStyleSettingsToNewStyle(Settings settings) {
        Map<String, Settings> result = new HashMap<>(1);

        Settings.Builder settingsBuilder = Settings.builder();

        settingsBuilder.put(ConfigConstants.LDAP_AUTHCZ_BASE,
                settings.get(ConfigConstants.LDAP_AUTHZ_ROLEBASE, DEFAULT_ROLEBASE));
        settingsBuilder.put(ConfigConstants.LDAP_AUTHCZ_SEARCH,
                settings.get(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, DEFAULT_ROLESEARCH));

        result.put("convertedOldStyleSettings", settingsBuilder.build());

        return Collections.singletonList(result.entrySet().iterator().next());
    }
    
    @Override
    public void fillRoles(final User user, final AuthCredentials optionalAuthCreds)
            throws ElasticsearchSecurityException {
        
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    fillRoles0(user, optionalAuthCreds);
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            if (e.getException() instanceof ElasticsearchSecurityException) {
                throw (ElasticsearchSecurityException) e.getException();
            } else if (e.getException() instanceof RuntimeException) {
                throw (RuntimeException) e.getException();
            } else {
                throw new RuntimeException(e.getException());
            }
        }
    }

    private void fillRoles0(final User user, final AuthCredentials optionalAuthCreds)
            throws ElasticsearchSecurityException {

        if (user == null) {
            return;
        }

        String authenticatedUser;
        String originalUserName;
        SearchResultEntry entry = null;
        String dn = null;

        if (user instanceof LdapUser && ((LdapUser) user).getUserEntry() != null && ((LdapUser) user).getUserEntry().getUbEntry() != null) {
            entry = ((LdapUser) user).getUserEntry().getUbEntry();
            dn = entry.getDN();
            authenticatedUser = entry.getDN();
            originalUserName = ((LdapUser) user).getOriginalUsername();
        } else {
            authenticatedUser = user.getName();
            originalUserName = user.getName();
        }

        final boolean rolesearchEnabled = settings.getAsBoolean(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true);

        if (log.isDebugEnabled()) {
            log.debug("Try to get roles for {}", authenticatedUser);
        }

        if (log.isTraceEnabled()) {
            log.trace("user class: {}", user.getClass());
            log.trace("authenticatedUser: {}", authenticatedUser);
            log.trace("originalUserName: {}", originalUserName);
            log.trace("entry: {}", String.valueOf(entry));
            log.trace("dn: {}", dn);
        }

        final List<String> skipUsers = settings.getAsList(ConfigConstants.LDAP_AUTHZ_SKIP_USERS,
                Collections.emptyList());
        if (!skipUsers.isEmpty() && (WildcardMatcher.matchAny(skipUsers, originalUserName)
                || WildcardMatcher.matchAny(skipUsers, authenticatedUser))) {
            if (log.isDebugEnabled()) {
                log.debug("Skipped search roles of user {}/{}", authenticatedUser, originalUserName);
            }
            return;
        }

        try (LDAPConnection con = lcm.getConnection()) {
            
            if (entry == null || dn == null) {

                if (isValidDn(authenticatedUser)) {
                    // assume dn
                    if (log.isTraceEnabled()) {
                        log.trace("{} is a valid DN", authenticatedUser);
                    }

                    entry = lcm.lookup(con, authenticatedUser);

                    if (entry == null) {
                        throw new ElasticsearchSecurityException("No user '" + authenticatedUser + "' found");
                    }

                } else {
                    entry = this.lcm.exists(con, user.getName());

                    if (log.isTraceEnabled()) {
                        log.trace("{} is not a valid DN and was resolved to {}", authenticatedUser, entry);
                    }

                    if (entry == null || entry.getDN() == null) {
                        throw new ElasticsearchSecurityException("No user " + authenticatedUser + " found");
                    }
                }

                dn = entry.getDN();

                if (log.isTraceEnabled()) {
                    log.trace("User found with DN {}", dn);
                }
            }

            final Set<LdapName> ldapRoles = new HashSet<>(150);
            final Set<String> nonLdapRoles = new HashSet<>(150);
            final HashMultimap<LdapName, Map.Entry<String, Settings>> resultRoleSearchBaseKeys = HashMultimap.create();

            // Roles as an attribute of the user entry
            // default is userrolename: memberOf
            final String userRoleNames = settings.get(ConfigConstants.LDAP_AUTHZ_USERROLENAME, DEFAULT_USERROLENAME);

            if (log.isTraceEnabled()) {
                log.trace("raw userRoleName(s): {}", userRoleNames);
            }

            // we support more than one rolenames, must be separated by a comma
            for (String userRoleName : userRoleNames.split(",")) {
                final String roleName = userRoleName.trim();
                if (entry.getAttribute(roleName) != null) {
                    final String[] userRoles = entry.getAttribute(roleName).getValues();
                    for (final String possibleRoleDN : userRoles) {
                        if (isValidDn(possibleRoleDN)) {
                            LdapName ldapName = new LdapName(possibleRoleDN);
                            ldapRoles.add(ldapName);
                            resultRoleSearchBaseKeys.putAll(ldapName, this.roleBaseSettings);
                        } else {
                            nonLdapRoles.add(possibleRoleDN);
                        }
                    }
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("User attr. ldap roles count: {}", ldapRoles.size());
                log.trace("User attr. ldap roles {}", ldapRoles);
                log.trace("User attr. non-ldap roles count: {}", nonLdapRoles.size());
                log.trace("User attr. non-ldap roles {}", nonLdapRoles);

            }

            // The attribute in a role entry containing the name of that role, Default is
            // "name".
            // Can also be "dn" to use the full DN as rolename.
            // rolename: name
            final String roleName = settings.get(ConfigConstants.LDAP_AUTHZ_ROLENAME, DEFAULT_ROLENAME);

            if (log.isTraceEnabled()) {
                log.trace("roleName: {}", roleName);
            }

            // Specify the name of the attribute which value should be substituted with {2}
            // Substituted with an attribute value from user's directory entry, of the
            // authenticated user
            // userroleattribute: null
            final String userRoleAttributeName = settings.get(ConfigConstants.LDAP_AUTHZ_USERROLEATTRIBUTE, null);

            if (log.isTraceEnabled()) {
                log.trace("userRoleAttribute: {}", userRoleAttributeName);
                log.trace("rolesearch: {}", settings.get(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, DEFAULT_ROLESEARCH));
            }

            String userRoleAttributeValue = null;
            final Attribute userRoleAttribute = userRoleAttributeName == null?null:entry.getAttribute(userRoleAttributeName);

            if (userRoleAttribute != null) {
                userRoleAttributeValue = Utils.getSingleStringValue(userRoleAttribute);
            }

            if (rolesearchEnabled) {
                String escapedDn = dn;

                for (Map.Entry<String, Settings> roleSearchSettingsEntry : roleBaseSettings) {
                    Settings roleSearchSettings = roleSearchSettingsEntry.getValue();
                    
                    ParametrizedFilter pf = new ParametrizedFilter(roleSearchSettings.get(ConfigConstants.LDAP_AUTHCZ_SEARCH, DEFAULT_ROLESEARCH));
                    pf.setParameter(ZERO_PLACEHOLDER, escapedDn);
                    pf.setParameter(ONE_PLACEHOLDER, originalUserName);
                    pf.setParameter(TWO_PLACEHOLDER,
                            userRoleAttributeValue == null ? null : userRoleAttributeValue);
                    
                    List<SearchResultEntry> rolesResult = lcm.search(con,
                            roleSearchSettings.get(ConfigConstants.LDAP_AUTHCZ_BASE, DEFAULT_ROLEBASE),
                            SearchScope.SUB,
                            pf
                            );

                    if (log.isTraceEnabled()) {
                        log.trace("Results for LDAP group search for " + escapedDn + " in base "
                                + roleSearchSettingsEntry.getKey() + ":\n" + rolesResult);
                    }

                    if (rolesResult != null && !rolesResult.isEmpty()) {
                        for (final Iterator<SearchResultEntry> iterator = rolesResult.iterator(); iterator.hasNext();) {
                            SearchResultEntry searchResultEntry = iterator.next();
                            LdapName ldapName = new LdapName(searchResultEntry.getDN());
                            ldapRoles.add(ldapName);
                            resultRoleSearchBaseKeys.put(ldapName, roleSearchSettingsEntry);
                        }
                    }
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("roles count total {}", ldapRoles.size());
            }

            // nested roles, makes only sense for DN style role names
            if (settings.getAsBoolean(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, false)) {

                final List<String> nestedRoleFilter = settings.getAsList(ConfigConstants.LDAP_AUTHZ_NESTEDROLEFILTER,
                        Collections.emptyList());

                if (log.isTraceEnabled()) {
                    log.trace("Evaluate nested roles");
                }

                final Set<LdapName> nestedReturn = new HashSet<>(ldapRoles);

                for (final LdapName roleLdapName : ldapRoles) {
                    Set<Map.Entry<String, Settings>> nameRoleSearchBaseKeys = resultRoleSearchBaseKeys
                            .get(roleLdapName);

                    if (nameRoleSearchBaseKeys == null) {
                        log.error("Could not find roleSearchBaseKeys for " + roleLdapName + "; existing: "
                                + resultRoleSearchBaseKeys);
                        continue;
                    }

                    final Set<LdapName> nestedRoles = resolveNestedRoles(roleLdapName, con, userRoleNames, 0,
                            rolesearchEnabled, nameRoleSearchBaseKeys, nestedRoleFilter);

                    if (log.isTraceEnabled()) {
                        log.trace("{} nested roles for {}", nestedRoles.size(), roleLdapName);
                    }

                    nestedReturn.addAll(nestedRoles);
                }

                for (final LdapName roleLdapName : nestedReturn) {
                    final String role = getRoleFromEntry(con, roleLdapName, roleName);

                    if (!Strings.isNullOrEmpty(role)) {
                        user.addRole(role);
                    } else {
                        log.warn("No or empty attribute '{}' for entry {}", roleName, roleLdapName);
                    }
                }

            } else {
                // DN roles, extract rolename according to config
                for (final LdapName roleLdapName : ldapRoles) {
                    final String role = getRoleFromEntry(con, roleLdapName, roleName);

                    if (!Strings.isNullOrEmpty(role)) {
                        user.addRole(role);
                    } else {
                        log.warn("No or empty attribute '{}' for entry {}", roleName, roleLdapName);
                    }
                }

            }

            // add all non-LDAP roles from user attributes to the final set of backend roles
            for (String nonLdapRoleName : nonLdapRoles) {
                user.addRole(nonLdapRoleName);
            }

            if (log.isDebugEnabled()) {
                log.debug("Roles for {} -> {}", user.getName(), user.getRoles());
            }

            if (log.isTraceEnabled()) {
                log.trace("returned user: {}", user);
            }

        } catch (final Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to fill user roles due to ", e);
            }
            throw new ElasticsearchSecurityException(e.toString(), e);
        }

    }

    protected Set<LdapName> resolveNestedRoles(final LdapName roleDn, LDAPConnection con,
            String userRoleName, int depth, final boolean rolesearchEnabled,
            Set<Map.Entry<String, Settings>> roleSearchBaseSettingsSet, final List<String> roleFilter)
            throws ElasticsearchSecurityException, LDAPException {

        if (!roleFilter.isEmpty() && WildcardMatcher.matchAny(roleFilter, roleDn.toString())) {

            if (log.isTraceEnabled()) {
                log.trace("Filter nested role {}", roleDn);
            }

            return Collections.emptySet();
        }

        depth++;

        final Set<LdapName> result = new HashSet<>(20);
        final HashMultimap<LdapName, Map.Entry<String, Settings>> resultRoleSearchBaseKeys = HashMultimap.create();

        final SearchResultEntry e0 = lcm.lookup(con, roleDn.toString());

        if (e0.getAttribute(userRoleName) != null) {
            final String[] userRoles = e0.getAttribute(userRoleName).getValues();

            for (final String possibleRoleDN : userRoles) {
                if (isValidDn(possibleRoleDN)) {
                    try {
                        LdapName ldapName = new LdapName(possibleRoleDN);
                        result.add(ldapName);
                        resultRoleSearchBaseKeys.putAll(ldapName, this.roleBaseSettings);
                    } catch (InvalidNameException e) {
                        // ignore
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Cannot add {} as a role because its not a valid dn", possibleRoleDN);
                    }
                }
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("result nested attr count for depth {} : {}", depth, result.size());
        }

        if (rolesearchEnabled) {
            String escapedDn = roleDn.toString();

            for (Map.Entry<String, Settings> roleSearchBaseSettingsEntry : Utils
                    .getOrderedBaseSettings(roleSearchBaseSettingsSet)) {
                Settings roleSearchSettings = roleSearchBaseSettingsEntry.getValue();
                
                ParametrizedFilter pf = new ParametrizedFilter(roleSearchSettings.get(ConfigConstants.LDAP_AUTHCZ_SEARCH, DEFAULT_ROLESEARCH));
                pf.setParameter(ZERO_PLACEHOLDER, escapedDn);
                pf.setParameter(ONE_PLACEHOLDER, escapedDn);

                List<SearchResultEntry> foundEntries = lcm.search(con,
                        roleSearchSettings.get(ConfigConstants.LDAP_AUTHCZ_BASE, DEFAULT_ROLEBASE),
                        SearchScope.SUB,
                        pf);

                if (log.isTraceEnabled()) {
                    log.trace("Results for LDAP group search for " + escapedDn + " in base "
                            + roleSearchBaseSettingsEntry.getKey() + ":\n" + foundEntries);
                }

                if (foundEntries != null) {
                    for (final SearchResultEntry entry : foundEntries) {
                        try {
                            final LdapName dn = new LdapName(entry.getDN());
                            result.add(dn);
                            resultRoleSearchBaseKeys.put(dn, roleSearchBaseSettingsEntry);
                        } catch (final InvalidNameException e) {
                            throw new LDAPException(ResultCode.INVALID_DN_SYNTAX, e);
                        }
                    }
                }
            }
        }

        int maxDepth = ConfigConstants.LDAP_AUTHZ_MAX_NESTED_DEPTH_DEFAULT;
        try {
            maxDepth = settings.getAsInt(ConfigConstants.LDAP_AUTHZ_MAX_NESTED_DEPTH,
                    ConfigConstants.LDAP_AUTHZ_MAX_NESTED_DEPTH_DEFAULT);
        } catch (Exception e) {
            log.error(ConfigConstants.LDAP_AUTHZ_MAX_NESTED_DEPTH + " is not parseable: " + e, e);
        }

        if (depth < maxDepth) {
            for (final LdapName nm : new HashSet<LdapName>(result)) {
                Set<Map.Entry<String, Settings>> nameRoleSearchBaseKeys = resultRoleSearchBaseKeys.get(nm);

                if (nameRoleSearchBaseKeys == null) {
                    log.error(
                            "Could not find roleSearchBaseKeys for " + nm + "; existing: " + resultRoleSearchBaseKeys);
                    continue;
                }

                final Set<LdapName> in = resolveNestedRoles(nm, con, userRoleName, depth, rolesearchEnabled,
                        nameRoleSearchBaseKeys, roleFilter);
                result.addAll(in);
            }
        }

        return result;
    }

    @Override
    public String getType() {
        return "ldap";
    }

    private boolean isValidDn(final String dn) {

        if (Strings.isNullOrEmpty(dn)) {
            return false;
        }

        try {
            new LdapName(dn);
        } catch (final Exception e) {
            return false;
        }

        return true;
    }

    private String getRoleFromEntry(LDAPConnection con, final LdapName ldapName, final String role) {

        if (ldapName == null || Strings.isNullOrEmpty(role)) {
            return null;
        }

        if("dn".equalsIgnoreCase(role)) {
            return ldapName.toString();
        }

        try {
            final SearchResultEntry roleEntry = lcm.lookup(con, ldapName.toString());

            if(roleEntry != null) {
                final Attribute roleAttribute = roleEntry.getAttribute(role);
                if(roleAttribute != null) {
                    return Utils.getSingleStringValue(roleAttribute);
                }
            }
        } catch (LDAPException e) {
            log.error("Unable to handle role {} because of ",ldapName, e.toString(), e);
        }

        return null;
    }

    @Override
    public void close() {
        if (this.lcm != null) {
            try {
                this.lcm.close();
            } catch (IOException e) {
                //ignore
            }
        }
    }
    
    public static TypedComponent.Info<LegacyAuthorizationBackend> INFO = new TypedComponent.Info<LegacyAuthorizationBackend>() {

        @Override
        public Class<LegacyAuthorizationBackend> getType() {
            return LegacyAuthorizationBackend.class;
        }

        @Override
        public String getName() {
            return "ldap2";
        }

        @Override
        public Factory<LegacyAuthorizationBackend> getFactory() {
            return LegacyComponentFactory.adapt(LDAPAuthorizationBackend2::new);
        }
    };    
    

}
