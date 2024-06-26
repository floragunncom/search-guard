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

package com.floragunn.dlic.auth.ldap.backend;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.FilterTemplate;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapEntry;
import org.ldaptive.SearchScope;

import com.floragunn.dlic.auth.ldap.LdapUser;
import com.floragunn.dlic.auth.ldap.LdapUser.DirEntry;
import com.floragunn.dlic.auth.ldap.util.ConfigConstants;
import com.floragunn.dlic.auth.ldap.util.LdapHelper;
import com.floragunn.dlic.auth.ldap.util.Utils;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.legacy.LegacyAuthenticationBackend;
import com.floragunn.searchguard.legacy.LegacyComponentFactory;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class LDAPAuthenticationBackend implements LegacyAuthenticationBackend {

    static final int ZERO_PLACEHOLDER = 0;
    static final String DEFAULT_USERBASE = "";
    static final String DEFAULT_USERSEARCH_PATTERN = "(sAMAccountName={0})";

    protected static final Logger log = LogManager.getLogger(LDAPAuthenticationBackend.class);

    private final Settings settings;
    private final Path configPath;
    private final List<Map.Entry<String, Settings>> userBaseSettings;
    private final int customAttrMaxValueLen;
    private final List<String> whitelistedAttributes;
    private Map<String, String> attributeMapping;

    public LDAPAuthenticationBackend(final Settings settings, final Path configPath) {
        this.settings = settings;
        this.configPath = configPath;
        this.userBaseSettings = getUserBaseSettings(settings);

        customAttrMaxValueLen = settings.getAsInt(ConfigConstants.LDAP_CUSTOM_ATTR_MAXVAL_LEN, 36);
        whitelistedAttributes = settings.getAsList(ConfigConstants.LDAP_CUSTOM_ATTR_WHITELIST,
                null);
        attributeMapping = Attributes.getFlatAttributeMapping(settings.getAsSettings("map_ldap_attrs_to_user_attrs"));
    }

    @Override
    public User authenticate(final AuthCredentials credentials) throws ElasticsearchSecurityException {

        final String user = credentials.getUsername();
        byte[] password = credentials.getPassword();

        try {

            final ConnectionConfig connectionConfig = LDAPAuthorizationBackend.getConnectionConfig(settings, configPath);

            LdapEntry entry = exists(user, connectionConfig, settings, userBaseSettings);

            // fake a user that no exists
            // makes guessing if a user exists or not harder when looking on the
            // authentication delay time
            if (entry == null && settings.getAsBoolean(ConfigConstants.LDAP_FAKE_LOGIN_ENABLED, false)) {
                String fakeLognDn = settings.get(ConfigConstants.LDAP_FAKE_LOGIN_DN, "CN=faketomakebindfail,DC=" + UUID.randomUUID());
                entry = new LdapEntry();
                entry.setDn(fakeLognDn);
                password = settings.get(ConfigConstants.LDAP_FAKE_LOGIN_PASSWORD, "fakeLoginPwd123").getBytes(StandardCharsets.UTF_8);
            } else if (entry == null) {
                throw new ElasticsearchSecurityException("No user " + user + " found");
            }

            final String dn = entry.getDn();

            if (log.isTraceEnabled()) {
                log.trace("Try to authenticate dn {}", dn);
            }

            LDAPAuthorizationBackend.checkConnection(connectionConfig, dn, password);

            final String usernameAttribute = settings.get(ConfigConstants.LDAP_AUTHC_USERNAME_ATTRIBUTE, null);
            String username = dn;

            if (usernameAttribute != null && entry.getAttribute(usernameAttribute) != null) {
                username = Utils.getSingleStringValue(entry.getAttribute(usernameAttribute));
            }

            if (log.isDebugEnabled()) {
                log.debug("Authenticated username {}", username);
            }

            // by default all ldap attributes which are not binary and with a max value
            // length of 36 are included in the user object
            // if the whitelist contains at least one value then all attributes will be
            // additional check if whitelisted (whitelist can contain wildcard and regex)
            LdapUser ldapUser = new LdapUser(username, credentials.getAuthDomainInfo().authBackendType(getType()), user, new DirEntry(entry), credentials, customAttrMaxValueLen, whitelistedAttributes);
            
            processAttributeMapping(ldapUser, entry);
            
            return ldapUser;

        } catch (final Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to authenticate user due to ", e);
            }
            throw new ElasticsearchSecurityException(e.toString(), e);
        } finally {
            Arrays.fill(password, (byte) '\0');
            password = null;
        }

    }

    @Override
    public String getType() {
        return "ldap";
    }
    
    @Override
    public boolean exists(User user) {
        ConnectionConfig connectionConfig = null;
        String userName = user.getName();

        if (user instanceof LdapUser) {
            userName = ((LdapUser) user).getUserEntry().getDN();
        }

        try {
            connectionConfig = LDAPAuthorizationBackend.getConnectionConfig(settings, configPath);
            LdapEntry userEntry = exists(userName, connectionConfig, settings, userBaseSettings);
            boolean exists = userEntry != null;
            
            if(exists) {
                user.addAttributes(LdapUser.extractLdapAttributes(userName, new DirEntry(userEntry), customAttrMaxValueLen, whitelistedAttributes));
                processAttributeMapping(user, userEntry);
            }
            
            return exists;
            
        } catch (final Exception e) {
            log.warn("User {} does not exist due to " + e, userName);
            if (log.isDebugEnabled()) {
                log.debug("User does not exist due to ", e);
            }
            return false;
        }
    }
    
    private void processAttributeMapping(User user, LdapEntry ldapEntry) {
        for (Map.Entry<String, String> entry : attributeMapping.entrySet()) {
            String sourceAttributeName = entry.getValue();
            String targetAttributeName = entry.getKey();

            if (sourceAttributeName.equals("dn")) {
                user.addStructuredAttribute(targetAttributeName, ldapEntry.getDn());
            } else {
                LdapAttribute ldapAttribute = ldapEntry.getAttribute(sourceAttributeName);

                if (ldapAttribute == null) {
                    continue;
                }

                user.addStructuredAttribute(targetAttributeName, ldapAttribute.getStringValues());
            }
        }
    }

    static List<Map.Entry<String, Settings>> getUserBaseSettings(Settings settings) {
        Map<String, Settings> userBaseSettingsMap = new HashMap<>(
                settings.getGroups(ConfigConstants.LDAP_AUTHCZ_USERS));

        if (!userBaseSettingsMap.isEmpty()) {
            if (settings.hasValue(ConfigConstants.LDAP_AUTHC_USERBASE)) {
                throw new RuntimeException(
                        "Both old-style and new-style configuration defined for LDAP authentication backend: "
                                + settings);
            }

            return Utils.getOrderedBaseSettings(userBaseSettingsMap);
        } else {
            Settings.Builder settingsBuilder = Settings.builder();
            settingsBuilder.put(ConfigConstants.LDAP_AUTHCZ_BASE,
                    settings.get(ConfigConstants.LDAP_AUTHC_USERBASE, DEFAULT_USERBASE));
            settingsBuilder.put(ConfigConstants.LDAP_AUTHCZ_SEARCH,
                    settings.get(ConfigConstants.LDAP_AUTHC_USERSEARCH, DEFAULT_USERSEARCH_PATTERN));

            return Collections.singletonList(Pair.of("_legacyConfig", settingsBuilder.build()));
        }
    }

    static LdapEntry exists(final String user, ConnectionConfig connectionConfig, Settings settings,
            List<Map.Entry<String, Settings>> userBaseSettings) throws Exception {

        if (settings.getAsBoolean(ConfigConstants.LDAP_FAKE_LOGIN_ENABLED, false)
                || settings.getAsBoolean(ConfigConstants.LDAP_SEARCH_ALL_BASES, false)
                || settings.hasValue(ConfigConstants.LDAP_AUTHC_USERBASE)) {
            return existsSearchingAllBases(user, connectionConfig, userBaseSettings);
        } else {
            return existsSearchingUntilFirstHit(user, connectionConfig, userBaseSettings);
        }

    }

    private static LdapEntry existsSearchingUntilFirstHit(final String user, ConnectionConfig connectionConfig,
            List<Map.Entry<String, Settings>> userBaseSettings) throws Exception {
        final String username = user;

        for (Map.Entry<String, Settings> entry : userBaseSettings) {
            Settings baseSettings = entry.getValue();

            FilterTemplate f = new FilterTemplate();
            f.setFilter(baseSettings.get(ConfigConstants.LDAP_AUTHCZ_SEARCH, DEFAULT_USERSEARCH_PATTERN));
            f.setParameter(ZERO_PLACEHOLDER, username);

            List<LdapEntry> result = LdapHelper.search(connectionConfig,
                    baseSettings.get(ConfigConstants.LDAP_AUTHCZ_BASE, DEFAULT_USERBASE),
                    f,
                    SearchScope.SUBTREE);

            if (log.isDebugEnabled()) {
                log.debug("Results for LDAP search for " + user + " in base " + entry.getKey() + ":\n" + result);
            }

            if (result != null && result.size() >= 1) {
                return result.get(0);
            }
        }

        return null;
    }


    private static LdapEntry existsSearchingAllBases(final String user, ConnectionConfig connectionConfig,
            List<Map.Entry<String, Settings>> userBaseSettings) throws Exception {
        final String username = user;
        Set<LdapEntry> result = new HashSet<>();

        for (Map.Entry<String, Settings> entry : userBaseSettings) {
            Settings baseSettings = entry.getValue();

            FilterTemplate f = new FilterTemplate();
            f.setFilter(baseSettings.get(ConfigConstants.LDAP_AUTHCZ_SEARCH, DEFAULT_USERSEARCH_PATTERN));
            f.setParameter(ZERO_PLACEHOLDER, username);

            List<LdapEntry> foundEntries = LdapHelper.search(connectionConfig,
                    baseSettings.get(ConfigConstants.LDAP_AUTHCZ_BASE, DEFAULT_USERBASE),
                    f,
                    SearchScope.SUBTREE);

            if (log.isDebugEnabled()) {
                log.debug("Results for LDAP search for " + user + " in base " + entry.getKey() + ":\n" + result);
            }

            if (foundEntries != null) {
                result.addAll(foundEntries);
            }
        }

        if (result.isEmpty()) {
            log.debug("No user " + username + " found");
            return null;
        }

        if (result.size() > 1) {
            log.debug("More than one user for '" + username + "' found");
            return null;
        }

        return result.iterator().next();
    }

 
    public static TypedComponent.Info<LegacyAuthenticationBackend> INFO = new TypedComponent.Info<LegacyAuthenticationBackend>() {

        @Override
        public Class<LegacyAuthenticationBackend> getType() {
            return LegacyAuthenticationBackend.class;
        }

        @Override
        public String getName() {
            return "ldap";
        }

        @Override
        public Factory<LegacyAuthenticationBackend> getFactory() {
            return LegacyComponentFactory.adapt(LDAPAuthenticationBackend::new);
        }
    };

}
