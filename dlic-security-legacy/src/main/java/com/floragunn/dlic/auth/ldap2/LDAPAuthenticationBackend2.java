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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.SpecialPermission;
import org.opensearch.common.settings.Settings;

import com.floragunn.dlic.auth.ldap.LdapUser;
import com.floragunn.dlic.auth.ldap.LdapUser.DirEntry;
import com.floragunn.dlic.auth.ldap.util.ConfigConstants;
import com.floragunn.dlic.auth.ldap.util.Utils;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator.SSLConfigException;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.legacy.LegacyAuthenticationBackend;
import com.floragunn.searchguard.configuration.Destroyable;
import com.floragunn.searchguard.legacy.LegacyComponentFactory;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchResultEntry;

public class LDAPAuthenticationBackend2 implements LegacyAuthenticationBackend, Destroyable {

    protected static final Logger log = LogManager.getLogger(LDAPAuthenticationBackend2.class);

    private final Settings settings;

    private final LDAPConnectionManager lcm;
    private final int customAttrMaxValueLen;
    private final List<String> whitelistedAttributes;
    private Map<String, String> attributeMapping;

    public LDAPAuthenticationBackend2(final Settings settings, final Path configPath) {
        try {
            this.settings = settings;
            this.lcm = new LDAPConnectionManager(settings, configPath);
            customAttrMaxValueLen = settings.getAsInt(ConfigConstants.LDAP_CUSTOM_ATTR_MAXVAL_LEN, 36);
            whitelistedAttributes = settings.getAsList(ConfigConstants.LDAP_CUSTOM_ATTR_WHITELIST, null);
            attributeMapping = Attributes.getFlatAttributeMapping(settings.getAsSettings("map_ldap_attrs_to_user_attrs"));
        } catch (LDAPException | SSLConfigException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public User authenticate(final AuthCredentials credentials) throws OpenSearchSecurityException {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<User>() {
                @Override
                public User run() throws Exception {
                    return authenticate0(credentials);
                }
            });
        } catch (PrivilegedActionException e) {
            if (e.getException() instanceof OpenSearchSecurityException) {
                throw (OpenSearchSecurityException) e.getException();
            } else if (e.getException() instanceof RuntimeException) {
                throw (RuntimeException) e.getException();
            } else {
                throw new RuntimeException(e.getException());
            }
        }
    }


    private User authenticate0(final AuthCredentials credentials) throws OpenSearchSecurityException {

        final String user = credentials.getUsername();
        byte[] password = credentials.getPassword();

        try {

            SearchResultEntry entry = null;

            try (LDAPConnection con = lcm.getConnection()) {
                entry = lcm.exists(con, user);
            }

            // fake a user that no exists
            // makes guessing if a user exists or not harder when looking on the
            // authentication delay time
            if (entry == null && settings.getAsBoolean(ConfigConstants.LDAP_FAKE_LOGIN_ENABLED, false)) {
                String fakeLognDn = settings.get(ConfigConstants.LDAP_FAKE_LOGIN_DN,
                        "CN=faketomakebindfail,DC=" + UUID.randomUUID().toString());
                entry = new SearchResultEntry(fakeLognDn, new Attribute[0]);
                password = settings.get(ConfigConstants.LDAP_FAKE_LOGIN_PASSWORD, "fakeLoginPwd123")
                        .getBytes(StandardCharsets.UTF_8);
            } else if (entry == null) {
                throw new OpenSearchSecurityException("No user " + user + " found");
            }

            final String dn = entry.getDN();

            if (log.isTraceEnabled()) {
                log.trace("Try to authenticate dn {}", dn);
            }
            
            lcm.checkDnPassword(dn, password);

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
            throw new OpenSearchSecurityException(e.toString(), e);
        } finally {
            Arrays.fill(password, (byte) '\0');
            password = null;
        }

    }

    @Override
    public String getType() {
        return "ldap";
    }
    
    private void processAttributeMapping(User user, SearchResultEntry ldapEntry) {
        for (Map.Entry<String, String> entry : attributeMapping.entrySet()) {
            String sourceAttributeName = entry.getValue();
            String targetAttributeName = entry.getKey();

            if (sourceAttributeName.equals("dn")) {
                user.addStructuredAttribute(targetAttributeName, ldapEntry.getDN());
            } else {
                Attribute ldapAttribute = ldapEntry.getAttribute(sourceAttributeName);

                if (ldapAttribute == null) {
                    continue;
                }

                user.addStructuredAttribute(targetAttributeName, Arrays.asList(ldapAttribute.getValues()));
            }
        }
    }
    
  

    @Override
    public void destroy() {
        if (this.lcm != null) {
            try {
                this.lcm.close();
            } catch (IOException e) {
                //ignore
            }
        }
    }

    @Override
    public boolean exists(User user) {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
            @Override
            public Boolean run() {
                return impersonate0(user);
            }
        });

    }

    private boolean impersonate0(final User user) {

        String userName = user.getName();

        if (user instanceof LdapUser) {
            userName = ((LdapUser) user).getUserEntry().getDN();
        }

        try (LDAPConnection con = lcm.getConnection()) {
            SearchResultEntry userEntry = lcm.exists(con, userName);

            boolean exists = userEntry != null;

            if (exists) {
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
    
    public static TypedComponent.Info<LegacyAuthenticationBackend> INFO = new TypedComponent.Info<LegacyAuthenticationBackend>() {

        @Override
        public Class<LegacyAuthenticationBackend> getType() {
            return LegacyAuthenticationBackend.class;
        }

        @Override
        public String getName() {
            return "ldap2";
        }

        @Override
        public Factory<LegacyAuthenticationBackend> getFactory() {
            return LegacyComponentFactory.adapt(LDAPAuthenticationBackend2::new);
        }
    };    

}
