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
/*
 * Contains code from https://github.com/opensearch-project/security/commit/6483d39678c7fe4b16ecd912f07256455cbd22dc#diff-8dac203b4ca3f3a2a0519881f2cd57ece64d19a0945b26476d7ae24495b11515
 *
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.floragunn.dlic.auth.ldap.backend;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
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
import org.ldaptive.BindConnectionInitializer;
import org.ldaptive.Connection;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.Credential;
import org.ldaptive.DefaultConnectionFactory;
import org.ldaptive.FilterTemplate;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapEntry;
import org.ldaptive.LdapException;
import org.ldaptive.SearchScope;
import org.ldaptive.ssl.AllowAnyHostnameVerifier;
import org.ldaptive.ssl.AllowAnyTrustManager;
import org.ldaptive.ssl.CredentialConfig;
import org.ldaptive.ssl.CredentialConfigFactory;
import org.ldaptive.ssl.SslConfig;

import com.floragunn.dlic.auth.ldap.LdapUser;
import com.floragunn.dlic.auth.ldap.util.ConfigConstants;
import com.floragunn.dlic.auth.ldap.util.LdapHelper;
import com.floragunn.dlic.auth.ldap.util.Utils;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.legacy.LegacyAuthorizationBackend;
import com.floragunn.searchguard.legacy.LegacyComponentFactory;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.PemKeyReader;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.HashMultimap;

public class LDAPAuthorizationBackend implements LegacyAuthorizationBackend {

    private static final List<String> DEFAULT_TLS_PROTOCOLS = Arrays.asList("TLSv1.2", "TLSv1.1");
    static final int ONE_PLACEHOLDER = 1;
    static final int TWO_PLACEHOLDER = 2;
    static final String DEFAULT_ROLEBASE = "";
    static final String DEFAULT_ROLESEARCH = "(member={0})";
    static final String DEFAULT_ROLENAME = "name";
    static final String DEFAULT_USERROLENAME = "memberOf";

    protected static final Logger log = LogManager.getLogger(LDAPAuthorizationBackend.class);
    private final Settings settings;
    private final Path configPath;
    private final List<Map.Entry<String, Settings>> roleBaseSettings;
    private final List<Map.Entry<String, Settings>> userBaseSettings;

    public LDAPAuthorizationBackend(final Settings settings, final Path configPath) {
        this.settings = settings;
        this.configPath = configPath;
        this.roleBaseSettings = getRoleSearchSettings(settings);
        this.userBaseSettings = LDAPAuthenticationBackend.getUserBaseSettings(settings);
    }
    
    public static void checkConnection(final ConnectionConfig connectionConfig, String bindDn, byte[] password) throws Exception {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                checkConnection0(connectionConfig, bindDn, password);
                return null;
            });
        } catch (PrivilegedActionException e) {
            throw e.getException();
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
    
    private static void checkConnection0(ConnectionConfig connectionConfig, String bindDn, byte[] password) throws LdapException {

        Connection connection = null;
        
        try {

            if (log.isDebugEnabled()) {
                log.debug("bindDn {}, password {}", bindDn, password != null && password.length > 0 ? "****" : "<not set>");
            }

            if (bindDn != null && (password == null || password.length == 0)) {
                throw new LdapException("no bindDn or no Password");
            }

            connectionConfig = ConnectionConfig.copy(connectionConfig);
            connectionConfig.setConnectionInitializers(new BindConnectionInitializer(bindDn, new Credential(password)));

            DefaultConnectionFactory connFactory = new DefaultConnectionFactory(connectionConfig);
            connection = connFactory.getConnection();

            connection.open();
        } finally {
            Utils.unbindAndCloseSilently(connection);
        }
    }

    public static ConnectionConfig getConnectionConfig(final Settings settings, final Path configPath) throws LdapException {
        final boolean enableSSL = settings.getAsBoolean(ConfigConstants.LDAPS_ENABLE_SSL, false);

        final List<String> ldapHosts = settings.getAsList(ConfigConstants.LDAP_HOSTS,
                Collections.singletonList("localhost"));

        Connection connection = null;
        Exception lastException = null;

        for (String ldapHost : ldapHosts) {

            if (log.isTraceEnabled()) {
                log.trace("Connect to {}", ldapHost);
            }

            try {

                final String[] split = ldapHost.split(":");

                int port;

                if (split.length > 1) {
                    port = Integer.parseInt(split[1]);
                } else {
                    port = enableSSL ? 636 : 389;
                }

                final ConnectionConfig.Builder configBuilder = ConnectionConfig.builder();
                configBuilder.url("ldap" + (enableSSL ? "s" : "") + "://" + split[0] + ":" + port);

                configureSSL(configBuilder, settings, configPath);

                final String bindDn = settings.get(ConfigConstants.LDAP_BIND_DN, null);
                final String password = settings.get(ConfigConstants.LDAP_PASSWORD, null);

                if (log.isDebugEnabled()) {
                    log.debug("bindDn {}, password {}", bindDn,
                            password != null && password.length() > 0 ? "****" : "<not set>");
                }

                if (bindDn != null && (password == null || password.length() == 0)) {
                    log.error("No password given for bind_dn {}. Will try to authenticate anonymously to ldap", bindDn);
                }

                final boolean enableClientAuth = settings.getAsBoolean(ConfigConstants.LDAPS_ENABLE_SSL_CLIENT_AUTH,
                        ConfigConstants.LDAPS_ENABLE_SSL_CLIENT_AUTH_DEFAULT);

                if (log.isDebugEnabled()) {
                    if (enableClientAuth && bindDn == null) {
                        log.debug("Will perform External SASL bind because client cert authentication is enabled");
                    } else if (bindDn == null) {
                        log.debug("Will perform anonymous bind because no bind dn is given");
                    } else if (enableClientAuth && bindDn != null) {
                        log.debug(
                                "Will perform simple bind with bind dn because to bind dn is given and overrides client cert authentication");
                    } else if (!enableClientAuth && bindDn != null) {
                        log.debug("Will perform simple bind with bind dn");
                    }
                }

                if (bindDn != null && password != null && password.length() > 0) {
                    configBuilder.connectionInitializers(new BindConnectionInitializer(bindDn, new Credential(password)));
                }

                DefaultConnectionFactory connFactory = new DefaultConnectionFactory(configBuilder.build());
                connection = connFactory.getConnection();
                connection.open();
                if (connection != null && connection.isOpen()) {
                    return configBuilder.build();
                } else {
                    Utils.unbindAndCloseSilently(connection);
                }
            } catch (final Exception e) {
                lastException = e;
                log.warn("Unable to connect to ldapserver {} due to {}. Try next.", ldapHost, e.toString());
                if (log.isDebugEnabled()) {
                    log.debug("Unable to connect to ldapserver due to ", e);
                }
                Utils.unbindAndCloseSilently(connection);
            }
        }

        if (connection == null || !connection.isOpen()) {
            Utils.unbindAndCloseSilently(connection);  //just in case
            connection = null;
            if (lastException == null) {
                throw new LdapException("Unable to connect to any of those ldap servers " + ldapHosts);
            } else {
                throw new LdapException(
                        "Unable to connect to any of those ldap servers " + ldapHosts + " due to " + lastException,
                        lastException);
            }
        }
        return null;
    }

    private static void configureSSL(final ConnectionConfig.Builder configBuilder, final Settings settings,
            final Path configPath) throws Exception {

        final boolean enableSSL = settings.getAsBoolean(ConfigConstants.LDAPS_ENABLE_SSL, false);
        final boolean enableStartTLS = settings.getAsBoolean(ConfigConstants.LDAPS_ENABLE_START_TLS, false);

        if (enableSSL || enableStartTLS) {

            final boolean enableClientAuth = settings.getAsBoolean(ConfigConstants.LDAPS_ENABLE_SSL_CLIENT_AUTH,
                    ConfigConstants.LDAPS_ENABLE_SSL_CLIENT_AUTH_DEFAULT);

            final boolean trustAll = settings.getAsBoolean(ConfigConstants.LDAPS_TRUST_ALL, false);

            final boolean verifyHostnames = !trustAll && settings.getAsBoolean(ConfigConstants.LDAPS_VERIFY_HOSTNAMES,
                    ConfigConstants.LDAPS_VERIFY_HOSTNAMES_DEFAULT);

            if (log.isDebugEnabled()) {
                log.debug("verifyHostname {}:", verifyHostnames);
                log.debug("trustall {}:", trustAll);
            }

            final boolean pem = settings.get(ConfigConstants.LDAPS_PEMTRUSTEDCAS_FILEPATH, null) != null
                    || settings.get(ConfigConstants.LDAPS_PEMTRUSTEDCAS_CONTENT, null) != null;

            final SslConfig sslConfig = new SslConfig();
            CredentialConfig cc;

            if (pem) {
                X509Certificate[] trustCertificates = PemKeyReader.loadCertificatesFromStream(
                        PemKeyReader.resolveStream(ConfigConstants.LDAPS_PEMTRUSTEDCAS_CONTENT, settings));

                if (trustCertificates == null) {
                    trustCertificates = PemKeyReader.loadCertificatesFromFile(PemKeyReader
                            .resolve(ConfigConstants.LDAPS_PEMTRUSTEDCAS_FILEPATH, settings, configPath, !trustAll));
                }
                // for client authentication
                X509Certificate authenticationCertificate = PemKeyReader.loadCertificateFromStream(
                        PemKeyReader.resolveStream(ConfigConstants.LDAPS_PEMCERT_CONTENT, settings));

                if (authenticationCertificate == null) {
                    authenticationCertificate = PemKeyReader.loadCertificateFromFile(PemKeyReader
                            .resolve(ConfigConstants.LDAPS_PEMCERT_FILEPATH, settings, configPath, enableClientAuth));
                }

                PrivateKey authenticationKey = PemKeyReader.loadKeyFromStream(
                        settings.get(ConfigConstants.LDAPS_PEMKEY_PASSWORD),
                        PemKeyReader.resolveStream(ConfigConstants.LDAPS_PEMKEY_CONTENT, settings));

                if (authenticationKey == null) {
                    authenticationKey = PemKeyReader
                            .loadKeyFromFile(settings.get(ConfigConstants.LDAPS_PEMKEY_PASSWORD), PemKeyReader.resolve(
                                    ConfigConstants.LDAPS_PEMKEY_FILEPATH, settings, configPath, enableClientAuth));
                }

                cc = CredentialConfigFactory.createX509CredentialConfig(trustCertificates, authenticationCertificate,
                        authenticationKey);

                if (log.isDebugEnabled()) {
                    log.debug("Use PEM to secure communication with LDAP server (client auth is {})",
                            authenticationKey != null);
                }

            } else {
                final KeyStore trustStore = PemKeyReader.loadKeyStore(
                        PemKeyReader.resolve(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, settings,
                                configPath, !trustAll),
                        settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD,
                                SSLConfigConstants.DEFAULT_STORE_PASSWORD),
                        settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_TYPE));

                final List<String> trustStoreAliases = settings.getAsList(ConfigConstants.LDAPS_JKS_TRUST_ALIAS, null);

                // for client authentication
                final KeyStore keyStore = PemKeyReader.loadKeyStore(
                        PemKeyReader.resolve(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, settings,
                                configPath, enableClientAuth),
                        settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD,
                                SSLConfigConstants.DEFAULT_STORE_PASSWORD),
                        settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_TYPE));
                final String keyStorePassword = settings.get(
                        SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD,
                        SSLConfigConstants.DEFAULT_STORE_PASSWORD);

                final String keyStoreAlias = settings.get(ConfigConstants.LDAPS_JKS_CERT_ALIAS, null);
                final String[] keyStoreAliases = keyStoreAlias == null ? null : new String[] { keyStoreAlias };

                if (enableClientAuth && keyStoreAliases == null) {
                    throw new IllegalArgumentException(ConfigConstants.LDAPS_JKS_CERT_ALIAS + " not given");
                }

                if (log.isDebugEnabled()) {
                    log.debug("Use Trust-/Keystore to secure communication with LDAP server (client auth is {})",
                            keyStore != null);
                    log.debug("trustStoreAliases: {}, keyStoreAlias: {}", trustStoreAliases, keyStoreAlias);
                }

                cc = CredentialConfigFactory.createKeyStoreCredentialConfig(trustStore,
                        trustStoreAliases == null ? null : trustStoreAliases.toArray(new String[0]), keyStore,
                        keyStorePassword, keyStoreAliases);

            }

            sslConfig.setCredentialConfig(cc);

            if (trustAll) {
                sslConfig.setTrustManagers(new AllowAnyTrustManager());
            }

            if (!verifyHostnames) {
                sslConfig.setHostnameVerifier(new AllowAnyHostnameVerifier());
            }

            // https://github.com/floragunncom/search-guard/issues/227
            final List<String> enabledCipherSuites = settings.getAsList(ConfigConstants.LDAPS_ENABLED_SSL_CIPHERS,
                    Collections.emptyList());
            final List<String> enabledProtocols = settings.getAsList(ConfigConstants.LDAPS_ENABLED_SSL_PROTOCOLS,
                    DEFAULT_TLS_PROTOCOLS);

            if (!enabledCipherSuites.isEmpty()) {
                sslConfig.setEnabledCipherSuites(enabledCipherSuites.toArray(new String[0]));
                log.debug("enabled ssl cipher suites for ldaps {}", enabledCipherSuites);
            }

            log.debug("enabled ssl/tls protocols for ldaps {}", enabledProtocols);
            sslConfig.setEnabledProtocols(enabledProtocols.toArray(new String[0]));
            configBuilder.sslConfig(sslConfig);
        }

        configBuilder.useStartTLS(enableStartTLS);

        final long connectTimeout = settings.getAsLong(ConfigConstants.LDAP_CONNECT_TIMEOUT, 5000L); // 0L means TCP
                                                                                                     // default timeout
        final long responseTimeout = settings.getAsLong(ConfigConstants.LDAP_RESPONSE_TIMEOUT, 0L); // 0L means wait
                                                                                                    // infinitely

        configBuilder.connectTimeout(Duration.ofMillis(connectTimeout < 0L ? 0L : connectTimeout)); // 5 sec by default
        configBuilder.responseTimeout(Duration.ofMillis(responseTimeout < 0L ? 0L : responseTimeout));
    }

    @Override
    public void fillRoles(final User user, final AuthCredentials optionalAuthCreds)
            throws ElasticsearchSecurityException {

        if (user == null) {
            return;
        }

        String authenticatedUser;
        String originalUserName;
        LdapEntry entry = null;
        String dn = null;
        
        if(log.isDebugEnabled())
        log.debug("DBGTRACE (2): username="+user.getName()+" -> "+Arrays.toString(user.getName().getBytes(StandardCharsets.UTF_8)));

        if (user instanceof LdapUser && ((LdapUser) user).getUserEntry() != null && ((LdapUser) user).getUserEntry().getLdaptiveEntry() != null) {
            entry = ((LdapUser) user).getUserEntry().getLdaptiveEntry();
            dn = entry.getDn();
            authenticatedUser = entry.getDn();
            originalUserName = ((LdapUser) user).getOriginalUsername();
        } else {
            authenticatedUser = user.getName();
            originalUserName = user.getName();
        }
        
        if(log.isDebugEnabled())
        log.debug("DBGTRACE (3): authenticatedUser="+authenticatedUser+" -> "+Arrays.toString(authenticatedUser.getBytes(StandardCharsets.UTF_8)));

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

        ConnectionConfig connectionConfig = null;

        try {

            connectionConfig = getConnectionConfig(settings, configPath);
            
            if (entry == null || dn == null) {

                if (isValidDn(authenticatedUser)) {
                    // assume dn
                    if (log.isTraceEnabled()) {
                        log.trace("{} is a valid DN", authenticatedUser);
                    }
                    
                    if(log.isDebugEnabled())
                    log.debug("DBGTRACE (4): authenticatedUser="+authenticatedUser+" -> "+Arrays.toString(authenticatedUser.getBytes(StandardCharsets.UTF_8)));


                    entry = LdapHelper.lookup(connectionConfig, authenticatedUser);

                    if (entry == null) {
                        throw new ElasticsearchSecurityException("No user '" + authenticatedUser + "' found");
                    }

                } else {
                    
                    if(log.isDebugEnabled())
                    log.debug("DBGTRACE (5): authenticatedUser="+user.getName()+" -> "+Arrays.toString(user.getName().getBytes(StandardCharsets.UTF_8)));

                    
                    entry = LDAPAuthenticationBackend.exists(user.getName(), connectionConfig, settings, userBaseSettings);

                    if (log.isTraceEnabled()) {
                        log.trace("{} is not a valid DN and was resolved to {}", authenticatedUser, entry);
                    }

                    if (entry == null || entry.getDn() == null) {
                        throw new ElasticsearchSecurityException("No user " + authenticatedUser + " found");
                    }
                }

                dn = entry.getDn();

                if (log.isTraceEnabled()) {
                    log.trace("User found with DN {}", dn);
                }
                
                if(log.isDebugEnabled())
                log.debug("DBGTRACE (6): dn"+dn+" -> "+Arrays.toString(dn.getBytes(StandardCharsets.UTF_8)));

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
                    final Collection<String> userRoles = entry.getAttribute(roleName).getStringValues();
                    for (final String possibleRoleDN : userRoles) {
                        
                        if(log.isDebugEnabled())
                        log.debug("DBGTRACE (7): possibleRoleDN"+possibleRoleDN);
                        
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
            final LdapAttribute userRoleAttribute = entry.getAttribute(userRoleAttributeName);

            if (userRoleAttribute != null) {
                userRoleAttributeValue = Utils.getSingleStringValue(userRoleAttribute);
            }

            if (rolesearchEnabled) {
                String escapedDn = dn;
                
                if(log.isDebugEnabled())
                log.debug("DBGTRACE (8): escapedDn"+escapedDn);

                for (Map.Entry<String, Settings> roleSearchSettingsEntry : roleBaseSettings) {
                    Settings roleSearchSettings = roleSearchSettingsEntry.getValue();

                    FilterTemplate f = new FilterTemplate();
                    f.setFilter(roleSearchSettings.get(ConfigConstants.LDAP_AUTHCZ_SEARCH, DEFAULT_ROLESEARCH));
                    f.setParameter(LDAPAuthenticationBackend.ZERO_PLACEHOLDER, escapedDn);
                    f.setParameter(ONE_PLACEHOLDER, originalUserName);
                    f.setParameter(TWO_PLACEHOLDER,
                            userRoleAttributeValue == null ? TWO_PLACEHOLDER : userRoleAttributeValue);

                    List<LdapEntry> rolesResult = LdapHelper.search(connectionConfig,
                            roleSearchSettings.get(ConfigConstants.LDAP_AUTHCZ_BASE, DEFAULT_ROLEBASE),
                            f,
                            SearchScope.SUBTREE);

                    if (log.isTraceEnabled()) {
                        log.trace("Results for LDAP group search for " + escapedDn + " in base "
                                + roleSearchSettingsEntry.getKey() + ":\n" + rolesResult);
                    }

                    if (rolesResult != null && !rolesResult.isEmpty()) {
                        for (final Iterator<LdapEntry> iterator = rolesResult.iterator(); iterator.hasNext();) {
                            LdapEntry searchResultEntry = iterator.next();
                            LdapName ldapName = new LdapName(searchResultEntry.getDn());
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

                    final Set<LdapName> nestedRoles = resolveNestedRoles(roleLdapName, connectionConfig, userRoleNames, 0,
                            rolesearchEnabled, nameRoleSearchBaseKeys, nestedRoleFilter);

                    if (log.isTraceEnabled()) {
                        log.trace("{} nested roles for {}", nestedRoles.size(), roleLdapName);
                    }

                    nestedReturn.addAll(nestedRoles);
                }

                for (final LdapName roleLdapName : nestedReturn) {
                    final String role = getRoleFromEntry(connectionConfig, roleLdapName, roleName);

                    if (!Strings.isNullOrEmpty(role)) {
                        user.addRole(role);
                    } else {
                        log.warn("No or empty attribute '{}' for entry {}", roleName, roleLdapName);
                    }
                }

            } else {
                // DN roles, extract rolename according to config
                for (final LdapName roleLdapName : ldapRoles) {
                    final String role = getRoleFromEntry(connectionConfig, roleLdapName, roleName);

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

    protected Set<LdapName> resolveNestedRoles(final LdapName roleDn, final ConnectionConfig connectionConfig,
            String userRoleName, int depth, final boolean rolesearchEnabled,
            Set<Map.Entry<String, Settings>> roleSearchBaseSettingsSet, final List<String> roleFilter)
            throws ElasticsearchSecurityException, LdapException {

        if (!roleFilter.isEmpty() && WildcardMatcher.matchAny(roleFilter, roleDn.toString())) {

            if (log.isTraceEnabled()) {
                log.trace("Filter nested role {}", roleDn);
            }

            return Collections.emptySet();
        }

        depth++;

        final Set<LdapName> result = new HashSet<>(20);
        final HashMultimap<LdapName, Map.Entry<String, Settings>> resultRoleSearchBaseKeys = HashMultimap.create();

        final LdapEntry e0 = LdapHelper.lookup(connectionConfig, roleDn);

        if (e0.getAttribute(userRoleName) != null) {
            final Collection<String> userRoles = e0.getAttribute(userRoleName).getStringValues();

            for (final String possibleRoleDN : userRoles) {
                
                if(log.isDebugEnabled())
                log.debug("DBGTRACE (10): possibleRoleDN"+possibleRoleDN);
                
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
            
            if(log.isDebugEnabled())
            log.debug("DBGTRACE (10): escapedDn"+escapedDn);

            for (Map.Entry<String, Settings> roleSearchBaseSettingsEntry : Utils
                    .getOrderedBaseSettings(roleSearchBaseSettingsSet)) {
                Settings roleSearchSettings = roleSearchBaseSettingsEntry.getValue();

                FilterTemplate f = new FilterTemplate();
                f.setFilter(roleSearchSettings.get(ConfigConstants.LDAP_AUTHCZ_SEARCH, DEFAULT_ROLESEARCH));
                f.setParameter(LDAPAuthenticationBackend.ZERO_PLACEHOLDER, escapedDn);
                f.setParameter(ONE_PLACEHOLDER, escapedDn);

                List<LdapEntry> foundEntries = LdapHelper.search(connectionConfig,
                        roleSearchSettings.get(ConfigConstants.LDAP_AUTHCZ_BASE, DEFAULT_ROLEBASE),
                        f,
                        SearchScope.SUBTREE);

                if (log.isTraceEnabled()) {
                    log.trace("Results for LDAP group search for " + escapedDn + " in base "
                            + roleSearchBaseSettingsEntry.getKey() + ":\n" + foundEntries);
                }

                if (foundEntries != null) {
                    for (final LdapEntry entry : foundEntries) {
                        try {
                            final LdapName dn = new LdapName(entry.getDn());
                            result.add(dn);
                            resultRoleSearchBaseKeys.put(dn, roleSearchBaseSettingsEntry);
                        } catch (final InvalidNameException e) {
                            throw new LdapException(e);
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

                final Set<LdapName> in = resolveNestedRoles(nm, connectionConfig, userRoleName, depth, rolesearchEnabled,
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

    private String getRoleFromEntry(final ConnectionConfig connectionConfig, final LdapName ldapName, final String role) {

        if (ldapName == null || Strings.isNullOrEmpty(role)) {
            return null;
        }

        if("dn".equalsIgnoreCase(role)) {
            return ldapName.toString();
        }

        try {
            final LdapEntry roleEntry = LdapHelper.lookup(connectionConfig, ldapName);

            if(roleEntry != null) {
                final LdapAttribute roleAttribute = roleEntry.getAttribute(role);
                if(roleAttribute != null) {
                    return Utils.getSingleStringValue(roleAttribute);
                }
            }
        } catch (LdapException e) {
            log.error("Unable to handle role {} because of ",ldapName, e.toString(), e);
        }

        return null;
    }
    
    public static TypedComponent.Info<LegacyAuthorizationBackend> INFO = new TypedComponent.Info<LegacyAuthorizationBackend>() {

        @Override
        public Class<LegacyAuthorizationBackend> getType() {
            return LegacyAuthorizationBackend.class;
        }

        @Override
        public String getName() {
            return "ldap";
        }

        @Override
        public Factory<LegacyAuthorizationBackend> getFactory() {
            return LegacyComponentFactory.adapt(LDAPAuthorizationBackend::new);
        }
    };   
}
