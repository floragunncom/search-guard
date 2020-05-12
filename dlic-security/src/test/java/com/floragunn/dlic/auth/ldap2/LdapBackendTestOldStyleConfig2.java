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

import java.io.File;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.dlic.auth.ldap.LdapUser;
import com.floragunn.dlic.auth.ldap.srv.EmbeddedLDAPServer;
import com.floragunn.dlic.auth.ldap.util.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.SearchResultEntry;

@RunWith(Parameterized.class)
public class LdapBackendTestOldStyleConfig2 {

    static {
        System.setProperty("sg.display_lic_none", "true");
        //System.setProperty("com.unboundid.ldap.sdk.debug.enabled", "true");
        //System.setProperty("com.unboundid.ldap.sdk.debug.type", "CONNECT");
        //System.setProperty("com.unboundid.ldap.sdk.debug.level", "FINEST");
    }

    private static EmbeddedLDAPServer ldapServer = null;

    private static int ldapPort;
    private static int ldapsPort;
    
    @BeforeClass
    public static void startLdapServer() throws Exception {
        ldapServer = new EmbeddedLDAPServer();
        ldapServer.start();
        ldapServer.applyLdif("base.ldif");
        ldapPort = ldapServer.getLdapPort();
        ldapsPort = ldapServer.getLdapsPort();
    }

    @Parameters
    public static Object[] parameters() {
        return new Object[] { Boolean.FALSE, Boolean.TRUE };
    }
    
    protected Settings healthCheckSettings() {
        return Settings.builder()
                .put("pool.health_check.enabled", true)
                .put("pool.health_check.interval_millis", 5L)
                .put("pool.health_check.validation.max_response_time", 300L)
                .put("pool.health_check.validation.on_create", true)
                .put("pool.health_check.validation.on_release", true)
                .put("pool.health_check.validation.on_exception", true)
                .put("pool.health_check.pruning.enabled", true).build();
    }

    protected Settings.Builder createBaseSettings() {
        if (healthCheckEnabled) {
            return Settings.builder()
                    .put(healthCheckSettings());
        } else {
            return Settings.builder();
        }
    }

    @Parameter
    public boolean healthCheckEnabled;

    @Test
    public void testLdapAuthentication() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationWithHealthChecks() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(healthCheckSettings())
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationFakeLogin() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_FAKE_LOGIN_ENABLED, true).build();

        new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("unknown", "unknown".getBytes(StandardCharsets.UTF_8)));
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapInjection() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        String injectString = "*jack*";

        @SuppressWarnings("unused")
        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials(injectString, "secret".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testLdapAuthenticationBindDn() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_BIND_DN, "cn=Captain Spock,ou=people,o=TEST")
                .put(ConfigConstants.LDAP_PASSWORD, "spocksecret").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationWrongBindDn() throws Exception {
        try {
            final Settings settings = createBaseSettings()
                    .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                    .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                    .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                    .put(ConfigConstants.LDAP_BIND_DN, "cn=Captain Spock,ou=people,o=TEST")
                    .put(ConfigConstants.LDAP_PASSWORD, "wrong").build();

            new LDAPAuthenticationBackend2(settings, null)
                    .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
            Assert.fail("Expected exception");
        } catch (Exception e) {
            Assert.assertTrue(ExceptionUtils.getStackTrace(e), ExceptionUtils.getStackTrace(e).contains("password was incorrect"));
        }
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationBindFail() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "wrong".getBytes(StandardCharsets.UTF_8)));
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationNoUser() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("UNKNOWN", "UNKNOWN".getBytes(StandardCharsets.UTF_8)));
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationFail() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "xxxxx".getBytes(StandardCharsets.UTF_8)));
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationFailWithHealthChecks() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(healthCheckSettings())
                .build();

        new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "xxxxx".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testLdapAuthenticationSSL() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapsPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false).put("path.home", ".").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationSSLWithHealthChecks() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapsPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put(healthCheckSettings())
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false).put("path.home", ".").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationSSLPEMFile() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapsPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put(ConfigConstants.LDAPS_PEMTRUSTEDCAS_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("ldap/root-ca.pem").toFile().getName())
                .put("verify_hostnames", false).put("path.home", ".")
                .put("path.conf", FileHelper.getAbsoluteFilePathFromClassPath("ldap/root-ca.pem").getParent()).build();
        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, Paths.get("src/test/resources/ldap"))
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationSSLPEMText() throws Exception {

        final Settings settingsFromFile = Settings
                .builder()
                .loadFromPath(
                        Paths
                        .get(FileHelper
                                .getAbsoluteFilePathFromClassPath("ldap/test1.yml")
                                .toFile()
                                .getAbsolutePath()))
                .build();
        Settings settings = Settings.builder().put(settingsFromFile).putList("hosts", "localhost:"+ldapsPort).build();
        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationSSLSSLv3() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapsPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false).putList("enabled_ssl_protocols", "SSLv3").put("path.home", ".").build();

        try {
            new LDAPAuthenticationBackend2(settings, null)
                    .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
            Assert.fail("Expected Exception");
        } catch (Exception e) {
            //Assert.assertEquals(org.ldaptive.provider.ConnectionException.class, e.getCause().getClass());
            Assert.assertTrue(ExceptionUtils.getStackTrace(e).contains("No appropriate protocol"));
        }

    }

    @Test
    public void testLdapAuthenticationSSLUnknowCipher() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapsPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false).putList("enabled_ssl_ciphers", "AAA").put("path.home", ".").build();

        try {
            new LDAPAuthenticationBackend2(settings, null)
                    .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
            Assert.fail("Expected Exception");
        } catch (Exception e) {
            //Assert.assertEquals(e.getCause().getClass().toString(), org.ldaptive.provider.ConnectionException.class, e.getCause().getClass());
            Assert.assertTrue(ExceptionUtils.getStackTrace(e), WildcardMatcher.match("*unsupported*ciphersuite*aaa*", ExceptionUtils.getStackTrace(e).toLowerCase()));
        }

    }

    @Test
    public void testLdapAuthenticationSpecialCipherProtocol() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapsPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false).putList("enabled_ssl_protocols", "TLSv1")
                .putList("enabled_ssl_ciphers", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA").put("path.home", ".").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());

    }

    @Test
    public void testLdapAuthenticationSSLNoKeystore() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapsPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false).put("path.home", ".").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationSSLFailPlain() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .build();

        try {
            new LDAPAuthenticationBackend2(settings, new File("").toPath())
                    .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
            Assert.fail("Expected exception");
        } catch (final Exception e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testLdapExists() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        final LDAPAuthenticationBackend2 lbe = new LDAPAuthenticationBackend2(settings, null);
        Assert.assertTrue(lbe.exists(new User("jacksonm")));
        Assert.assertFalse(lbe.exists(new User("doesnotexist")));
    }

    @Test
    public void testLdapAuthorization() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                // .put("searchguard.authentication.authorization.ldap.userrolename",
                // "(uniqueMember={0})")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
        Assert.assertEquals(user.getName(), user.getUserEntry().getDN());
    }

    @Test
    public void testLdapAuthorizationWithHealthChecks() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(healthCheckSettings())
                // .put("searchguard.authentication.authorization.ldap.userrolename",
                // "(uniqueMember={0})")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
        Assert.assertEquals(user.getName(), user.getUserEntry().getDN());
    }

    @Test
    public void testLdapAuthenticationReferral() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        try(LDAPConnectionManager conMngt = new LDAPConnectionManager(settings, null)){
            try(LDAPConnection con = conMngt.getConnection()) {
                final SearchResultEntry ref1 = conMngt.lookup(con, "cn=Ref1,ou=people,o=TEST");
                Assert.assertEquals("cn=refsolved,ou=people,o=TEST", ref1.getDN());
            }
        }

    }

    @Test
    public void testLdapEscape() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true).build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("ssign", "ssignsecret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Special\\, Sign,ou=people,o=TEST", user.getName());
        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);
        Assert.assertEquals("cn=Special\\, Sign,ou=people,o=TEST", user.getName());
        Assert.assertEquals(4, user.getRoles().size());
        Assert.assertTrue(user.getRoles().toString().contains("ceo"));
    }

    @Test
    public void testLdapAuthorizationRoleSearchUsername() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(cn={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember=cn={1},ou=people,o=TEST)").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("Michael Jackson", "secret".getBytes(StandardCharsets.UTF_8)));

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("Michael Jackson", user.getOriginalUsername());
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getUserEntry().getDN());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
        Assert.assertEquals(user.getName(), user.getUserEntry().getDN());
    }

    @Test
    public void testLdapAuthorizationOnly() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})").build();

        final User user = new User("jacksonm");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("jacksonm", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
    }

    @Test
    public void testLdapAuthorizationNested() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})").build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(4, user.getRoles().size());
        Assert.assertEquals("nested1", new ArrayList<>(new TreeSet<>(user.getRoles())).get(1));
    }

    @Test
    public void testLdapAuthorizationNestedFilter() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .putList(ConfigConstants.LDAP_AUTHZ_NESTEDROLEFILTER, "cn=nested2,ou=groups,o=TEST").build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
        Assert.assertEquals("nested2", new ArrayList<>(new TreeSet<>(user.getRoles())).get(1));
    }

    @Test
    public void testLdapAuthorizationDnNested() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "dn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})").build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(4, user.getRoles().size());
        Assert.assertEquals("cn=nested1,ou=groups,o=TEST", new ArrayList<>(new TreeSet<>(user.getRoles())).get(1));
    }

    @Test
    public void testLdapAuthorizationDn() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "dn")
                .put(ConfigConstants.LDAP_AUTHC_USERNAME_ATTRIBUTE, "UID")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, false)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})").build();

        final User user = new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes()));

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("jacksonm", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("cn=ceo,ou=groups,o=TEST", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
    }

    @Test
    public void testLdapAuthenticationUserNameAttribute() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERNAME_ATTRIBUTE, "uid").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("jacksonm", user.getName());
    }

    @Test
    public void testLdapAuthenticationStartTLS() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAPS_ENABLE_START_TLS, true)
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false).put("path.home", ".").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthorizationSkipUsers() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .putList(ConfigConstants.LDAP_AUTHZ_SKIP_USERS, "cn=Michael Jackson,ou*people,o=TEST").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
        Assert.assertEquals(0, user.getRoles().size());
        Assert.assertEquals(user.getName(), user.getUserEntry().getDN());
    }

    @Test
    public void testLdapAuthorizationNestedAttr() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true).build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(8, user.getRoles().size());
        Assert.assertEquals("nested3", new ArrayList<>(new TreeSet<>(user.getRoles())).get(4));
        Assert.assertEquals("rolemo4", new ArrayList<>(new TreeSet<>(user.getRoles())).get(7));
    }

    @Test
    public void testLdapAuthorizationNestedAttrFilter() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true)
                .putList(ConfigConstants.LDAP_AUTHZ_NESTEDROLEFILTER, "cn=rolemo4*").build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(6, user.getRoles().size());
        Assert.assertEquals("role2", new ArrayList<>(new TreeSet<>(user.getRoles())).get(4));
        Assert.assertEquals("nested1", new ArrayList<>(new TreeSet<>(user.getRoles())).get(2));

    }

    @Test
    public void testLdapAuthorizationNestedAttrFilterAll() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true)
                .putList(ConfigConstants.LDAP_AUTHZ_NESTEDROLEFILTER, "*").build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(4, user.getRoles().size());

    }

    @Test
    public void testLdapAuthorizationNestedAttrFilterAllEqualsNestedFalse() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, false) // -> same like
                                                                             // putList(ConfigConstants.LDAP_AUTHZ_NESTEDROLEFILTER,
                                                                             // "*")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true).build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(4, user.getRoles().size());

    }

    @Test
    public void testLdapAuthorizationNestedAttrNoRoleSearch() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "unused").put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(((unused")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, false).build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(3, user.getRoles().size());
        Assert.assertEquals("nested3", new ArrayList<>(new TreeSet<>(user.getRoles())).get(1));
        Assert.assertEquals("rolemo4", new ArrayList<>(new TreeSet<>(user.getRoles())).get(2));
    }

    @Test
    public void testCustomAttributes() throws Exception {

        Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
        Assert.assertEquals(user.getCustomAttributesMap().toString(), 16, user.getCustomAttributesMap().size());
        Assert.assertFalse(user.getCustomAttributesMap().toString(),
                user.getCustomAttributesMap().keySet().contains("attr.ldap.userpassword"));

        settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_CUSTOM_ATTR_MAXVAL_LEN, 0).build();

        user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));

        Assert.assertEquals(user.getCustomAttributesMap().toString(), 2, user.getCustomAttributesMap().size());

        settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .putList(ConfigConstants.LDAP_CUSTOM_ATTR_WHITELIST, "*objectclass*", "entryParentId").build();

        user = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));

        Assert.assertEquals(user.getCustomAttributesMap().toString(), 2, user.getCustomAttributesMap().size());

    }

    @Test
    public void testLdapAuthorizationNonDNRoles() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description, ou") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true).build();

        final User user = new User("nondnroles");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("nondnroles", user.getName());
        Assert.assertEquals(5, user.getRoles().size());
        Assert.assertTrue("Roles do not contain non-LDAP role 'kibanauser'", user.getRoles().contains("kibanauser"));
        Assert.assertTrue("Roles do not contain non-LDAP role 'humanresources'",
                user.getRoles().contains("humanresources"));
        Assert.assertTrue("Roles do not contain LDAP role 'dummyempty'", user.getRoles().contains("dummyempty"));
        Assert.assertTrue("Roles do not contain non-LDAP role 'role2'", user.getRoles().contains("role2"));
        Assert.assertTrue("Roles do not contain non-LDAP role 'anotherrole' from second role name",
                user.getRoles().contains("anotherrole"));
    }
    
    @Test
    public void testLdapAuthorizationNonDNEntry() throws Exception {

        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "description")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .build();

        final User user = new User("jacksonm");

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("jacksonm", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo-ceo", new ArrayList(new TreeSet(user.getRoles())).get(0));
    }
    
    @Test
    public void testLdapSpecial186() throws Exception {

        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "description")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null).authenticate(new AuthCredentials("spec186", "spec186"
                .getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("CN=AA BB/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST", user.getName());
        //Assert.assertEquals("AA BB/CC (DD) my, company end=with=whitespace ", user.getUserEntry().getUbEntry().getAttribute("cn").getValue());
        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertEquals(3, user.getRoles().size());
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLEx(186n) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186nn) consists of\\, special="));
        
        new LDAPAuthorizationBackend2(settings, null).fillRoles(new User("spec186"), null);
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLEx(186n) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186nn) consists of\\, special="));
        
        new LDAPAuthorizationBackend2(settings, null).fillRoles(new User("CN=AA BB/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST"), null);
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLEx(186n) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186nn) consists of\\, special="));
        
        new LDAPAuthorizationBackend2(settings, null).fillRoles(new User("CN=AA BB\\/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST"), null);
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLEx(186n) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186nn) consists of\\, special="));
    }
    
    @Test
    public void testLdapSpecial186_2() throws Exception {

        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "dn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null).authenticate(new AuthCredentials("spec186", "spec186"
                .getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Assert.assertEquals("CN=AA BB/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST", user.getName());
        //Assert.assertEquals("AA BB/CC (DD) my, company end=with=whitespace ", user.getUserEntry().getUbEntry().getAttribute("cn").getValue());
        new LDAPAuthorizationBackend2(settings, null).fillRoles(user, null);

        Assert.assertEquals(3, user.getRoles().size());
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186n) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186nn) consists of\\, special\\=chars\\ "));
        
        new LDAPAuthorizationBackend2(settings, null).fillRoles(new User("spec186"), null);
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186n) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186nn) consists of\\, special\\=chars\\ "));

        
        new LDAPAuthorizationBackend2(settings, null).fillRoles(new User("CN=AA BB/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST"), null);
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186n) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186nn) consists of\\, special\\=chars\\ "));
        
        new LDAPAuthorizationBackend2(settings, null).fillRoles(new User("CN=AA BB\\/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST"), null);
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186n) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186nn) consists of\\, special\\=chars\\ "));
    }
    
    @Test
    public void testOperationalAttributes() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend2(settings, null).authenticate(new AuthCredentials("jacksonm", "secret"
                .getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull(user);
        Attribute operationAttribute = user.getUserEntry().getUbEntry().getAttribute("entryUUID");
        Assert.assertNotNull(operationAttribute);
        Assert.assertNotNull(operationAttribute.getValue());
        Assert.assertTrue(operationAttribute.getValue().length() > 10);
        Assert.assertTrue(operationAttribute.getValue().split("-").length == 5);
    }

    @AfterClass
    public static void tearDown() throws Exception {

        if (ldapServer != null) {
            ldapServer.stop();
        }

    }
}
