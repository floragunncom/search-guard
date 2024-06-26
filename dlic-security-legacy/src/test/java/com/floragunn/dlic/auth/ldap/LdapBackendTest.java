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

package com.floragunn.dlic.auth.ldap;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.TreeSet;

import com.floragunn.dlic.auth.ldap.util.Utils;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.ldaptive.Connection;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapEntry;

import com.floragunn.dlic.auth.ldap.backend.LDAPAuthenticationBackend;
import com.floragunn.dlic.auth.ldap.backend.LDAPAuthorizationBackend;
import com.floragunn.dlic.auth.ldap.srv.LdapServer;
import com.floragunn.dlic.auth.ldap.util.ConfigConstants;
import com.floragunn.dlic.auth.ldap.util.LdapHelper;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class LdapBackendTest {

    static {
        System.setProperty("sg.display_lic_none", "true");
    }

    private static LdapServer tlsLdapServer = LdapServer.createTls("base.ldif");
    private static LdapServer startTlsLdapServer = LdapServer.createStartTls("base.ldif");
    private static LdapServer plainTextLdapServer = LdapServer.createPlainText("base.ldif");


    @Test
    public void testLdapAuthentication() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationFakeLogin() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_FAKE_LOGIN_ENABLED, true)
                .build();

        new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("unknown").password("unknown").build());
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapInjection() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        String injectString = "*jack*";


        @SuppressWarnings("unused")
        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser(injectString).password("secret").build());
    }

    @Test
    public void testLdapAuthenticationBindDn() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_BIND_DN, "cn=Captain Spock,ou=people,o=TEST")
                .put(ConfigConstants.LDAP_PASSWORD, "spocksecret")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationWrongBindDn() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_BIND_DN, "cn=Captain Spock,ou=people,o=TEST")
                .put(ConfigConstants.LDAP_PASSWORD, "wrong")
                .build();

        new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationBindFail() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("wrong").build());
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationNoUser() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("UNKNOWN").password("UNKNOWN").build());
    }

    @Test(expected = ElasticsearchSecurityException.class)
    public void testLdapAuthenticationFail() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("xxxxx").build());
    }

    @Test
    public void testLdapAuthenticationSSL() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + tlsLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false)
                .put("path.home", ".")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationSSLPEMFile() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + tlsLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put(ConfigConstants.LDAPS_PEMTRUSTEDCAS_FILEPATH, FileHelper.getAbsoluteFilePathFromClassPath("ldap/root-ca.pem").toFile().getName())
                .put("verify_hostnames", false)
                .put("path.home", ".")
                .put("path.conf", FileHelper.getAbsoluteFilePathFromClassPath("ldap/root-ca.pem").getParent())
                .build();
        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, Paths.get("src/test/resources/ldap")).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
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
        Settings settings = Settings.builder().put(settingsFromFile).putList("hosts", "localhost:" + tlsLdapServer.getPort()).build();
        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationSSLSSLv3() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + tlsLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false)
                .putList("enabled_ssl_protocols", "SSLv3")
                .put("path.home", ".")
                .build();

        try {
            new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), org.ldaptive.LdapException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Unable to connec"));
        }

    }

    @Test
    public void testLdapAuthenticationSSLUnknowCipher() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + tlsLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false)
                .putList("enabled_ssl_ciphers", "AAA")
                .put("path.home", ".")
                .build();

        try {
            new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), org.ldaptive.LdapException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Unable to connec"));
        }

    }

    @Test
    public void testLdapAuthenticationSpecialCipherProtocol() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + tlsLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false)
                .putList("enabled_ssl_protocols", "TLSv1.2")
                .putList("enabled_ssl_ciphers", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256")
                .put("path.home", ".")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());

    }

    @Test
    public void testLdapAuthenticationSSLNoKeystore() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + tlsLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAPS_ENABLE_SSL, true)
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false)
                .put("path.home", ".")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthenticationSSLFailPlain() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + tlsLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAPS_ENABLE_SSL, true).build();

        try {
            new LDAPAuthenticationBackend(settings, null)
                    .authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        } catch (final Exception e) {
            Assert.assertEquals(org.ldaptive.LdapException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testLdapExists() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        final LDAPAuthenticationBackend lbe = new LDAPAuthenticationBackend(settings, null);
        Assert.assertTrue(lbe.exists(new User("jacksonm")));
        Assert.assertFalse(lbe.exists(new User("doesnotexist")));
    }

    @Test
    public void testLdapAuthorization() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                // .put("searchguard.authentication.authorization.ldap.userrolename",
                // "(uniqueMember={0})")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
        Assert.assertEquals(user.getName(), user.getUserEntry().getDN());
    }

    @Test
    public void testLdapAuthenticationReferral() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        final ConnectionConfig connectionConfig = LDAPAuthorizationBackend.getConnectionConfig(settings, null);
        final LdapEntry ref1 = LdapHelper.lookup(connectionConfig, "cn=Ref1,ou=people,o=TEST");
        Assert.assertNotNull(ref1);
        Assert.assertEquals("cn=refsolved,ou=people,o=TEST", ref1.getDn());

    }


    @Test
    public void testLdapEscape() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("ssign").password("ssignsecret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Special\\, Sign,ou=people,o=TEST", user.getName());
        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);
        Assert.assertEquals("cn=Special\\, Sign,ou=people,o=TEST", user.getName());
        Assert.assertEquals(4, user.getRoles().size());
        Assert.assertTrue(user.getRoles().toString().contains("ceo"));
    }

    @Test
    public void testLdapAuthorizationRoleSearchUsername() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(cn={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember=cn={1},ou=people,o=TEST)")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("Michael Jackson").password("secret").build());

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("Michael Jackson", user.getOriginalUsername());
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getUserEntry().getDN());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
        Assert.assertEquals(user.getName(), user.getUserEntry().getDN());
    }

    @Test
    public void testLdapAuthorizationOnly() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .build();

        final User user = new User("jacksonm");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("jacksonm", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
    }

    @Test
    public void testLdapAuthorizationNonDNEntry() throws Exception {

        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "description")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .build();

        final User user = new User("jacksonm");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("jacksonm", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo-ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
    }

    @Test
    public void testLdapAuthorizationNested() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(4, user.getRoles().size());
        Assert.assertEquals("nested1", new ArrayList<>(new TreeSet<>(user.getRoles())).get(1));
    }

    @Test
    public void testLdapAuthorizationNestedFilter() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .putList(ConfigConstants.LDAP_AUTHZ_NESTEDROLEFILTER, "cn=nested2,ou=groups,o=TEST")
                .build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
        Assert.assertEquals("nested2", new ArrayList<>(new TreeSet<>(user.getRoles())).get(1));
    }

    @Test
    public void testLdapAuthorizationDnNested() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "dn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(4, user.getRoles().size());
        Assert.assertEquals("cn=nested1,ou=groups,o=TEST", new ArrayList<>(new TreeSet<>(user.getRoles())).get(1));
    }

    @Test
    public void testLdapAuthorizationDn() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "dn")
                .put(ConfigConstants.LDAP_AUTHC_USERNAME_ATTRIBUTE, "UID")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, false)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .build();

        final User user = new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("jacksonm", user.getName());
        Assert.assertEquals(2, user.getRoles().size());
        Assert.assertEquals("cn=ceo,ou=groups,o=TEST", new ArrayList<>(new TreeSet<>(user.getRoles())).get(0));
    }

    @Test
    public void testLdapAuthenticationUserNameAttribute() throws Exception {


        final Settings settings = Settings.builder().putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST").put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERNAME_ATTRIBUTE, "uid").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("jacksonm", user.getName());
    }

    @Test
    public void testLdapAuthenticationStartTLS() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + startTlsLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAPS_ENABLE_START_TLS, true)
                .put(ConfigConstants.LDAP_BIND_DN, "cn=Michael Jackson,ou=people,o=TEST")
                .put(ConfigConstants.LDAP_PASSWORD, "secret")                
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ldap/truststore.jks"))
                .put("verify_hostnames", false).put("path.home", ".")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testLdapAuthorizationSkipUsers() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .putList(ConfigConstants.LDAP_AUTHZ_SKIP_USERS, "cn=Michael Jackson,ou*people,o=TEST")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
        Assert.assertEquals(0, user.getRoles().size());
        Assert.assertEquals(user.getName(), user.getUserEntry().getDN());
    }

    @Test
    public void testLdapAuthorizationSkipUsersNoDn() throws Exception {

        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .putList(ConfigConstants.LDAP_AUTHZ_SKIP_USERS, "jacksonm")
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
        Assert.assertEquals(0, user.getRoles().size());
        Assert.assertEquals(user.getName(), user.getUserEntry().getDN());
    }

    @Test
    public void testLdapAuthorizationNestedAttr() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true)
                .build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(8, user.getRoles().size());
        Assert.assertEquals("nested3", new ArrayList<>(new TreeSet<>(user.getRoles())).get(4));
        Assert.assertEquals("rolemo4", new ArrayList<>(new TreeSet<>(user.getRoles())).get(7));
    }

    @Test
    public void testLdapAuthorizationNestedAttrFilter() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true)
                .putList(ConfigConstants.LDAP_AUTHZ_NESTEDROLEFILTER, "cn=rolemo4*")
                .build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(6, user.getRoles().size());
        Assert.assertEquals("role2", new ArrayList<>(new TreeSet<>(user.getRoles())).get(4));
        Assert.assertEquals("nested1", new ArrayList<>(new TreeSet<>(user.getRoles())).get(2));

    }

    @Test
    public void testLdapAuthorizationNestedAttrFilterAll() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true)
                .putList(ConfigConstants.LDAP_AUTHZ_NESTEDROLEFILTER, "*")
                .build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(4, user.getRoles().size());

    }

    @Test
    public void testLdapAuthorizationNestedAttrFilterAllEqualsNestedFalse() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, false) //-> same like putList(ConfigConstants.LDAP_AUTHZ_NESTEDROLEFILTER, "*")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true)
                .build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(4, user.getRoles().size());

    }

    @Test
    public void testLdapAuthorizationNestedAttrNoRoleSearch() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "unused")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(((unused")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, false)
                .build();

        final User user = new User("spock");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("spock", user.getName());
        Assert.assertEquals(3, user.getRoles().size());
        Assert.assertEquals("nested3", new ArrayList<>(new TreeSet<>(user.getRoles())).get(1));
        Assert.assertEquals("rolemo4", new ArrayList<>(new TreeSet<>(user.getRoles())).get(2));
    }

    @Deprecated
    @Test
    public void testCustomAttributes() throws Exception {

        Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" + plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user.getName());
        Assert.assertEquals(user.getCustomAttributesMap().toString(), 16, user.getCustomAttributesMap().size());
        Assert.assertFalse(user.getCustomAttributesMap().toString(), user.getCustomAttributesMap().keySet().contains("attr.ldap.userpassword"));

        settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" +  plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_CUSTOM_ATTR_MAXVAL_LEN, 0)
                .build();

        user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());

        Assert.assertEquals(user.getCustomAttributesMap().toString(), 2, user.getCustomAttributesMap().size());

        settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "127.0.0.1:4", "localhost:" +  plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .putList(ConfigConstants.LDAP_CUSTOM_ATTR_WHITELIST, "*objectclass*", "entryParentId")
                .build();

        user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());

        Assert.assertEquals(user.getCustomAttributesMap().toString(), 2, user.getCustomAttributesMap().size());

    }

    @Test
    public void testLdapAuthorizationNonDNRoles() throws Exception {

        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" +  plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_USERROLENAME, "description, ou") // no memberOf OID
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH_ENABLED, true)
                .build();

        final User user = new User("nondnroles");

        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertNotNull(user);
        Assert.assertEquals("nondnroles", user.getName());
        Assert.assertEquals(5, user.getRoles().size());
        Assert.assertTrue("Roles do not contain non-LDAP role 'kibanauser'", user.getRoles().contains("kibanauser"));
        Assert.assertTrue("Roles do not contain non-LDAP role 'humanresources'", user.getRoles().contains("humanresources"));
        Assert.assertTrue("Roles do not contain LDAP role 'dummyempty'", user.getRoles().contains("dummyempty"));
        Assert.assertTrue("Roles do not contain non-LDAP role 'role2'", user.getRoles().contains("role2"));
        Assert.assertTrue("Roles do not contain non-LDAP role 'anotherrole' from second role name", user.getRoles().contains("anotherrole"));
    }

    @Test
    public void testLdapSpecial186() throws Exception {

        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" +  plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "description")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("spec186").password("spec186").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("CN=AA BB/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST", user.getName());
        Assert.assertEquals("AA BB/CC (DD) my, company end=with=whitespace ", Utils.getSingleStringValue(user.getUserEntry().getLdaptiveEntry().getAttribute("cn")));
        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertEquals(3, user.getRoles().size());
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLEx(186n) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186nn) consists of\\, special="));

        new LDAPAuthorizationBackend(settings, null).fillRoles(new User("spec186"), null);
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLEx(186n) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186nn) consists of\\, special="));

        new LDAPAuthorizationBackend(settings, null).fillRoles(new User("CN=AA BB/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST"), null);
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLEx(186n) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186nn) consists of\\, special="));

        new LDAPAuthorizationBackend(settings, null).fillRoles(new User("CN=AA BB\\/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST"), null);
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLEx(186n) consists of\\, special="));
        Assert.assertTrue(user.getRoles().toString().contains("ROLE/(186nn) consists of\\, special="));
    }

    @Test
    public void testLdapSpecial186_2() throws Exception {

        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" +  plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "dn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("spec186").password("spec186").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("CN=AA BB/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST", user.getName());
        Assert.assertEquals("AA BB/CC (DD) my, company end=with=whitespace ", Utils.getSingleStringValue(user.getUserEntry().getLdaptiveEntry().getAttribute("cn")));
        new LDAPAuthorizationBackend(settings, null).fillRoles(user, null);

        Assert.assertEquals(3, user.getRoles().size());
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186n) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186nn) consists of\\, special\\=chars\\ "));

        new LDAPAuthorizationBackend(settings, null).fillRoles(new User("spec186"), null);
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186n) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186nn) consists of\\, special\\=chars\\ "));


        new LDAPAuthorizationBackend(settings, null).fillRoles(new User("CN=AA BB/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST"), null);
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186n) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186nn) consists of\\, special\\=chars\\ "));

        new LDAPAuthorizationBackend(settings, null).fillRoles(new User("CN=AA BB\\/CC (DD) my\\, company end\\=with\\=whitespace\\ ,ou=people,o=TEST"), null);
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186n) consists of\\, special\\=chars\\ "));
        Assert.assertTrue(user.getRoles().toString().contains("cn=ROLE/(186nn) consists of\\, special\\=chars\\ "));
    }

    @Test
    public void testOperationalAttributes() throws Exception {


        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" +  plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("jacksonm").password("secret").build());
        Assert.assertNotNull(user);
        LdapAttribute operationAttribute = user.getUserEntry().getLdaptiveEntry().getAttribute("entryUUID");
        Assert.assertNotNull(operationAttribute);
        Assert.assertNotNull(operationAttribute.getStringValue());
        Assert.assertTrue(operationAttribute.getStringValue().length() > 10);
        Assert.assertTrue(operationAttribute.getStringValue().split("-").length == 5);
    }

    @Test
    public void testMultiCn() throws Exception {
        final Settings settings = Settings.builder()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" +  plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})")
                .put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})")
                .put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .build();

        final LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null).authenticate(AuthCredentials.forUser("multi").password("multi").build());
        Assert.assertNotNull(user);
        Assert.assertEquals("cn=cabc,ou=people,o=TEST", user.getName());
    }

    @Test
    public void testMultiValuedAttributes() throws Exception {
        final Settings settings = Settings.builder().putList(ConfigConstants.LDAP_HOSTS, "localhost:" +  plainTextLdapServer.getPort())
                .put(ConfigConstants.LDAP_AUTHC_USERSEARCH, "(uid={0})").put(ConfigConstants.LDAP_AUTHC_USERBASE, "ou=people,o=TEST")
                .put(ConfigConstants.LDAP_AUTHZ_ROLEBASE, "ou=groups,o=TEST").put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put(ConfigConstants.LDAP_AUTHZ_ROLESEARCH, "(uniqueMember={0})").put(ConfigConstants.LDAP_AUTHZ_RESOLVE_NESTED_ROLES, true)
                .put("map_ldap_attrs_to_user_attrs.mapped_mail", "mail").put("map_ldap_attrs_to_user_attrs.mapped_description", "description")
                .build();

        LdapUser user = (LdapUser) new LDAPAuthenticationBackend(settings, null)
                .authenticate(AuthCredentials.forUser("multivalued").password("multivalued").build());

        Assert.assertNotNull(user);

        Assert.assertThat((Iterable<?>) user.getStructuredAttributes().get("mapped_mail"),
                Matchers.containsInAnyOrder("multivalued1@example.com", "multivalued2@example.com", "multivalued3@example.com"));

        Assert.assertThat((Iterable<?>) user.getStructuredAttributes().get("mapped_description"),
                Matchers.containsInAnyOrder("cn=multivalued1,ou=groups,o=TEST", "cn=multivalued2,ou=groups,o=TEST"));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (tlsLdapServer != null) {
            tlsLdapServer.stop();
        }
        if (startTlsLdapServer != null) {
            startTlsLdapServer.stop();
        }
        if (plainTextLdapServer != null) {
            plainTextLdapServer.stop();
        }
    }
}
