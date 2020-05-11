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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.TreeSet;

import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.floragunn.dlic.auth.ldap.LdapUser;
import com.floragunn.dlic.auth.ldap.backend.LDAPAuthenticationBackend;
import com.floragunn.dlic.auth.ldap.backend.LDAPAuthorizationBackend;
import com.floragunn.dlic.auth.ldap.srv.EmbeddedLDAPServer;
import com.floragunn.dlic.auth.ldap.util.ConfigConstants;
import com.floragunn.searchguard.user.AuthCredentials;

public class LdapBackendInterop {

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
        ldapServer.applyLdif("base.ldif","base2.ldif");
        ldapPort = ldapServer.getLdapPort();
        ldapsPort = ldapServer.getLdapsPort();
    }

    protected Settings.Builder createBaseSettings() {
        return Settings.builder();
    }

    @Test
    public void testLdapAuthorizationInterop() throws Exception {

        final Settings settings = createBaseSettings()
                .putList(ConfigConstants.LDAP_HOSTS, "localhost:" + ldapPort)
                .put("users.u1.search", "(uid={0})").put("users.u1.base", "ou=people,o=TEST")
                .put("roles.g1.base", "ou=groups,o=TEST").put(ConfigConstants.LDAP_AUTHZ_ROLENAME, "cn")
                .put("roles.g1.search", "(uniqueMember={0})")
                // .put("searchguard.authentication.authorization.ldap.userrolename",
                // "(uniqueMember={0})")
                .build();

        final LdapUser user1 = (LdapUser) new LDAPAuthenticationBackend(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));

        new LDAPAuthorizationBackend2(settings, null).fillRoles(user1, null);

        Assert.assertNotNull(user1);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user1.getName());
        Assert.assertEquals(2, user1.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user1.getRoles())).get(0));
        Assert.assertEquals(user1.getName(), user1.getUserEntry().getDN());
        
        
        final LdapUser user2 = (LdapUser) new LDAPAuthenticationBackend2(settings, null)
                .authenticate(new AuthCredentials("jacksonm", "secret".getBytes(StandardCharsets.UTF_8)));

        new LDAPAuthorizationBackend(settings, null).fillRoles(user2, null);

        Assert.assertNotNull(user2);
        Assert.assertEquals("cn=Michael Jackson,ou=people,o=TEST", user2.getName());
        Assert.assertEquals(2, user2.getRoles().size());
        Assert.assertEquals("ceo", new ArrayList<>(new TreeSet<>(user2.getRoles())).get(0));
        Assert.assertEquals(user2.getName(), user2.getUserEntry().getDN());
    }

    @AfterClass
    public static void tearDown() throws Exception {

        if (ldapServer != null) {
            ldapServer.stop();
        }

    }
}
