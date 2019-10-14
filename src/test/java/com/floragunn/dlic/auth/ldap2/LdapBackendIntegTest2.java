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

import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.floragunn.dlic.auth.ldap.srv.EmbeddedLDAPServer;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class LdapBackendIntegTest2 extends SingleClusterTest {

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
    
    @Override
    protected String getResourceFolder() {
        return "ldap";
    }

    @Test
    public void testIntegLdapAuthenticationSSL() throws Exception {
        String sgConfigAsYamlString = FileHelper.loadFile("ldap/sg_config_ldap2.yml");
        sgConfigAsYamlString = sgConfigAsYamlString.replace("${ldapsPort}", String.valueOf(ldapsPort));
        System.out.println(sgConfigAsYamlString);
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfigAsYamlString(sgConfigAsYamlString), Settings.EMPTY);
        final RestHelper rh = nonSslRestHelper();
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("jacksonm", "secret")).getStatusCode());
    }
    
    @Test
    public void testIntegLdapAuthenticationSSLFail() throws Exception {
        String sgConfigAsYamlString = FileHelper.loadFile("ldap/sg_config_ldap2.yml");
        sgConfigAsYamlString = sgConfigAsYamlString.replace("${ldapsPort}", String.valueOf(ldapsPort));
        System.out.println(sgConfigAsYamlString);
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfigAsYamlString(sgConfigAsYamlString), Settings.EMPTY);
        final RestHelper rh = nonSslRestHelper();
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("wrong", "wrong")).getStatusCode());
    }
    
    @Test
    public void testAttributesWithImpersonation() throws Exception {
        String sgConfigAsYamlString = FileHelper.loadFile("ldap/sg_config_ldap2.yml");
        sgConfigAsYamlString = sgConfigAsYamlString.replace("${ldapsPort}", String.valueOf(ldapsPort));
        final Settings settings = Settings.builder()
                .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+".cn=Captain Spock,ou=people,o=TEST", "*")
                .build();
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfigAsYamlString(sgConfigAsYamlString), settings);
        final RestHelper rh = nonSslRestHelper();
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executeGetRequest("_searchguard/authinfo", new BasicHeader("sg_impersonate_as", "jacksonm")
                ,encodeBasicHeader("spock", "spocksecret"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("ldap.dn"));
        Assert.assertTrue(res.getBody().contains("attr.ldap.entryDN"));
        Assert.assertTrue(res.getBody().contains("attr.ldap.subschemaSubentry"));

    }
    
    @AfterClass
    public static void tearDownLdap() throws Exception {

        if (ldapServer != null) {
            ldapServer.stop();
        }

    }
}
