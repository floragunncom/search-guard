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

import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.dlic.auth.ldap.srv.LdapServer;
import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;

public class LdapBackendIntegTest extends SingleClusterTest {

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    private static LdapServer tlsLdapServer = LdapServer.createTls("base.ldif"); 

        
    @Override
    protected String getResourceFolder() {
        return "ldap";
    }

    @Test
    public void testIntegLdapAuthenticationSSL() throws Exception {
        String sgConfigAsYamlString = FileHelper.loadFile("ldap/sg_config.yml");
        sgConfigAsYamlString = sgConfigAsYamlString.replace("${ldapsPort}", String.valueOf(tlsLdapServer.getPort()));
        System.out.println(sgConfigAsYamlString);
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfigAsYamlString(sgConfigAsYamlString), Settings.EMPTY);
        final RestHelper rh = nonSslRestHelper();
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("jacksonm", "secret")).getStatusCode());
    }
    
    @Test
    public void testIntegLdapAuthenticationSSLFail() throws Exception {
        String sgConfigAsYamlString = FileHelper.loadFile("ldap/sg_config.yml");
        sgConfigAsYamlString = sgConfigAsYamlString.replace("${ldapsPort}", String.valueOf(tlsLdapServer.getPort()));
        System.out.println(sgConfigAsYamlString);
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfigAsYamlString(sgConfigAsYamlString), Settings.EMPTY);
        final RestHelper rh = nonSslRestHelper();
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("wrong", "wrong")).getStatusCode());
    }
    
    @Test
    public void testAttributesWithImpersonation() throws Exception {
        String sgConfigAsYamlString = FileHelper.loadFile("ldap/sg_config.yml");
        sgConfigAsYamlString = sgConfigAsYamlString.replace("${ldapsPort}", String.valueOf(tlsLdapServer.getPort()));
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
    
    @Test
    public void ldapDlsIntegrationTest() throws Exception {
        String sgConfigAsYamlString = FileHelper.loadFile("ldap/sg_config.yml");
        sgConfigAsYamlString = sgConfigAsYamlString.replace("${ldapsPort}", String.valueOf(tlsLdapServer.getPort()));
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfigAsYamlString(sgConfigAsYamlString), Settings.EMPTY);
        
        RestHelper rh = nonSslRestHelper();
        HttpResponse res;

        try (Client tc = getPrivilegedInternalNodeClient()) {

            tc.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"a\", \"amount\": 1010}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"b\", \"amount\": 2020}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"c\", \"amount\": 3030}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"d\", \"amount\": 4040}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"e\", \"amount\": 5050}",
                    XContentType.JSON)).actionGet();
        }
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/dls_test/_search?pretty&size=100", encodeBasicHeader("jacksonm", "secret"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"value\" : 5,\n      \"relation"));

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/dls_test/_search?pretty&size=100", encodeBasicHeader("propsreplace", "propsreplace"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"value\" : 3,\n      \"relation"));
    }
    
    @AfterClass
    public static void tearDownLdap() throws Exception {

        if (tlsLdapServer != null) {
            tlsLdapServer.stop();
        }

    }
}
