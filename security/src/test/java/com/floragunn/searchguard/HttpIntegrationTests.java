/*
 * Copyright 2015-2017 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class HttpIntegrationTests extends SingleClusterTest {

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @Test
    public void testHTTPBasic() throws Exception {
        final Settings settings = Settings.builder()
                .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+".worf", "knuddel","nonexists")
                .build();
        setup(settings);
        final RestHelper rh = nonSslRestHelper();
    
            try (Client tc = getInternalTransportClient()) {                    
                tc.admin().indices().create(new CreateIndexRequest("copysf")).actionGet();         
                tc.index(new IndexRequest("vulcangov").type("kolinahr").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();                
                tc.index(new IndexRequest("starfleet").type("ships").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("starfleet_academy").type("students").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("starfleet_library").type("public").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("klingonempire").type("ships").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("public").type("legends").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("v2").type("legends").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("v3").type("legends").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
     
                tc.index(new IndexRequest("spock").type("type01").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("kirk").type("type01").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("role01_role02").type("type01").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
    
                tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("starfleet","starfleet_academy","starfleet_library").alias("sf"))).actionGet();
                tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("klingonempire","vulcangov").alias("nonsf"))).actionGet();
                tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("public").alias("unrestricted"))).actionGet();

            }
            
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("").getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("_search").getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeDeleteRequest("nonexistentindex*", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest(".nonexistentindex*", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("searchguard/config/2", "{}",encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, rh.executeGetRequest("searchguard/config/0", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, rh.executeGetRequest("xxxxyyyy/config/0", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("abc", "abc:abc")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("userwithnopassword", "")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("userwithblankpassword", "")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("worf", "wrongpasswd")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", new BasicHeader("Authorization", "Basic "+"wrongheader")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", new BasicHeader("Authorization", "Basic ")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", new BasicHeader("Authorization", "Basic")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", new BasicHeader("Authorization", "")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("picard", "picard")).getStatusCode());
    
            for(int i=0; i< 10; i++) {
                Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("worf", "wrongpasswd")).getStatusCode());
            }
    
            Assert.assertEquals(HttpStatus.SC_OK, rh.executePutRequest("/theindex","{}",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_CREATED, rh.executePutRequest("/theindex/type/1?refresh=true","{\"a\":0}",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            //Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("/theindex/_analyze?text=this+is+a+test",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            //Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("_analyze?text=this+is+a+test",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeDeleteRequest("/theindex",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeDeleteRequest("/klingonempire",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("starfleet/_search", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("_search", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("starfleet/ships/_search?pretty", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeDeleteRequest("searchguard/", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("/searchguard/_close", null,encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("/searchguard/_upgrade", null,encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("/searchguard/_mapping","{}",encodeBasicHeader("worf", "worf")).getStatusCode());
    
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("searchguard/", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("searchguard/config/2", "{}",encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("searchguard/config/0",encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeDeleteRequest("searchguard/config/0",encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("searchguard/config/0","{}",encodeBasicHeader("worf", "worf")).getStatusCode());
            
            HttpResponse resc = rh.executeGetRequest("_cat/indices/public?v",encodeBasicHeader("bug108", "nagilum"));
            Assert.assertTrue(resc.getBody().contains("green"));
            Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
            
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("role01_role02/type01/_search?pretty",encodeBasicHeader("user_role01_role02_role03", "user_role01_role02_role03")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("role01_role02/type01/_search?pretty",encodeBasicHeader("user_role01", "user_role01")).getStatusCode());
    
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("spock/type01/_search?pretty",encodeBasicHeader("spock", "spock")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("spock/type01/_search?pretty",encodeBasicHeader("kirk", "kirk")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("kirk/type01/_search?pretty",encodeBasicHeader("kirk", "kirk")).getStatusCode());

    //all  
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("_mapping/config?include_type_name=true","{\"i\" : [\"4\"]}",encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("searchguard/_mget","{\"ids\" : [\"0\"]}",encodeBasicHeader("worf", "worf")).getStatusCode());
            
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("starfleet/ships/_search?pretty", encodeBasicHeader("worf", "worf")).getStatusCode());
    
            try (Client tc = getInternalTransportClient()) {       
                tc.index(new IndexRequest("searchguard").type(getType()).id("roles").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("roles", FileHelper.readYamlContent("sg_roles_deny.yml"))).actionGet();
                ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(new String[]{"roles"})).actionGet();
                Assert.assertFalse(cur.hasFailures());
                Assert.assertEquals(clusterInfo.numNodes, cur.getNodes().size());
            }
            
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("starfleet/ships/_search?pretty", encodeBasicHeader("worf", "worf")).getStatusCode());
    
            try (Client tc = getInternalTransportClient()) {
                tc.index(new IndexRequest("searchguard").type(getType()).id("roles").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("roles", FileHelper.readYamlContent("sg_roles.yml"))).actionGet();
                ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(new String[]{"roles"})).actionGet();
                Assert.assertFalse(cur.hasFailures());
                Assert.assertEquals(clusterInfo.numNodes, cur.getNodes().size());
            }
            
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("starfleet/ships/_search?pretty", encodeBasicHeader("worf", "worf")).getStatusCode());
            HttpResponse res = rh.executeGetRequest("_search?pretty", encodeBasicHeader("nagilum", "nagilum"));
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 11"));
            Assert.assertTrue(!res.getBody().contains("searchguard"));
            
            res = rh.executeGetRequest("_nodes/stats?pretty", encodeBasicHeader("nagilum", "nagilum"));
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
            Assert.assertTrue(res.getBody().contains("total_in_bytes"));
            Assert.assertTrue(res.getBody().contains("max_file_descriptors"));
            Assert.assertTrue(res.getBody().contains("buffer_pools"));
            Assert.assertFalse(res.getBody().contains("\"nodes\" : { }"));
            
            res = rh.executePostRequest("*/_upgrade", "", encodeBasicHeader("nagilum", "nagilum"));
            System.out.println(res.getBody());
            System.out.println(res.getStatusReason());
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
            
            String bulkBody = 
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value1\" }" +System.lineSeparator()+
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }"+System.lineSeparator()+
                "{ \"field2\" : \"value2\" }"+System.lineSeparator();
    
            res = rh.executePostRequest("_bulk", bulkBody, encodeBasicHeader("writer", "writer"));
            System.out.println(res.getBody());
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());  
            Assert.assertTrue(res.getBody().contains("\"errors\":false"));
            Assert.assertTrue(res.getBody().contains("\"status\":201"));  
            
            res = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader("sg_tenant", "unittesttenant"), encodeBasicHeader("worf", "worf"));
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
            Assert.assertTrue(res.getBody().contains("sg_tenants"));
            Assert.assertTrue(res.getBody().contains("unittesttenant"));
            Assert.assertTrue(res.getBody().contains("\"kltentrw\":true"));
            Assert.assertTrue(res.getBody().contains("\"user_name\":\"worf\""));
            
            res = rh.executeGetRequest("_searchguard/authinfo", encodeBasicHeader("worf", "worf"));
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
            Assert.assertTrue(res.getBody().contains("sg_tenants"));
            Assert.assertTrue(res.getBody().contains("\"user_requested_tenant\":null"));
            Assert.assertTrue(res.getBody().contains("\"kltentrw\":true"));
            Assert.assertTrue(res.getBody().contains("\"user_name\":\"worf\""));
            Assert.assertTrue(res.getBody().contains("\"custom_attribute_names\":[]"));
            Assert.assertFalse(res.getBody().contains("attributes="));
            
            res = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("custattr", "nagilum"));
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
            Assert.assertTrue(res.getBody().contains("sg_tenants"));
            Assert.assertTrue(res.getBody().contains("\"user_requested_tenant\" : null"));
            Assert.assertTrue(res.getBody().contains("\"user_name\" : \"custattr\""));
            Assert.assertTrue(res.getBody().contains("\"custom_attribute_names\" : ["));
            Assert.assertTrue(res.getBody().contains("attr.internal.c3"));
            Assert.assertTrue(res.getBody().contains("attr.internal.c1"));
            
            res = rh.executeGetRequest("v2/_search", encodeBasicHeader("custattr", "nagilum"));
            Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
            
            res = rh.executeGetRequest("v3/_search", encodeBasicHeader("custattr", "nagilum"));
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
            
            final String reindex = "{"+
                    "\"source\": {"+    
                      "\"index\": \"starfleet\""+
                    "},"+
                    "\"dest\": {"+
                      "\"index\": \"copysf\""+
                    "}"+
                  "}";
    
            res = rh.executePostRequest("_reindex?pretty", reindex, encodeBasicHeader("nagilum", "nagilum"));
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"total\" : 1"));
            Assert.assertTrue(res.getBody().contains("\"batches\" : 1"));
            Assert.assertTrue(res.getBody().contains("\"failures\" : [ ]"));
            
            //rest impersonation
            res = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("sg_impersonate_as","knuddel"), encodeBasicHeader("worf", "worf"));
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
            Assert.assertTrue(res.getBody(), res.getBody().contains("User knuddel"));
            Assert.assertTrue(res.getBody(), res.getBody().contains("attr.internal.test1"));
            Assert.assertFalse(res.getBody(), res.getBody().contains("worf"));
            
            res = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("sg_impersonate_as","nonexists"), encodeBasicHeader("worf", "worf"));
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
            
            res = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("sg_impersonate_as","notallowed"), encodeBasicHeader("worf", "worf"));
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        }

    @Test
    public void testHTTPSCompressionEnabled() throws Exception {
        final Settings settings = Settings.builder()
                .put("searchguard.ssl.http.enabled",true)
                .put("searchguard.ssl.http.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("truststore.jks"))
                .put("http.compression",true)
                .build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings, true);
        final RestHelper rh = restHelper(); //ssl resthelper

        HttpResponse res = rh.executeGetRequest("_searchguard/sslinfo", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        System.out.println(res);
        assertContains(res, "*ssl_protocol\":\"TLSv1.2*");
        res = rh.executeGetRequest("_nodes", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        System.out.println(res);
        assertNotContains(res, "*\"compression\":\"false\"*");
        assertContains(res, "*\"compression\":\"true\"*");
    }
    
    @Test
    public void testHTTPSCompression() throws Exception {
        final Settings settings = Settings.builder()
                .put("searchguard.ssl.http.enabled",true)
                .put("searchguard.ssl.http.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("truststore.jks"))
                .build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings, true);
        final RestHelper rh = restHelper(); //ssl resthelper

        HttpResponse res = rh.executeGetRequest("_searchguard/sslinfo", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        System.out.println(res);
        assertContains(res, "*ssl_protocol\":\"TLSv1.2*");
        res = rh.executeGetRequest("_nodes", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        System.out.println(res);
        assertContains(res, "*\"compression\":\"false\"*");
        assertNotContains(res, "*\"compression\":\"true\"*");
    }

    @Test
    public void testHTTPAnon() throws Exception {
    
            setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_anon.yml"), Settings.EMPTY, true);
            
            RestHelper rh = nonSslRestHelper();
    
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("").getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("worf", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
            HttpResponse resc = rh.executeGetRequest("_searchguard/authinfo");
            System.out.println(resc.getBody());
            Assert.assertTrue(resc.getBody().contains("sg_anonymous"));
            Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
            
            resc = rh.executeGetRequest("_searchguard/authinfo?pretty=true");
            System.out.println(resc.getBody());
            Assert.assertTrue(resc.getBody().contains("\"remote_address\" : \"")); //check pretty print
            Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
            
            resc = rh.executeGetRequest("_searchguard/authinfo", encodeBasicHeader("nagilum", "nagilum"));
            System.out.println(resc.getBody());
            Assert.assertTrue(resc.getBody().contains("nagilum"));
            Assert.assertFalse(resc.getBody().contains("sg_anonymous"));
            Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
            
            try (Client tc = getInternalTransportClient()) {    
                tc.index(new IndexRequest("searchguard").type(getType()).id("config").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("config", FileHelper.readYamlContent("sg_config.yml"))).actionGet();
                tc.index(new IndexRequest("searchguard").type(getType()).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("internalusers").source("internalusers", FileHelper.readYamlContent("sg_internal_users.yml"))).actionGet();
                ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(new String[]{"config","roles","rolesmapping","internalusers","actiongroups"})).actionGet();
                Assert.assertFalse(cur.hasFailures());
                Assert.assertEquals(clusterInfo.numNodes, cur.getNodes().size());
             }
    
            
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("").getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("_searchguard/authinfo").getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("worf", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    }

    @Test
    public void testHTTPClientCert() throws Exception {
        final Settings settings = Settings.builder()
                .put("searchguard.ssl.http.clientauth_mode","REQUIRE")
                .put("searchguard.ssl.http.enabled",true)
                .put("searchguard.ssl.http.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("truststore.jks"))
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED_PROTOCOLS, "TLSv1.1","TLSv1.2")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_PROTOCOLS, "TLSv1.1","TLSv1.2")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256")
                .build();
        
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_clientcert.yml"), settings, true);
    
        try (Client tc = getInternalTransportClient()) {

            tc.index(new IndexRequest("vulcangov").type("type").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
            
            ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(new String[]{"config","roles","rolesmapping","internalusers","actiongroups"})).actionGet();
            Assert.assertFalse(cur.hasFailures());
            Assert.assertEquals(clusterInfo.numNodes, cur.getNodes().size());
        }
    
        RestHelper rh = restHelper();
        
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "spock-keystore.jks";
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_search").getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("searchguard/"+getType()+"/x", "{}").getStatusCode());
        
        rh.keystore = "kirk-keystore.jks";
        Assert.assertEquals(HttpStatus.SC_CREATED, rh.executePutRequest("searchguard/"+getType()+"/y", "{}").getStatusCode());
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("_searchguard/authinfo")).getStatusCode());
        System.out.println(res.getBody());
    }

    @Test
    @Ignore
    public void testHTTPPlaintextErrMsg() throws Exception {
        
        try {
            final Settings settings = Settings.builder()
                    .put("searchguard.ssl.http.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("node-0-keystore.jks"))
                    .put("searchguard.ssl.http.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("truststore.jks"))
                    .put("searchguard.ssl.http.enabled", true)
                    .build();
            setup(settings);
            RestHelper rh = nonSslRestHelper();
            rh.executeGetRequest("", encodeBasicHeader("worf", "worf"));
            Assert.fail("NoHttpResponseException expected");
        } catch (NoHttpResponseException e) {
            String log = FileUtils.readFileToString(new File("unittest.log"), StandardCharsets.UTF_8);
            Assert.assertTrue(log, log.contains("speaks http plaintext instead of ssl, will close the channel"));
        } catch (Exception e) {
            Assert.fail("NoHttpResponseException expected but was "+e.getClass()+"#"+e.getMessage());
        }
        
      }

    @Test
    public void testHTTPProxyDefault() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_proxy.yml"), Settings.EMPTY, true);
        RestHelper rh = nonSslRestHelper();
    
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("").getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),new BasicHeader("x-proxy-user", "scotty"), encodeBasicHeader("nagilum-wrong", "nagilum-wrong")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),new BasicHeader("x-proxy-user-wrong", "scotty"), encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "a"),new BasicHeader("x-proxy-user", "scotty"), encodeBasicHeader("nagilum-wrong", "nagilum-wrong")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "a,b,c"),new BasicHeader("x-proxy-user", "scotty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),new BasicHeader("x-proxy-user", "scotty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),new BasicHeader("X-Proxy-User", "scotty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),new BasicHeader("x-proxy-user", "scotty"),new BasicHeader("x-proxy-roles", "starfleet,engineer")).getStatusCode());
        
    }

    @Test
    public void testHTTPProxyRolesSeparator() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_proxy_custom.yml"), Settings.EMPTY, true);
        RestHelper rh = nonSslRestHelper();
        // separator is configured as ";" so separating roles with "," leads to one (wrong) backend role
        HttpResponse res = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),new BasicHeader("user", "scotty"),new BasicHeader("roles", "starfleet,engineer"));
        Assert.assertTrue("Expected one backend role since separator is incorrect", res.getBody().contains("\"backend_roles\":[\"starfleet,engineer\"]"));    
        // correct separator, now we should see two backend roles
        res = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),new BasicHeader("user", "scotty"),new BasicHeader("roles", "starfleet;engineer"));
        Assert.assertTrue("Expected two backend roles string since separator is correct: " + res.getBody(), res.getBody().contains("\"backend_roles\":[\"starfleet\",\"engineer\"]"));    
        
    }

    @Test
        public void testHTTPBasic2() throws Exception {
            
            setup(Settings.EMPTY, new DynamicSgConfig(), Settings.EMPTY);
    
            try (Client tc = getInternalTransportClient(this.clusterInfo, Settings.EMPTY)) {
                
                tc.admin().indices().create(new CreateIndexRequest("copysf")).actionGet();
                
                tc.index(new IndexRequest("vulcangov").type("kolinahr").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                 
                tc.index(new IndexRequest("starfleet").type("ships").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                 
                tc.index(new IndexRequest("starfleet_academy").type("students").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                
                tc.index(new IndexRequest("starfleet_library").type("public").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                
                tc.index(new IndexRequest("klingonempire").type("ships").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                
                tc.index(new IndexRequest("public").type("legends").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
               
                tc.index(new IndexRequest("spock").type("type01").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("kirk").type("type01").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("role01_role02").type("type01").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
    
                tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("starfleet","starfleet_academy","starfleet_library").alias("sf"))).actionGet();
                tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("klingonempire","vulcangov").alias("nonsf"))).actionGet();
                tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("public").alias("unrestricted"))).actionGet();
            }
            
            RestHelper rh = nonSslRestHelper();
            
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("").getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeDeleteRequest("nonexistentindex*", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest(".nonexistentindex*", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("searchguard/config/2", "{}",encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, rh.executeGetRequest("searchguard/config/0", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, rh.executeGetRequest("xxxxyyyy/config/0", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("abc", "abc:abc")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("userwithnopassword", "")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("userwithblankpassword", "")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("worf", "wrongpasswd")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", new BasicHeader("Authorization", "Basic "+"wrongheader")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", new BasicHeader("Authorization", "Basic ")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", new BasicHeader("Authorization", "Basic")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", new BasicHeader("Authorization", "")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("picard", "picard")).getStatusCode());
    
            for(int i=0; i< 10; i++) {
                Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("worf", "wrongpasswd")).getStatusCode());
            }
            
            Assert.assertEquals(HttpStatus.SC_OK, rh.executePutRequest("/theindex","{}",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_CREATED, rh.executePutRequest("/theindex/type/1?refresh=true","{\"a\":0}",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            //Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("/theindex/_analyze?text=this+is+a+test",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            //Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("_analyze?text=this+is+a+test",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeDeleteRequest("/theindex",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeDeleteRequest("/klingonempire",encodeBasicHeader("theindexadmin", "theindexadmin")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("starfleet/_search", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("_search", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("starfleet/ships/_search?pretty", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeDeleteRequest("searchguard/", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("/searchguard/_close", null,encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("/searchguard/_upgrade", null,encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("/searchguard/_mapping","{}",encodeBasicHeader("worf", "worf")).getStatusCode());
    
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("searchguard/", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("searchguard/config/2", "{}",encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("searchguard/config/0",encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeDeleteRequest("searchguard/config/0",encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("searchguard/config/0","{}",encodeBasicHeader("worf", "worf")).getStatusCode());
            
            HttpResponse resc = rh.executeGetRequest("_cat/indices/public",encodeBasicHeader("bug108", "nagilum"));
            System.out.println(resc.getBody());
            //Assert.assertTrue(resc.getBody().contains("green"));
            Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
            
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("role01_role02/type01/_search?pretty",encodeBasicHeader("user_role01_role02_role03", "user_role01_role02_role03")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("role01_role02/type01/_search?pretty",encodeBasicHeader("user_role01", "user_role01")).getStatusCode());
    
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("spock/type01/_search?pretty",encodeBasicHeader("spock", "spock")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("spock/type01/_search?pretty",encodeBasicHeader("kirk", "kirk")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("kirk/type01/_search?pretty",encodeBasicHeader("kirk", "kirk")).getStatusCode());
            
            System.out.println("ok");
    //all
            
            
        }
    
    @Test
    public void testBulk() throws Exception {
        final Settings settings = Settings.builder()
                .put(ConfigConstants.SEARCHGUARD_ROLES_MAPPING_RESOLUTION, "BOTH")
                .build();
        setup(Settings.EMPTY, new DynamicSgConfig().setSgRoles("sg_roles_bulk.yml"), settings);
        final RestHelper rh = nonSslRestHelper();
    
        String bulkBody = 
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value1\" }" +System.lineSeparator()+
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }"+System.lineSeparator()+
                "{ \"field2\" : \"value2\" }"+System.lineSeparator();
    
        HttpResponse res = rh.executePostRequest("_bulk", bulkBody, encodeBasicHeader("bulk", "nagilum"));
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());  
        Assert.assertTrue(res.getBody().contains("\"errors\":false"));
        Assert.assertTrue(res.getBody().contains("\"status\":201"));  
    }
    
    @Test
    public void test557() throws Exception {
        final Settings settings = Settings.builder()
                .put(ConfigConstants.SEARCHGUARD_ROLES_MAPPING_RESOLUTION, "BOTH")
                .build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings);
        
        try (Client tc = getInternalTransportClient(this.clusterInfo, Settings.EMPTY)) {
            
            tc.admin().indices().create(new CreateIndexRequest("copysf")).actionGet();
            
            tc.index(new IndexRequest("vulcangov").type("kolinahr").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
             
            tc.index(new IndexRequest("starfleet").type("ships").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
             
            tc.index(new IndexRequest("starfleet_academy").type("students").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
            
        }
        
        final RestHelper rh = nonSslRestHelper();

        HttpResponse res = rh.executePostRequest("/*/_search", "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":10}}}}", encodeBasicHeader("nagilum", "nagilum"));
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());  
        Assert.assertTrue(res.getBody().contains("starfleet_academy"));
        res = rh.executePostRequest("/*/_search", "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":10}}}}", encodeBasicHeader("557", "nagilum"));
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());  
        Assert.assertTrue(res.getBody().contains("starfleet_academy"));  
    }
    
    @Test
    public void testTenantInfo() throws Exception {
        final Settings settings = Settings.builder()
                .build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings);
        
        /*
         
            [admin_1, praxisrw, abcdef_2_2, kltentro, praxisro, kltentrw]
			admin_1==.kibana_-1139640511_admin1
			praxisrw==.kibana_-1386441176_praxisrw
			abcdef_2_2==.kibana_-634608247_abcdef22
			kltentro==.kibana_-2014056171_kltentro
			praxisro==.kibana_-1386441184_praxisro
			kltentrw==.kibana_-2014056163_kltentrw
         
         */
        
        try (Client tc = getInternalTransportClient(this.clusterInfo, Settings.EMPTY)) {
                        
            tc.index(new IndexRequest(".kibana-6").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":2}", XContentType.JSON)).actionGet();            
            tc.index(new IndexRequest(".kibana_-1139640511_admin1").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-1386441176_praxisrw").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-634608247_abcdef22").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-12345_123456").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana2_-12345_123456").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_9876_xxx_ccc").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_fff_eee").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();

            
            tc.index(new IndexRequest("esb-prod-5").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":5}", XContentType.JSON)).actionGet();

            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices(".kibana-6").alias(".kibana"))).actionGet();
            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("esb-prod-5").alias(".kibana_-2014056163_kltentrw"))).actionGet();
            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("esb-prod-5").alias("esb-alias-5"))).actionGet();

        }
        
        final RestHelper rh = nonSslRestHelper();

        HttpResponse res = rh.executeGetRequest("/_searchguard/tenantinfo?pretty", encodeBasicHeader("itt1635", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());  

        res = rh.executeGetRequest("/_searchguard/tenantinfo?pretty", encodeBasicHeader("kibanaserver", "kibanaserver"));
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("\".kibana_-1139640511_admin1\" : \"admin_1\""));
        Assert.assertTrue(res.getBody().contains("\".kibana_-1386441176_praxisrw\" : \"praxisrw\""));
        Assert.assertTrue(res.getBody().contains(".kibana_-2014056163_kltentrw\" : \"kltentrw\""));
        Assert.assertTrue(res.getBody().contains("\".kibana_-634608247_abcdef22\" : \"abcdef_2_2\""));
        Assert.assertTrue(res.getBody().contains("\".kibana_-12345_123456\" : \"__private__\""));
        Assert.assertFalse(res.getBody().contains(".kibana-6"));
        Assert.assertFalse(res.getBody().contains("esb-"));
        Assert.assertFalse(res.getBody().contains("xxx"));
        Assert.assertFalse(res.getBody().contains(".kibana2"));
    }
    
    @Test
    public void testRestImpersonation() throws Exception {
        final Settings settings = Settings.builder()
                .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+".worf", "someotherusernotininternalusersfile")
                .build();
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_rest_impersonation.yml"), settings);
        final RestHelper rh = nonSslRestHelper();
        
        //rest impersonation
        HttpResponse res = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("sg_impersonate_as","someotherusernotininternalusersfile"), encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody(), res.getBody().contains("User someotherusernotininternalusersfile"));
        Assert.assertFalse(res.getBody(), res.getBody().contains("worf"));
    }
    
    @Test
    public void testSslOnlyMode() throws Exception {
        final Settings settings = Settings.builder()
                .put(ConfigConstants.SEARCHGUARD_SSL_ONLY, true)
                .build();
        setupSslOnlyMode(settings);
        final RestHelper rh = nonSslRestHelper();
        
        HttpResponse res = rh.executeGetRequest("/_searchguard/sslinfo");
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        
        res = rh.executePutRequest("/xyz/_doc/1","{\"a\":5}");
        Assert.assertEquals(HttpStatus.SC_CREATED, res.getStatusCode());
        
        res = rh.executeGetRequest("/_mappings");
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        
        res = rh.executeGetRequest("/_search");
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }
    
    @Test
    public void testAll() throws Exception {
        final Settings settings = Settings.builder().build();
        setup(settings);
        final RestHelper rh = nonSslRestHelper();

        try (Client tc = getInternalTransportClient()) {
            tc.index(new IndexRequest("abcdef").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                    .actionGet();
        }

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("_all/_search", encodeBasicHeader("worf", "worf")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("*/_search", encodeBasicHeader("worf", "worf")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("_search", encodeBasicHeader("worf", "worf")).getStatusCode());
    }
    

    @Test
    public void testRateLimiting() throws Exception {
    
            setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_auth_ratelimiting.yml"), Settings.EMPTY, true);
            
            RestHelper rh = nonSslRestHelper();
    
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("nagilum", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("nagilum", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("nagilum", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("worf", "worf")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("x1", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("x2", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("x3", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("x4", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("x5", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("x6", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("x7", "wrong")).getStatusCode());
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("x8", "wrong")).getStatusCode());

            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("worf", "worf")).getStatusCode());

            
    }

    @Test
    public void testEnvReplace() throws Exception {
        final Settings settings = Settings.builder()
                .build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings);

        try (Client tc = getInternalTransportClient(this.clusterInfo, Settings.EMPTY)) {
            tc.index(new IndexRequest("index1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();            
            tc.index(new IndexRequest("index2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":2}", XContentType.JSON)).actionGet();            
            tc.index(new IndexRequest("index3").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();            
            tc.index(new IndexRequest("env.replace@example.comp.com").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":4}", XContentType.JSON)).actionGet();            
        }
        
        final RestHelper rh = nonSslRestHelper();

        HttpResponse res = rh.executeGetRequest("/index1/_search?pretty", encodeBasicHeader("env.replace@example.comp.com", "nagilum"));
        Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("/index2/_search?pretty", encodeBasicHeader("env.replace@example.comp.com", "nagilum"));
        Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("/env.replace@example.comp.com/_search?pretty", encodeBasicHeader("env.replace@example.comp.com", "nagilum"));
        Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
        //res = rh.executeGetRequest("/index3/_search?pretty", encodeBasicHeader("env.replace@example.comp.com", "nagilum"));
        //Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
    }
    
    @Test
    public void testNoEnvReplace() throws Exception {
        final Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_DISABLE_ENVVAR_REPLACEMENT, true).build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings);

        try (Client tc = getInternalTransportClient(this.clusterInfo, Settings.EMPTY)) {
            tc.index(new IndexRequest("index1").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();            
            tc.index(new IndexRequest("index2").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":2}", XContentType.JSON)).actionGet();            
            tc.index(new IndexRequest("index3").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();            
            tc.index(new IndexRequest("env.replace@example.comp.com").type("doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":4}", XContentType.JSON)).actionGet();            
        }
        
        final RestHelper rh = nonSslRestHelper();

        HttpResponse res = rh.executeGetRequest("/index1/_search?pretty", encodeBasicHeader("env.replace@example.comp.com", "nagilum"));
        Assert.assertEquals(res.getBody(), HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executeGetRequest("/index2/_search?pretty", encodeBasicHeader("env.replace@example.comp.com", "nagilum"));
        Assert.assertEquals(res.getBody(), HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executeGetRequest("/env.replace@example.comp.com/_search?pretty", encodeBasicHeader("env.replace@example.comp.com", "nagilum"));
        Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
    }
}
