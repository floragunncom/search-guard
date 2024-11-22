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

package com.floragunn.searchguard.legacy;

import java.util.TreeSet;

import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;

import org.junit.Test;

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.legacy.auth.HTTPClientCertAuthenticator;
import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public class IntegrationTests extends SingleClusterTest {

    @Test
    public void testSearchScroll() throws Exception {
        
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> e.printStackTrace());
        
    final Settings settings = Settings.builder()
            .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+".worf", "knuddel","nonexists")
            .build();
    setup(settings);
    final RestHelper rh = nonSslRestHelper();

        Client tc = getPrivilegedInternalNodeClient();
        for(int i=0; i<3; i++)
        tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        
        //System.out.println("########search");
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executeGetRequest("vulcangov/_search?scroll=1m&pretty=true", encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());
        
        //System.out.println(res.getBody());
        int start = res.getBody().indexOf("_scroll_id") + 15;
        String scrollid = res.getBody().substring(start, res.getBody().indexOf("\"", start+1));
        //System.out.println(scrollid);
        //System.out.println("########search scroll");
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executePostRequest("/_search/scroll?pretty=true", "{\"scroll_id\" : \""+scrollid+"\"}", encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());


        //System.out.println("########search done");
        
        
    }
    
    @Test
    public void testDnParsingCertAuth() throws Exception {
        Settings settings = Settings.builder()
                .put("username_attribute", "cn")
                .put("roles_attribute", "l")
                .build();
        HTTPClientCertAuthenticator auth = new HTTPClientCertAuthenticator(settings, null);
        Assert.assertEquals("abc", auth.extractCredentials(null, newThreadContext("cn=abc,cn=xxx,l=ert,st=zui,c=qwe")).getUsername());
        Assert.assertEquals("abc", auth.extractCredentials(null, newThreadContext("cn=abc,l=ert,st=zui,c=qwe")).getUsername());
        Assert.assertEquals("abc", auth.extractCredentials(null, newThreadContext("CN=abc,L=ert,st=zui,c=qwe")).getUsername());     
        Assert.assertEquals("abc", auth.extractCredentials(null, newThreadContext("l=ert,cn=abc,st=zui,c=qwe")).getUsername());
        Assert.assertNull(auth.extractCredentials(null, newThreadContext("L=ert,CN=abc,c,st=zui,c=qwe")));
        Assert.assertEquals("abc", auth.extractCredentials(null, newThreadContext("l=ert,st=zui,c=qwe,cn=abc")).getUsername());
        Assert.assertEquals("abc", auth.extractCredentials(null, newThreadContext("L=ert,st=zui,c=qwe,CN=abc")).getUsername()); 
        Assert.assertEquals("L=ert,st=zui,c=qwe", auth.extractCredentials(null, newThreadContext("L=ert,st=zui,c=qwe")).getUsername()); 
        Assert.assertArrayEquals(new String[] {"ert"}, auth.extractCredentials(null, newThreadContext("cn=abc,l=ert,st=zui,c=qwe")).getBackendRoles().toArray(new String[0]));
        Assert.assertArrayEquals(new String[] {"bleh", "ert"}, new TreeSet<>(auth.extractCredentials(null, newThreadContext("cn=abc,l=ert,L=bleh,st=zui,c=qwe")).getBackendRoles()).toArray(new String[0]));
        
        settings = Settings.builder()
                .build();
        auth = new HTTPClientCertAuthenticator(settings, null);
        Assert.assertEquals("cn=abc,l=ert,st=zui,c=qwe", auth.extractCredentials(null, newThreadContext("cn=abc,l=ert,st=zui,c=qwe")).getUsername());
    }
    
    private ThreadContext newThreadContext(String sslPrincipal) {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(ConfigConstants.SG_SSL_PRINCIPAL, sslPrincipal);
        return threadContext;
    }

    @Test
    public void testDNSpecials() throws Exception {
    
        final Settings settings = Settings.builder()
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, FileHelper.getAbsoluteFilePathFromClassPath("node-untspec5-keystore.p12"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, "1")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_TYPE, "PKCS12")
                .putList("searchguard.nodes_dn", "EMAILADDRESS=unt@tst.com,CN=node-untspec5.example.com,OU=SSL,O=Te\\, st,L=Test,C=DE")
                .putList("searchguard.authcz.admin_dn", "EMAILADDRESS=unt@xxx.com,CN=node-untspec6.example.com,OU=SSL,O=Te\\, st,L=Test,C=DE")
                .put("searchguard.cert.oid","1.2.3.4.5.6")
                .build();
        
        setup(Settings.EMPTY, new DynamicSgConfig(), settings, true);
        RestHelper rh = nonSslRestHelper();
        
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("").getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("worf", "worf")).getStatusCode());
    
    }
    
    @Test
    public void testDNSpecials1() throws Exception {
    
        final Settings settings = Settings.builder()
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, FileHelper.getAbsoluteFilePathFromClassPath("node-untspec5-keystore.p12"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, "1")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_TYPE, "PKCS12")
                .putList("searchguard.nodes_dn", "EMAILADDRESS=unt@tst.com,CN=node-untspec5.example.com,OU=SSL,O=Te\\, st,L=Test,C=DE")
                .putList("searchguard.authcz.admin_dn", "EMAILADDREss=unt@xxx.com,  cn=node-untspec6.example.com, OU=SSL,O=Te\\, st,L=Test, c=DE")
                .put("searchguard.cert.oid","1.2.3.4.5.6")
                .build();

        
        setup(Settings.EMPTY, new DynamicSgConfig(), settings, true);
        RestHelper rh = nonSslRestHelper();
        
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("").getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("worf", "worf")).getStatusCode());
    }

    @Test
    public void testMultiget() throws Exception {
    
        setup();
    
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("mindex1").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("mindex2").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":2}", XContentType.JSON)).actionGet();

        //sg_multiget -> picard
        
        
            String mgetBody = "{"+
            "\"docs\" : ["+
                "{"+
                     "\"_index\" : \"mindex1\","+
                    "\"_id\" : \"1\""+
               " },"+
               " {"+
                   "\"_index\" : \"mindex2\","+
                   " \"_id\" : \"2\""+
                "}"+
            "]"+
        "}";
       
       RestHelper rh = nonSslRestHelper();
       HttpResponse resc = rh.executePostRequest("_mget?refresh=true", mgetBody, encodeBasicHeader("picard", "picard"));
       //System.out.println(resc.getBody());
       Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
       Assert.assertFalse(resc.getBody().contains("type2"));
        
    }

    @Test
    public void testRestImpersonation() throws Exception {
    
        final Settings settings = Settings.builder()
                 .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+".spock", "knuddel","userwhonotexists").build();
 
        setup(settings);
        
        RestHelper rh = nonSslRestHelper();
        
        //knuddel:
        //    hash: _rest_impersonation_only_
    
        HttpResponse resp;
        resp = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("sg_impersonate_as", "knuddel"), encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, resp.getStatusCode());
    
        resp = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("sg_impersonate_as", "knuddel"), encodeBasicHeader("spock", "spock"));
        Assert.assertEquals(HttpStatus.SC_OK, resp.getStatusCode());
        Assert.assertTrue(resp.getBody(), resp.getBody().contains("User knuddel"));
        Assert.assertFalse(resp.getBody().contains("spock"));
        
        resp = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("sg_impersonate_as", "userwhonotexists"), encodeBasicHeader("spock", "spock"));
        //System.out.println(resp.getBody());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, resp.getStatusCode());
    
        resp = rh.executeGetRequest("/_searchguard/authinfo", new BasicHeader("sg_impersonate_as", "invalid"), encodeBasicHeader("spock", "spock"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, resp.getStatusCode());
    }

    @Test
    public void testSingle() throws Exception {
    
        setup();
    
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("shakespeare").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(new String[]{"config","roles","rolesmapping","internalusers","actiongroups"})).actionGet();
        Assert.assertFalse(cur.hasFailures());
        Assert.assertEquals(clusterInfo.numNodes, cur.getNodes().size());

        RestHelper rh = nonSslRestHelper();
        //sg_shakespeare -> picard
    
        HttpResponse resc = rh.executeGetRequest("shakespeare/_search", encodeBasicHeader("picard", "picard"));
        //System.out.println(resc.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody().contains("\"content\":1"));
        
        resc = rh.executeHeadRequest("shakespeare", encodeBasicHeader("picard", "picard"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        
    }

    @Test
    public void testSpecialUsernames() throws Exception {
    
        setup();    
        RestHelper rh = nonSslRestHelper();
        
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("bug.99", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("", encodeBasicHeader("a", "b")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("\"'+-,;_?*@<>!$%&/()=#", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("§ÄÖÜäöüß", "nagilum")).getStatusCode());
    
    }

    @Test
    public void testXff() throws Exception {
    
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_xff.yml"), Settings.EMPTY, true);
        RestHelper rh = nonSslRestHelper();
        HttpResponse resc = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader("x-forwarded-for", "10.0.0.7"), encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(200, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("10.0.0.7"));
    }

    @Test
    public void testRegexExcludes() throws Exception {
        
        setup(Settings.EMPTY, new DynamicSgConfig(), Settings.EMPTY);

        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("indexa").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"indexa\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("indexb").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"indexb\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("isallowed").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"isallowed\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("special").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"special\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("alsonotallowed").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"alsonotallowed\":1}", XContentType.JSON)).actionGet();

        RestHelper rh = nonSslRestHelper();
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("index*/_search",encodeBasicHeader("rexclude", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("indexa/_search",encodeBasicHeader("rexclude", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("isallowed/_search",encodeBasicHeader("rexclude", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("special/_search",encodeBasicHeader("rexclude", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executeGetRequest("alsonotallowed/_search",encodeBasicHeader("rexclude", "nagilum")).getStatusCode());
    }
        
    @Test
    public void testMultiRoleSpan2() throws Exception {
        
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_multirolespan.yml"), Settings.EMPTY);
        final RestHelper rh = nonSslRestHelper();

        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("mindex_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("mindex_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":2}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("mindex_3").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":2}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("mindex_4").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":2}", XContentType.JSON)).actionGet();

        HttpResponse res = rh.executeGetRequest("/mindex_1,mindex_2/_search", encodeBasicHeader("mindex12", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        
        res = rh.executeGetRequest("/mindex_1,mindex_3/_search", encodeBasicHeader("mindex12", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());

        res = rh.executeGetRequest("/mindex_1,mindex_4/_search", encodeBasicHeader("mindex12", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
         
    }
    
    @Test
    public void testSGUnderscore() throws Exception {
        
        setup();
        final RestHelper rh = nonSslRestHelper();
        
        HttpResponse res = rh.executePostRequest("abc_xyz_2018_05_24/_doc/1", "{\"content\":1}", encodeBasicHeader("underscore", "nagilum"));
        
        res = rh.executeGetRequest("abc_xyz_2018_05_24/_doc/1", encodeBasicHeader("underscore", "nagilum"));
        Assert.assertTrue(res.getBody(),res.getBody().contains("\"content\":1"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("abc_xyz_2018_05_24/_refresh", encodeBasicHeader("underscore", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("aaa_bbb_2018_05_24/_refresh", encodeBasicHeader("underscore", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
    }

    @Test
    public void testUpdate() throws Exception {
        final Settings settings = Settings.builder()
                .put("searchguard.roles_mapping_resolution", "BOTH")
                .build();
        setup(settings);
        final RestHelper rh = nonSslRestHelper();

        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("indexc").id("0")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"content\":1}", XContentType.JSON)).actionGet();

        HttpResponse res = rh.executePostRequest("indexc/_update/0?pretty=true&refresh=true", "{\"doc\" : {\"content\":2}}",
                encodeBasicHeader("user_c", "user_c"));
        //System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testSgIndexSecurity() throws Exception {
        setup();
        final RestHelper rh = nonSslRestHelper();
        
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("indexa").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":\"indexa\"}", XContentType.JSON)).actionGet();


        HttpResponse res = rh.executePutRequest("searchguard,inde*/_mapping?pretty", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePutRequest("searchguard/_mapping?pretty", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("*earc*gua*/_mapping?pretty", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("*/_mapping?pretty", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePutRequest("_all/_mapping?pretty", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePostRequest("searchguard/_close", "",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executeDeleteRequest("searchguard",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executeDeleteRequest("_all",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePutRequest("*/_settings", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("_settings", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("_all/_settings", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("searchguard/_settings", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("searchgu*/_settings", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        
        res = rh.executePostRequest("searchguard/_freeze", "",encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertTrue(res.getStatusCode() >= 400);

        String bulkBody = "{ \"index\" : { \"_index\" : \"searchguard\", \"_id\" : \"1\" } }\n" //
                + "{ \"field1\" : \"value1\" }\n"//
                + "{ \"index\" : { \"_index\" : \"searchguard\", \"_id\" : \"2\" } }\n" //
                + "{ \"field2\" : \"value2\" }\n"//
                + "{ \"index\" : { \"_index\" : \"myindex\", \"_id\" : \"2\" } }\n" //
                + "{ \"field2\" : \"value2\" }\n" //
                + "{ \"delete\" : { \"_index\" : \"searchguard\", \"_id\" : \"config\" } }\n";
        res = rh.executePostRequest("_bulk?refresh=true&pretty", bulkBody, encodeBasicHeader("nagilum", "nagilum"));
        //System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        
        Assert.assertEquals(4, res.getBody().split("\"status\" : 403,").length);

    }
    
    @Test
    public void testSgIndexSecurityWithSgIndexExcluded() throws Exception {
        final Settings settings = Settings.builder()
                .put("action.destructive_requires_name",false)
                .build();

        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_dnfof.yml"), settings);
        
        final RestHelper rh = nonSslRestHelper();
        
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("indexa").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":\"indexa\"}", XContentType.JSON)).actionGet();

        HttpResponse res = rh.executePutRequest("searchguard*,inde*/_mapping?pretty", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());

        res = rh.executePutRequest("searchguard/_mapping?pretty", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("*earc*gua*/_mapping?pretty", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("*/_mapping?pretty&ignore_unavailable=true", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePutRequest("_all/_mapping?pretty&ignore_unavailable=true", "{\"properties\": {\"name\":{\"type\":\"text\"}}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePostRequest("searchguard/_close", "",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executeDeleteRequest("searchguard",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("*/_settings?ignore_unavailable=true", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePutRequest("_settings?ignore_unavailable=true", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePutRequest("_all/_settings?ignore_unavailable=true", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePutRequest("searchguard/_settings", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("searchgu*/_settings", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        res = rh.executePutRequest("*,-searchguard/_settings", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executePutRequest("*,-searchguard,-index*/_settings", "{\"index\" : {\"number_of_replicas\" : 2}}",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, res.getStatusCode());
        res = rh.executeDeleteRequest("_all?ignore_unavailable=true",
                encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

}
