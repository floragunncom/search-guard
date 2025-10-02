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

package com.floragunn.searchguard.enterprise.auditlog.integration;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.enterprise.auditlog.AbstractAuditlogiUnitTest;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

import static com.floragunn.searchsupport.Constants.DEFAULT_ACK_TIMEOUT;
import static com.floragunn.searchsupport.Constants.DEFAULT_MASTER_TIMEOUT;

public class BasicAuditlogTest extends AbstractAuditlogiUnitTest {
    
    @Test
    public void testSimpleAuthenticated() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        setupStarfleetIndex();
        TestAuditlogImpl.clear();
        
        //System.out.println("#### testSimpleAuthenticated");
        HttpResponse response = rh.executeGetRequest("_search", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Thread.sleep(1500);
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("GRANTED_PRIVILEGES"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:data/read/search"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("REST"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().toLowerCase().contains("authorization"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testSSLPlainText() throws Exception {
    //if this fails permanently look in the logs for an abstract method error or method not found error.
    //needs proper ssl plugin version
        
        Settings additionalSettings = Settings.builder()
                .put("searchguard.ssl.http.enabled",true)
                .put("searchguard.ssl.http.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        TestAuditlogImpl.clear();

        try {
            nonSslRestHelper().executeGetRequest("_search", encodeBasicHeader("admin", "admin"));
            Assert.fail();
        } catch (NoHttpResponseException e) {
            //expected
        }

        Thread.sleep(1500);
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertFalse(TestAuditlogImpl.messages.isEmpty());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("SSL_EXCEPTION"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("exception_stacktrace"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("not an SSL/TLS record"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testDefaultsRest() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        setupStarfleetIndex();
        TestAuditlogImpl.clear();
               
        HttpResponse response = rh.executeGetRequest("_search", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        
        Thread.sleep(1500);
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(2, TestAuditlogImpl.messages.size());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("GRANTED_PRIVILEGES"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("AUTHENTICATED"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:data/read/search"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("TRANSPORT"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("\"audit_request_effective_user\" : \"admin\""));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("REST"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().toLowerCase().contains("authorization"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testAuthenticated() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        setupStarfleetIndex();
        TestAuditlogImpl.clear();
   
        testMsearch();
        TestAuditlogImpl.clear();
        
        testBulkAuth();
        TestAuditlogImpl.clear();
        
        testBulkNonAuth();
        TestAuditlogImpl.clear();
        
        testUpdateSettings();
        TestAuditlogImpl.clear();
    }
    
    @Test
    public void testNonAuthenticated() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put("searchguard.audit.threadpool.size", -1)
                .build();
        
        setup(additionalSettings);
        setupStarfleetIndex();
        TestAuditlogImpl.clear();
        
        testJustAuthenticated();
        TestAuditlogImpl.clear();
        testBadHeader();
        TestAuditlogImpl.clear();
        testMissingPriv();
        TestAuditlogImpl.clear();
        testUnauthenticated();
        TestAuditlogImpl.clear();
        testUnknownAuthorization();
        TestAuditlogImpl.clear();
        testWrongUser();
        TestAuditlogImpl.clear();

    }
    
    public void testWrongUser() throws Exception {
      
        HttpResponse response = rh.executeGetRequest("", encodeBasicHeader("wronguser", "admin"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        Thread.sleep(500);
        Assert.assertTrue(TestAuditlogImpl.sb.toString(),TestAuditlogImpl.sb.toString().contains("FAILED_LOGIN"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString(),TestAuditlogImpl.sb.toString().contains("wronguser"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString(),TestAuditlogImpl.sb.toString().contains(AuditMessage.UTC_TIMESTAMP));
        Assert.assertFalse(TestAuditlogImpl.sb.toString(),TestAuditlogImpl.sb.toString().contains("AUTHENTICATED"));
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
   

    public void testUnknownAuthorization() throws Exception {
       
        HttpResponse response = rh.executeGetRequest("", encodeBasicHeader("unknown", "unknown"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("FAILED_LOGIN"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString(),TestAuditlogImpl.sb.toString().contains("Basic dW5rbm93bjp1bmtub3du"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains(AuditMessage.UTC_TIMESTAMP));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("AUTHENTICATED"));
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    

    public void testUnauthenticated() throws Exception {
     
        //System.out.println("#### testUnauthenticated");
        HttpResponse response = rh.executeGetRequest("_search");
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        Thread.sleep(1500);
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("FAILED_LOGIN"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("/_search"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains(AuditMessage.UTC_TIMESTAMP));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("AUTHENTICATED"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
        
    }

    public void testJustAuthenticated() throws Exception {
        HttpResponse response = rh.executeGetRequest("", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertEquals(0, TestAuditlogImpl.messages.size());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    

    public void testSgIndexAttempt() throws Exception {
       
        HttpResponse response = rh.executePutRequest("searchguard/_doc/0", "{}", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("MISSING_PRIVILEGES"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("SG_INDEX_ATTEMPT"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("admin"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains(AuditMessage.UTC_TIMESTAMP));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("AUTHENTICATED"));
        Assert.assertEquals(2, TestAuditlogImpl.messages.size());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    

    public void testBadHeader() throws Exception {
      
        HttpResponse response = rh.executeGetRequest("", new BasicHeader("_sg_bad", "bad"), encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        Assert.assertFalse(TestAuditlogImpl.sb.toString(), TestAuditlogImpl.sb.toString().contains("AUTHENTICATED"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString(), TestAuditlogImpl.sb.toString().contains("BAD_HEADERS"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString(), TestAuditlogImpl.sb.toString().contains("_sg_bad"));
        Assert.assertEquals(TestAuditlogImpl.sb.toString(), 1, TestAuditlogImpl.messages.size());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    
    public void testMissingPriv() throws Exception {

        HttpResponse response = rh.executeGetRequest("sf/_search", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("MISSING_PRIVILEGES"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:data/read/search"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("worf"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("\"sf\""));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains(AuditMessage.UTC_TIMESTAMP));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("AUTHENTICATED"));
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
	public void testMsearch() throws Exception {
        
        String msearch = 
                "{\"index\":\"sf\", \"ignore_unavailable\": true}"+System.lineSeparator()+
                "{\"size\":0,\"query\":{\"match_all\":{}}}"+System.lineSeparator()+
                "{\"index\":\"sf\", \"ignore_unavailable\": true}"+System.lineSeparator()+
                "{\"size\":0,\"query\":{\"match_all\":{}}}"+System.lineSeparator();           
            
        //System.out.println("##### msaerch");
        HttpResponse response = rh.executePostRequest("_msearch?pretty", msearch, encodeBasicHeader("admin", "admin"));        
        Assert.assertEquals(response.getStatusReason(), HttpStatus.SC_OK, response.getStatusCode());
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString(), TestAuditlogImpl.sb.toString().contains("indices:data/read/msearch"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString(), TestAuditlogImpl.sb.toString().contains("indices:data/read/search"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString(), TestAuditlogImpl.sb.toString().contains("match_all"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_trace_task_id"));
        Assert.assertEquals(TestAuditlogImpl.sb.toString(), 4, TestAuditlogImpl.messages.size());
        Assert.assertFalse(TestAuditlogImpl.sb.toString().toLowerCase().contains("authorization"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
	}
	
	
    public void testBulkAuth() throws Exception {

        //System.out.println("#### testBulkAuth");
        String bulkBody = 
                "{ \"index\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value1\" }" +System.lineSeparator()+
                "{ \"index\" : { \"_index\" : \"worf\", \"_id\" : \"2\" } }"+System.lineSeparator()+
                "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
                
                "{ \"update\" : {\"_id\" : \"1\", \"_index\" : \"test\"} }"+System.lineSeparator()+
                "{ \"doc\" : {\"field\" : \"valuex\"} }"+System.lineSeparator()+
                "{ \"delete\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"create\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value3x\" }"+System.lineSeparator();
                

        HttpResponse response = rh.executePostRequest("_bulk", bulkBody, encodeBasicHeader("admin", "admin"));
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());  
        Assert.assertTrue(response.getBody().contains("\"errors\":false"));
        Assert.assertTrue(response.getBody().contains("\"status\":201"));                   
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:admin/auto_create"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:data/write/bulk"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("IndexRequest"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_trace_task_parent_id"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_trace_task_id"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("{ \\\"create\\\" : { \\\"_index\\\" : \\\"test\\\", \\\"_id\\\" : \\\"1\\\" } }"));
        //may vary because we log shardrequests which are not predictable here
        Assert.assertTrue(TestAuditlogImpl.messages.size() >= 17); 
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    public void testBulkNonAuth() throws Exception {

        String bulkBody = 
                "{ \"index\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value1\" }" +System.lineSeparator()+
                "{ \"index\" : { \"_index\" : \"worf\", \"_id\" : \"2\" } }"+System.lineSeparator()+
                "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
                
                "{ \"update\" : {\"_id\" : \"1\", \"_index\" : \"test\"} }"+System.lineSeparator()+
                "{ \"doc\" : {\"field\" : \"valuex\"} }"+System.lineSeparator()+
                "{ \"delete\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"create\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value3x\" }"+System.lineSeparator();

        HttpResponse response = rh.executePostRequest("_bulk", bulkBody, encodeBasicHeader("worf", "worf"));
        //System.out.println(response.getBody());

        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("\"errors\":true"));
        Assert.assertTrue(response.getBody().contains("\"status\":200")); 
        Assert.assertTrue(response.getBody().contains("\"status\":403"));   
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("MISSING_PRIVILEGES"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:data/write/bulk[s]"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("IndexRequest"));
        //may vary because we log shardrequests which are not predictable here
        Assert.assertTrue(TestAuditlogImpl.messages.size() >= 7);
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
	
    public void testUpdateSettings() throws Exception {
        
        String json = 
        "{"+
            "\"persistent\" : {"+
                "\"indices.recovery.max_bytes_per_sec\" : \"50mb\""+
            "},"+
            "\"transient\" : {"+
                "\"indices.recovery.max_bytes_per_sec\" : \"50mb\""+
             "}"+
        "}";

        HttpResponse response = rh.executePutRequest("_cluster/settings", json, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("AUTHENTICATED"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("cluster:admin/settings/update"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices.recovery.max_bytes_per_sec"));

        //may vary because we log may hit master directly or not
        Assert.assertTrue(TestAuditlogImpl.messages.size() > 1);
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testIndexPattern() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", "internal_elasticsearch")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_LOG_REQUEST_BODY, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_INDICES, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 10) //must be greater 0
                .put("searchguard.audit.config.index", "'auditlog-'YYYY.MM.dd.ss")
                .build();
        
        setup(additionalSettings);
        setupStarfleetIndex();

        final boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
        final String keystore = rh.keystore;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "auditlog/kirk-keystore.jks";
        HttpResponse res = rh.executeGetRequest("_cat/indices", new Header[0]);
        rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
        rh.keystore = keystore;

        Assert.assertTrue(res.getBody(), res.getBody().contains("auditlog-20"));
    }
    
    @Test
    public void testAliases() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        
        Client tc = getNodeClient();
        tc.admin().indices().create(new CreateIndexRequest("copysf")).actionGet();
        tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("starfleet").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("starfleet_academy").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("starfleet_library").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("klingonempire").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("public").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.index(new IndexRequest("spock").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("kirk").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("role01_role02").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.admin().indices().aliases(new IndicesAliasesRequest(DEFAULT_MASTER_TIMEOUT, DEFAULT_ACK_TIMEOUT).addAliasAction(AliasActions.add().indices("starfleet","starfleet_academy","starfleet_library").alias("sf"))).actionGet();
        tc.admin().indices().aliases(new IndicesAliasesRequest(DEFAULT_MASTER_TIMEOUT, DEFAULT_ACK_TIMEOUT).addAliasAction(AliasActions.add().indices("klingonempire","vulcangov").alias("nonsf"))).actionGet();
        tc.admin().indices().aliases(new IndicesAliasesRequest(DEFAULT_MASTER_TIMEOUT, DEFAULT_ACK_TIMEOUT).addAliasAction(AliasActions.add().indices("public").alias("unrestricted"))).actionGet();

        TestAuditlogImpl.clear();
        
        HttpResponse response = rh.executeGetRequest("sf/_search?pretty&ignore_unavailable=true", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("starfleet_academy"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("starfleet_library"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("starfleet"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("sf"));
        Assert.assertEquals(2, TestAuditlogImpl.messages.size());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testScroll() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        
        Client tc = getNodeClient();
        for(int i=0; i<3; i++)
        tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        TestAuditlogImpl.clear();
        
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executeGetRequest("vulcangov/_search?scroll=1m&pretty=true", encodeBasicHeader("admin", "admin"))).getStatusCode());
        int start = res.getBody().indexOf("_scroll_id") + 15;
        String scrollid = res.getBody().substring(start, res.getBody().indexOf("\"", start+1));
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executePostRequest("/_search/scroll?pretty=true", "{\"scroll_id\" : \""+scrollid+"\"}", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertEquals(4, TestAuditlogImpl.messages.size());
        
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executeGetRequest("vulcangov/_search?scroll=1m&pretty=true", encodeBasicHeader("admin", "admin"))).getStatusCode());
        start = res.getBody().indexOf("_scroll_id") + 15;
        scrollid = res.getBody().substring(start, res.getBody().indexOf("\"", start+1));
        TestAuditlogImpl.clear();
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res=rh.executePostRequest("/_search/scroll?pretty=true", "{\"scroll_id\" : \""+scrollid+"\"}", encodeBasicHeader("admin2", "admin"))).getStatusCode());
        Thread.sleep(1000);
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("InternalScrollSearchRequest"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("MISSING_PRIVILEGES"));
        Assert.assertTrue(TestAuditlogImpl.messages.size() > 2);
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testAliasResolution() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
      
       
        Client tc = getNodeClient();
        for(int i=0; i<3; i++)
        tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.admin().indices().aliases(new IndicesAliasesRequest(DEFAULT_MASTER_TIMEOUT, DEFAULT_ACK_TIMEOUT).addAliasAction(AliasActions.add().alias("thealias").index("vulcangov"))).actionGet();

        TestAuditlogImpl.clear();
        HttpResponse response = rh.executeGetRequest("thealias/_search?pretty&ignore_unavailable=true", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("thealias"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_trace_resolved_indices"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("vulcangov"));
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
        TestAuditlogImpl.clear();
    }
    
    @Test
    public void testAliasBadHeaders() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
           
        TestAuditlogImpl.clear();
        HttpResponse response = rh.executeGetRequest("_search?pretty", new BasicHeader("_sg_user", "xxx"), encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("YWRtaW46YWRtaW4"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("BAD_HEADERS"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("xxx"));
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
        TestAuditlogImpl.clear();
    }
    
    @Test
    public void testIndexCloseDelete() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        
        Client tc = getNodeClient();
        tc.admin().indices().create(new CreateIndexRequest("index1")).actionGet();
        tc.admin().indices().create(new CreateIndexRequest("index2")).actionGet();

        TestAuditlogImpl.clear();
        
        HttpResponse response = rh.executeDeleteRequest("index1?pretty", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executePostRequest("index2/_close?pretty", "", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        //System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:admin/close"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:admin/delete"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString(), TestAuditlogImpl.messages.size() >= 2);
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }

    @Test
    public void testDeleteByQuery() throws Exception {
        
        final Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .build();
        setup(settings);

        Client tc = getNodeClient();
        for(int i=0; i<3; i++)
        tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        TestAuditlogImpl.clear();
        
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executePostRequest("/vulcango*/_delete_by_query?refresh=true&wait_for_completion=true&pretty=true", "{\"query\" : {\"match_all\" : {}}}", encodeBasicHeader("admin", "admin"))).getStatusCode());
        assertContains(res, "*\"deleted\" : 3,*");
        String auditlogContents = TestAuditlogImpl.sb.toString();
        Assert.assertTrue(auditlogContents.contains("indices:data/write/delete/byquery"));
        Assert.assertTrue(auditlogContents.contains("indices:data/write/bulk"));
        Assert.assertTrue(auditlogContents.contains("indices:data/read/search"));
    }
}
