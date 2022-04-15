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

package com.floragunn.searchguard.dlic.dlsfls;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;

public class FieldMaskedTest extends AbstractDlsFlsTest{

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();    
    
    protected void populateData(Client tc) {

        tc.index(new IndexRequest("searchguard").id("config").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("config", FileHelper.readYamlContent("dlsfls/sg_config.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("internalusers")
                .source("internalusers", FileHelper.readYamlContent("dlsfls/sg_internal_users.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").id("roles").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("roles", FileHelper.readYamlContent("dlsfls/sg_roles.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("rolesmapping")
                .source("rolesmapping", FileHelper.readYamlContent("dlsfls/sg_roles_mapping.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("actiongroups")
                .source("actiongroups", FileHelper.readYamlContent("dlsfls/sg_action_groups.yml"))).actionGet();
        
        tc.index(new IndexRequest("deals").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust1\"}, \"ip_source\": \"100.100.1.1\",\"ip_dest\": \"123.123.1.1\",\"amount\": 10}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("deals").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust2\"}, \"ip_source\": \"100.100.2.2\",\"ip_dest\": \"123.123.2.2\",\"amount\": 20}", XContentType.JSON)).actionGet();

        
        for (int i=0; i<30;i++) {
            tc.index(new IndexRequest("deals").id("a"+i).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"customer\": {\"name\":\"cust1\"}, \"ip_source\": \"200.100.1.1\",\"ip_dest\": \"123.123.1.1\",\"amount\": 10}", XContentType.JSON)).actionGet();
        }
        
     }
    
    @Test
    public void testMaskedAggregations() throws Exception {

        setup();


        String query = "{"+
            "\"query\" : {"+
                 "\"match_all\": {}"+
            "},"+
            "\"aggs\" : {"+
                "\"ips\" : { \"terms\" : { \"field\" : \"ip_source.keyword\" } }"+
            "}"+
        "}";

        HttpResponse res;
        //Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty&size=0", query, encodeBasicHeader("admin", "admin"))).getStatusCode());
        //Assert.assertTrue(res.getBody().contains("100.100"));

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty&size=0", query, encodeBasicHeader("user_masked", "password"))).getStatusCode());
        Assert.assertFalse(res.getBody().contains("100.100"));

    }
    
    @Test
    public void testMaskedAggregationsRace() throws Exception {

        setup();


        String query = "{"+
            "\"aggs\" : {"+
                "\"ips\" : { \"terms\" : { \"field\" : \"ip_source.keyword\", \"size\": 1002, \"show_term_doc_count_error\": true } }"+
            "}"+
        "}";


        
            HttpResponse res;
            Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty&size=0", query, encodeBasicHeader("admin", "admin"))).getStatusCode());
            Assert.assertTrue(res.getBody().contains("100.100"));
            Assert.assertTrue(res.getBody().contains("200.100"));
            Assert.assertTrue(res.getBody().contains("\"doc_count\" : 30"));
            Assert.assertTrue(res.getBody().contains("\"doc_count\" : 1"));
            Assert.assertFalse(res.getBody().contains("e1623afebfa505884e249a478640ec98094d19a72ac7a89dd0097e28955bb5ae"));
            Assert.assertFalse(res.getBody().contains("26a8671e57fefc13504f8c61ced67ac98338261ace1e5bf462038b2f2caae16e"));
            Assert.assertFalse(res.getBody().contains("87873bdb698e5f0f60e0b02b76dad1ec11b2787c628edbc95b7ff0e82274b140"));
    
            Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty&size=0", query, encodeBasicHeader("user_masked", "password"))).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"doc_count\" : 30"));
            Assert.assertTrue(res.getBody().contains("\"doc_count\" : 1"));
            Assert.assertFalse(res.getBody().contains("100.100"));
            Assert.assertFalse(res.getBody().contains("200.100"));           
            Assert.assertTrue(res.getBody().contains("e1623afebfa505884e249a478640ec98094d19a72ac7a89dd0097e28955bb5ae"));
            Assert.assertTrue(res.getBody().contains("26a8671e57fefc13504f8c61ced67ac98338261ace1e5bf462038b2f2caae16e"));
            Assert.assertTrue(res.getBody().contains("87873bdb698e5f0f60e0b02b76dad1ec11b2787c628edbc95b7ff0e82274b140"));

        
        
        for(int i=0;i<10;i++) {
            Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty&size=0", query, encodeBasicHeader("admin", "admin"))).getStatusCode());
            Assert.assertTrue(res.getBody().contains("100.100"));
            Assert.assertTrue(res.getBody().contains("200.100"));
            Assert.assertTrue(res.getBody().contains("\"doc_count\" : 30"));
            Assert.assertTrue(res.getBody().contains("\"doc_count\" : 1"));
            Assert.assertFalse(res.getBody().contains("e1623afebfa505884e249a478640ec98094d19a72ac7a89dd0097e28955bb5ae"));
            Assert.assertFalse(res.getBody().contains("26a8671e57fefc13504f8c61ced67ac98338261ace1e5bf462038b2f2caae16e"));
            Assert.assertFalse(res.getBody().contains("87873bdb698e5f0f60e0b02b76dad1ec11b2787c628edbc95b7ff0e82274b140"));
        }

    }
    
    @Test
    public void testMaskedSearch() throws Exception {
        
        setup();

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=100", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 32"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertTrue(res.getBody().contains("cust2"));
        Assert.assertTrue(res.getBody().contains("100.100.1.1"));
        Assert.assertTrue(res.getBody().contains("100.100.2.2"));
        Assert.assertFalse(res.getBody().contains("87873bdb698e5f0f60e0b02b76dad1ec11b2787c628edbc95b7ff0e82274b140"));

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=100&q=100.100.1.1", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("100.100.1.1"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=100", encodeBasicHeader("user_masked", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 32"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertTrue(res.getBody().contains("cust2"));
        Assert.assertFalse(res.getBody().contains("100.100.1.1"));
        Assert.assertFalse(res.getBody().contains("100.100.2.2"));
        Assert.assertTrue(res.getBody().contains("87873bdb698e5f0f60e0b02b76dad1ec11b2787c628edbc95b7ff0e82274b140"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=100&q=100.100.1.1", encodeBasicHeader("user_masked", "password"))).getStatusCode());
        Assert.assertFalse(res.getBody(), res.getBody().contains("100.100.1.1"));
        Assert.assertFalse(res.getBody(), res.getBody().contains("ip_source"));

    }
    
    @Test
    public void testMaskedSearchLocalHash() throws Exception {
        
        Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_COMPLIANCE_LOCAL_HASHING_ENABLED, true).build();
        setup(settings);

        HttpResponse res;
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=100", encodeBasicHeader("user_masked", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 32"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertTrue(res.getBody().contains("cust2"));
        Assert.assertFalse(res.getBody().contains("100.100.1.1"));
        Assert.assertFalse(res.getBody().contains("100.100.2.2"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=100&q=100.100.1.1", encodeBasicHeader("user_masked", "password"))).getStatusCode());
        Assert.assertFalse(res.getBody(), res.getBody().contains("100.100.1.1"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("ip_source"));

    }
    
    @Test
    public void testMaskedSearchDCAggLocalHash() throws Exception {
        
        Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_COMPLIANCE_LOCAL_HASHING_ENABLED, true).build();
        setup(settings);

        HttpResponse res;
        
        String agg = "{\n"+
                "  \"query\": {\n" + 
                "    \"term\": {\n" + 
                "      \"customer.name\": \"cust1\"\n" + 
                "    }\n" + 
                "  },  \"aggs\" : {\n" + 
                "        \"type_count\" : {\n" + 
                "            \"cardinality\" : {\n" + 
                "                \"field\" : \"ip_source.keyword\"\n" + 
                "            }\n" + 
                "        }\n" + 
                "    }\n" + 
                "}";

        for(int i=0; i<15;i++) {
            Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty&size=0", agg, encodeBasicHeader("user_masked", "password"))).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 31"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
            Assert.assertTrue(res.getBody().contains("\"value\" : 2"));
        }

        System.out.println(rh.executeGetRequest("/deals/_stats/request_cache,query_cache?human&pretty", encodeBasicHeader("admin", "admin")).getBody());
        
        initialize(getPrivilegedInternalNodeClient(), new DynamicSgConfig().setSgConfig("sg_config_salt2_changed.yml"));

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty&size=0", agg, encodeBasicHeader("user_masked", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 31"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertTrue(res.getBody().contains("\"value\" : 2"));
        
    }
    
    @Test
    public void testMaskedGet() throws Exception {
        
        setup();

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_doc/0?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"found\" : true"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertFalse(res.getBody().contains("cust2"));
        Assert.assertTrue(res.getBody().contains("100.100.1.1"));
        Assert.assertFalse(res.getBody().contains("100.100.2.2"));
        Assert.assertFalse(res.getBody().contains("87873bdb698e5f0f60e0b02b76dad1ec11b2787c628edbc95b7ff0e82274b140"));

        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_doc/0?pretty", encodeBasicHeader("user_masked", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"found\" : true"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertFalse(res.getBody().contains("cust2"));
        Assert.assertFalse(res.getBody().contains("100.100.1.1"));
        Assert.assertFalse(res.getBody().contains("100.100.2.2"));
        Assert.assertTrue(res.getBody().contains("87873bdb698e5f0f60e0b02b76dad1ec11b2787c628edbc95b7ff0e82274b140"));
    }
    
    @Test
    public void testMaskedGetPrefix() throws Exception {
        
        Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_COMPLIANCE_MASK_PREFIX, "anon:").build();
        setup(settings);

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_doc/0?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"found\" : true"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertFalse(res.getBody().contains("cust2"));
        Assert.assertTrue(res.getBody().contains("100.100.1.1"));
        Assert.assertFalse(res.getBody().contains("100.100.2.2"));
        Assert.assertFalse(res.getBody().contains("87873bdb698e5f0f60e0b02b76dad1ec11b2787c628edbc95b7ff0e82274b140"));
        Assert.assertFalse(res.getBody().contains("anon:"));

        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_doc/0?pretty", encodeBasicHeader("user_masked", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"found\" : true"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertFalse(res.getBody().contains("cust2"));
        Assert.assertFalse(res.getBody().contains("100.100.1.1"));
        Assert.assertFalse(res.getBody().contains("100.100.2.2"));
        Assert.assertTrue(res.getBody().contains("anon:87873bdb698e5f0f60e0b02b76dad1ec11b2787c628edbc95b7ff0e82274b140"));
        Assert.assertTrue(res.getBody().contains("anon:"));
    }
    

}