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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public class DFMFieldMaskedTest extends AbstractDlsFlsTest{
    
    
    protected void populateData(TransportClient tc) {

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
        
        tc.index(new IndexRequest("deals-0").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust1\"}, \"ip_source\": \"100.100.1.1\",\"ip_dest\": \"123.123.1.1\",\"amount\": 10}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("deals-1").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust2\"}, \"ip_source\": \"100.100.2.2\",\"ip_dest\": \"123.123.2.2\",\"amount\": 20}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("deals-2").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust3\"}, \"ip_source\": \"100.100.2.3\",\"ip_dest\": \"123.123.3.2\",\"amount\": 30}", XContentType.JSON)).actionGet();

        tc.index(new IndexRequest("deals-outdated-1").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust4\"}, \"ip_source\": \"100.100.32.1\",\"ip_dest\": \"123.123.4.2\",\"amount\": 100}", XContentType.JSON)).actionGet();

        tc.index(new IndexRequest("deals-outdated-2").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust5\"}, \"ip_source\": \"100.100.3.2\",\"ip_dest\": \"123.123.5.2\",\"amount\": 200}", XContentType.JSON)).actionGet();

        tc.index(new IndexRequest("deals-outdated-3").id("3").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust6\"}, \"ip_source\": \"100.100.3.3\",\"ip_dest\": \"123.123.6.2\",\"amount\": 300}", XContentType.JSON)).actionGet();

        tc.index(new IndexRequest("index1-1").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"field1\": 1, \"field2\": \"value-2-1\", \"field3\": \"value-3-1\", \"field4\": \"value-4-1\" }", XContentType.JSON)).actionGet();

        tc.index(new IndexRequest("index1-2").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"field1\": 2, \"field2\": \"value-2-2\", \"field3\": \"value-3-2\", \"field4\": \"value-4-2\" }", XContentType.JSON)).actionGet();
        
        tc.index(new IndexRequest("index1-3").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"field1\": 3, \"field2\": \"value-2-3\", \"field3\": \"value-3-3\", \"field4\": \"value-4-3\" }", XContentType.JSON)).actionGet();
        
        tc.index(new IndexRequest("index1-4").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"field1\": 4, \"field2\": \"value-2-4\", \"field3\": \"value-3-4\", \"field4\": \"value-4-4\" }", XContentType.JSON)).actionGet();
                                    
     }
    
    @Test
    public void testMaskedSearch() throws Exception {
        
        final Settings settings = Settings.builder().build();
        setup(settings);

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals-outdated-*/_search?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        System.out.println(res.getBody());

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals-outdated-*/_search?pretty", encodeBasicHeader("dfm_user", "password"))).getStatusCode());
        System.out.println(res.getBody());

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals-*/_search?pretty", encodeBasicHeader("dfm_user", "password"))).getStatusCode());
        System.out.println(res.getBody());
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals*/_search?pretty", encodeBasicHeader("dfm_user", "password"))).getStatusCode());
        System.out.println(res.getBody());
    }

    @Test
    public void testDFMUnrestrictedUser() throws Exception {
    	// admin user sees all
        final Settings settings = Settings.builder().build();
        setup(settings);

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/index1-*/_search?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        System.out.println(res.getBody());
    
        // the only document in index1-1 is filtered by DLS query, so normally no hit in index-1-1
        Assert.assertTrue(res.getBody().contains("index1-1"));        

        // field3 and field4 - normally filtered out by FLS
        Assert.assertTrue(res.getBody().contains("value-3-1"));
        Assert.assertTrue(res.getBody().contains("value-4-1"));
        Assert.assertTrue(res.getBody().contains("value-3-2"));
        Assert.assertTrue(res.getBody().contains("value-4-2"));
        Assert.assertTrue(res.getBody().contains("value-3-3"));
        Assert.assertTrue(res.getBody().contains("value-4-3"));
        Assert.assertTrue(res.getBody().contains("value-3-4"));
        Assert.assertTrue(res.getBody().contains("value-4-4"));
        
        // field2 - normally masked
        Assert.assertTrue(res.getBody().contains("value-2-1"));
        Assert.assertTrue(res.getBody().contains("value-2-2"));
        Assert.assertTrue(res.getBody().contains("value-2-3"));
        Assert.assertTrue(res.getBody().contains("value-2-4"));        
    }

    
    @Test
    public void testDFMRestrictedUser() throws Exception {
        // tests that the DFM settings are applied. User has only one role
    	// with D/F/M all enabled, so restrictions must kick in
        final Settings settings = Settings.builder().build();
        setup(settings);

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/_searchguard/authinfo?pretty", encodeBasicHeader("dfm_restricted_role", "password"))).getStatusCode());
        System.out.println(res.getBody());

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/index1-*/_search?pretty", encodeBasicHeader("dfm_restricted_role", "password"))).getStatusCode());
        System.out.println(res.getBody());
    
        // the only document in index1-1 is filtered by DLS query, so  no hit in index-1-1
        Assert.assertFalse(res.getBody().contains("index1-1"));        
      
        // field3 and field4 -  filtered out by FLS
        Assert.assertFalse(res.getBody().contains("value-3-1"));
        Assert.assertFalse(res.getBody().contains("value-4-1"));
        Assert.assertFalse(res.getBody().contains("value-3-2"));
        Assert.assertFalse(res.getBody().contains("value-4-2"));
        Assert.assertFalse(res.getBody().contains("value-3-3"));
        Assert.assertFalse(res.getBody().contains("value-4-3"));
        Assert.assertFalse(res.getBody().contains("value-3-4"));
        Assert.assertFalse(res.getBody().contains("value-4-4"));
        
        // field2 - normally masked
        Assert.assertFalse(res.getBody().contains("value-2-1"));
        Assert.assertFalse(res.getBody().contains("value-2-2"));
        Assert.assertFalse(res.getBody().contains("value-2-3"));
        Assert.assertFalse(res.getBody().contains("value-2-4")); 

        // field2 - check also some masked vallues
        Assert.assertTrue(res.getBody().contains("514b27191e2322b0f7cd6afc3a5d657ff438fd0cc8dc229bd1a589804fdffd99"));
        Assert.assertTrue(res.getBody().contains("3090f7e867f390fb96b20ba30ee518b09a927b857393ebd1262f31191a385efa"));
    }

    @Test
    public void testDFMRestrictedAndUnrestrictedAllIndices() throws Exception {

    	// user has the restricted role as in test testDFMRestrictedUser(). In addition, user has
    	// another role with the same index pattern as the restricted role but no DFM settings. In that
    	// case the unrestricted role should trump the restricted one, so basically user has 
    	// full access again. 
        final Settings settings = Settings.builder().build();
        setup(settings);

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/index1-*/_search?pretty", encodeBasicHeader("dfm_restricted_and_unrestricted_all_indices_role", "password"))).getStatusCode());
        System.out.println(res.getBody());
    
        // the only document in index1-1 is filtered by DLS query, so normally no hit in index-1-1
        Assert.assertTrue(res.getBody().contains("index1-1"));        

        // field3 and field4 - normally filtered out by FLS
        Assert.assertTrue(res.getBody().contains("value-3-1"));
        Assert.assertTrue(res.getBody().contains("value-4-1"));
        Assert.assertTrue(res.getBody().contains("value-3-2"));
        Assert.assertTrue(res.getBody().contains("value-4-2"));
        Assert.assertTrue(res.getBody().contains("value-3-3"));
        Assert.assertTrue(res.getBody().contains("value-4-3"));
        Assert.assertTrue(res.getBody().contains("value-3-4"));
        Assert.assertTrue(res.getBody().contains("value-4-4"));
        
        // field2 - normally masked
        Assert.assertTrue(res.getBody().contains("value-2-1"));
        Assert.assertTrue(res.getBody().contains("value-2-2"));
        Assert.assertTrue(res.getBody().contains("value-2-3"));
        Assert.assertTrue(res.getBody().contains("value-2-4"));        
    }
    
    @Test
    public void testDFMRestrictedAndUnrestrictedOneIndex() throws Exception {

    	// user has the restricted role as in test testDFMRestrictedUser(). In addition, user has
    	// another role where the index pattern matches two specific index ("index1-2", "index-1-1"), means this role has two indices
    	// which are more specific than the index pattern in the restricted role ("index1-*"), So the second role should
    	// remove the DMF restrictions from exactly two indices. Otherwise, restrictions still apply.
        final Settings settings = Settings.builder().build();
        setup(settings);

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/_searchguard/authinfo?pretty", encodeBasicHeader("dfm_restricted_and_unrestricted_one_index_role", "password"))).getStatusCode());
        System.out.println(res.getBody());
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/index1-*/_search?pretty", encodeBasicHeader("dfm_restricted_and_unrestricted_one_index_role", "password"))).getStatusCode());
        System.out.println(res.getBody());
    
        // we have a role that places no restrictions on index-1-1, lifting the DLS from the restricted role
        // So we expect one unrestricted hit in this index
        Assert.assertTrue(res.getBody().contains("index1-1"));
        Assert.assertTrue(res.getBody().contains("value-2-1"));
        Assert.assertTrue(res.getBody().contains("value-3-1"));
        Assert.assertTrue(res.getBody().contains("value-4-1"));
        
        // field3 and field4 - normally filtered out by FLS. Secondary role
        // lifts restrictions for insex1-1 and index1-4, so only those
        // values should be visible for index1-1 and index1-4
        Assert.assertTrue(res.getBody().contains("value-3-1"));
        Assert.assertTrue(res.getBody().contains("value-4-1"));
        Assert.assertTrue(res.getBody().contains("value-3-4"));
        Assert.assertTrue(res.getBody().contains("value-4-4"));

        // FLS restrictions still in place for index1-2 and index1-3, those
        // fields must not be present
        Assert.assertFalse(res.getBody().contains("value-3-2"));
        Assert.assertFalse(res.getBody().contains("value-4-2"));
        Assert.assertFalse(res.getBody().contains("value-3-3"));
        Assert.assertFalse(res.getBody().contains("value-4-3"));

        // field2 - normally masked, but for index1-1 and index1-4 restrictions are
        // lifted by secondary role, so we have cleartext in index1-1 and index1-4 
        Assert.assertTrue(res.getBody().contains("value-2-1"));
        Assert.assertTrue(res.getBody().contains("value-2-4"));   
        
        // but we still have masked values for index1-2 and index1-3, check
        // for actual masked values
        Assert.assertTrue(res.getBody().contains("514b27191e2322b0f7cd6afc3a5d657ff438fd0cc8dc229bd1a589804fdffd99"));
        Assert.assertTrue(res.getBody().contains("3090f7e867f390fb96b20ba30ee518b09a927b857393ebd1262f31191a385efa")); 
    }    
}