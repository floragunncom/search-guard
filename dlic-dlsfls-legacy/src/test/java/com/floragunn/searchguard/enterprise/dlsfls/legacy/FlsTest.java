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

package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;

public class FlsTest extends AbstractDlsFlsTest{
    
    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @Override
    protected void populateData(Client tc) {


                
        tc.index(new IndexRequest("deals").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust1\"}, \"zip\": \"12345\",\"secret\": \"tellnoone\",\"amount\": 10}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("deals").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"customer\": {\"name\":\"cust2\", \"ctype\":\"industry\"}, \"amount\": 1500}", XContentType.JSON)).actionGet();
    }
    
    @Test
    public void testFieldCapabilities() throws Exception {
        
        setup();

        HttpResponse res;
        

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_field_caps?fields=*&pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("customer"));
        Assert.assertTrue(res.getBody().contains("customer.name"));
        Assert.assertTrue(res.getBody().contains("zip"));
        Assert.assertTrue(res.getBody().contains("ctype"));
        Assert.assertTrue(res.getBody().contains("amount"));
        Assert.assertTrue(res.getBody().contains("secret"));

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_field_caps?fields=*&pretty", encodeBasicHeader("dept_manager_fls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("customer"));
        Assert.assertTrue(res.getBody().contains("customer.name"));
        Assert.assertTrue(res.getBody().contains("zip"));
        Assert.assertTrue(res.getBody().contains("ctype"));
        Assert.assertFalse(res.getBody().contains("amount"));
        Assert.assertFalse(res.getBody().contains("secret"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_field_caps?fields=*&pretty", encodeBasicHeader("dept_manager_fls_reversed_fields", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertFalse(res.getBody().contains("customer"));
        Assert.assertFalse(res.getBody().contains("customer.name"));
        Assert.assertFalse(res.getBody().contains("zip"));
        Assert.assertFalse(res.getBody().contains("ctype"));
        Assert.assertTrue(res.getBody().contains("amount"));
        Assert.assertTrue(res.getBody().contains("secret"));
    }
    
    @Test
    public void testMapping() throws Exception {
        
        setup();

        HttpResponse res;
        

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_mapping?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("customer"));
        Assert.assertTrue(res.getBody().contains("name"));
        Assert.assertTrue(res.getBody().contains("zip"));
        Assert.assertTrue(res.getBody().contains("ctype"));
        Assert.assertTrue(res.getBody().contains("amount"));
        Assert.assertTrue(res.getBody().contains("secret"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_mapping?pretty", encodeBasicHeader("dept_manager_fls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("customer"));
        Assert.assertTrue(res.getBody().contains("name"));
        Assert.assertTrue(res.getBody().contains("zip"));
        Assert.assertTrue(res.getBody().contains("ctype"));
        Assert.assertFalse(res.getBody().contains("amount"));
        Assert.assertFalse(res.getBody().contains("secret"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_mapping?pretty", encodeBasicHeader("dept_manager_fls_reversed_fields", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("customer"));
        Assert.assertFalse(res.getBody().contains("name"));
        Assert.assertFalse(res.getBody().contains("zip"));
        Assert.assertFalse(res.getBody().contains("ctype"));
        Assert.assertTrue(res.getBody().contains("amount"));
        Assert.assertTrue(res.getBody().contains("secret"));
    }
    
    
    @Test
    public void testFlsSearch() throws Exception {
        
        setup();

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 2,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertTrue(res.getBody().contains("cust2"));
        Assert.assertTrue(res.getBody().contains("zip"));
        Assert.assertTrue(res.getBody().contains("ctype"));
        Assert.assertTrue(res.getBody().contains("amount"));
        Assert.assertTrue(res.getBody().contains("secret"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty", encodeBasicHeader("dept_manager_fls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 2,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertTrue(res.getBody().contains("cust2"));
        Assert.assertTrue(res.getBody().contains("zip"));
        Assert.assertTrue(res.getBody().contains("ctype"));
        Assert.assertFalse(res.getBody().contains("amount"));
        Assert.assertFalse(res.getBody().contains("secret"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty", encodeBasicHeader("dept_manager_fls_reversed_fields", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 2,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertFalse(res.getBody().contains("cust1"));
        Assert.assertFalse(res.getBody().contains("cust2"));
        Assert.assertFalse(res.getBody().contains("zip"));
        Assert.assertFalse(res.getBody().contains("ctype"));
        Assert.assertTrue(res.getBody().contains("amount"));
        Assert.assertTrue(res.getBody().contains("secret"));
    }
    
    @Test
    public void testFlsGet() throws Exception {
        
        setup();

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_doc/0?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"found\" : true"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertFalse(res.getBody().contains("cust2"));
        Assert.assertTrue(res.getBody().contains("zip"));
        Assert.assertFalse(res.getBody().contains("ctype"));
        Assert.assertTrue(res.getBody().contains("amount"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_doc/0?pretty", encodeBasicHeader("dept_manager_fls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"found\" : true"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertFalse(res.getBody().contains("cust2"));
        Assert.assertTrue(res.getBody().contains("zip"));
        Assert.assertFalse(res.getBody().contains("ctype"));
        Assert.assertFalse(res.getBody().contains("amount"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_doc/0?realtime=true&pretty", encodeBasicHeader("dept_manager_fls", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"found\" : true"));
        Assert.assertTrue(res.getBody().contains("cust1"));
        Assert.assertFalse(res.getBody().contains("cust2"));
        Assert.assertTrue(res.getBody().contains("zip"));
        Assert.assertFalse(res.getBody().contains("ctype"));
        Assert.assertFalse(res.getBody().contains("amount"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_doc/0?realtime=true&pretty", encodeBasicHeader("dept_manager_fls_reversed_fields", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"found\" : true"));
        Assert.assertFalse(res.getBody().contains("cust1"));
        Assert.assertFalse(res.getBody().contains("cust2"));
        Assert.assertFalse(res.getBody().contains("zip"));
        Assert.assertFalse(res.getBody().contains("ctype"));
        Assert.assertTrue(res.getBody().contains("amount"));
        Assert.assertTrue(res.getBody().contains("secret"));
    }
    
 
}