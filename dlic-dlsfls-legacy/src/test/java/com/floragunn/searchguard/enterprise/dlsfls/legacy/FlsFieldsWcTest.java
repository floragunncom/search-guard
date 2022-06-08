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

import java.io.IOException;

import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentType;

import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;


public class FlsFieldsWcTest extends AbstractDlsFlsTest{
    
    @Override
    protected void populateData(Client tc) {


        
        tc.admin().indices().create(new CreateIndexRequest("deals")
        .simpleMapping("timestamp","type=date","@timestamp","type=date")).actionGet();
        
        try {
            String doc = FileHelper.loadFile("dlsfls_legacy/doc1.json");

            for (int i = 0; i < 10; i++) {
                final String moddoc = doc.replace("<name>", "cust" + i).replace("<employees>", "" + i).replace("<date>", "1970-01-02");
                tc.index(new IndexRequest("deals").id("0" + i).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(moddoc, XContentType.JSON)).actionGet();
            }

        } catch (IOException e) {
            Assert.fail(e.toString());
        }

    }
    
    
    @Test
    public void testFields() throws Exception {        
        setup();

        String query = FileHelper.loadFile("dlsfls_legacy/flsquery.json");
        
        HttpResponse res;        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query, encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("secret"));
        Assert.assertTrue(res.getBody().contains("@timestamp"));
        Assert.assertTrue(res.getBody().contains("\"timestamp"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query, encodeBasicHeader("fls_fields_wc", "password"))).getStatusCode());
        Assert.assertFalse(res.getBody().contains("customer"));
        Assert.assertFalse(res.getBody().contains("secret"));
        Assert.assertFalse(res.getBody().contains("timestamp"));
        Assert.assertFalse(res.getBody().contains("numfield5"));
    }
    
    @Test
    public void testFields2() throws Exception {        
        setup();

        String query = FileHelper.loadFile("dlsfls_legacy/flsquery2.json");
        
        HttpResponse res;        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query, encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("secret"));
        Assert.assertTrue(res.getBody().contains("@timestamp"));
        Assert.assertTrue(res.getBody().contains("\"timestamp"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query, encodeBasicHeader("fls_fields_wc", "password"))).getStatusCode());
        Assert.assertFalse(res.getBody().contains("customer"));
        Assert.assertFalse(res.getBody().contains("secret"));
        Assert.assertFalse(res.getBody().contains("timestamp"));
        Assert.assertTrue(res.getBody().contains("numfield5"));
    }
}