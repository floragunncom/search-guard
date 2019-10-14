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

import java.io.IOException;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class FlsFieldsWcTest extends AbstractDlsFlsTest{
    
    @Override
    protected void populateData(TransportClient tc) {


        
        tc.admin().indices().create(new CreateIndexRequest("deals")
        .mapping("deals", "timestamp","type=date","@timestamp","type=date")).actionGet();
        
        try {
            String doc = FileHelper.loadFile("dlsfls/doc1.json");

            for (int i = 0; i < 10; i++) {
                final String moddoc = doc.replace("<name>", "cust" + i).replace("<employees>", "" + i).replace("<date>", "1970-01-02");
                tc.index(new IndexRequest("deals").type("deals").id("0" + i).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(moddoc, XContentType.JSON)).actionGet();
            }

        } catch (IOException e) {
            Assert.fail(e.toString());
        }

    }
    
    
    @Test
    public void testFields() throws Exception {        
        setup();

        String query = FileHelper.loadFile("dlsfls/flsquery.json");
        
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

        String query = FileHelper.loadFile("dlsfls/flsquery2.json");
        
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