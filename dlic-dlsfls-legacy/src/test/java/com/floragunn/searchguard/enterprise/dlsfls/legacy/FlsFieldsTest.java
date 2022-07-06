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

import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FlsFieldsTest extends AbstractDlsFlsTest{
    
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

        } catch (Exception e) {
            Assert.fail(e.toString());
        }

    }
    
    
    @Test
    public void testFields() throws Exception {        
        setup();

        String query = FileHelper.loadFile("dlsfls_legacy/flsquery.json");
        
        RestHelper.HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query, encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody(), res.getBody().contains("secret"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("@timestamp"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("\"timestamp"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("numfield5"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty", query, encodeBasicHeader("fls_fields", "password"))).getStatusCode());
        Assert.assertFalse(res.getBody(), res.getBody().contains("customer"));
        Assert.assertFalse(res.getBody(), res.getBody().contains("secret"));
        Assert.assertFalse(res.getBody(), res.getBody().contains("timestamp"));
        Assert.assertFalse(res.getBody(), res.getBody().contains("numfield5"));
    }
    
    @Test
    public void testFields2() throws Exception {        
        setup();

        String query = FileHelper.loadFile("dlsfls_legacy/flsquery2.json");
        
        RestHelper.HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty=true", query, encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody(), res.getBody().contains("secret"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("@timestamp"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("\"timestamp"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/deals/_search?pretty=true", query, encodeBasicHeader("fls_fields", "password"))).getStatusCode());
        Assert.assertFalse(res.getBody(), res.getBody().contains("customer"));
        Assert.assertFalse(res.getBody(), res.getBody().contains("secret"));
        Assert.assertFalse(res.getBody(), res.getBody().contains("timestamp"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("numfield5"));
    }
}