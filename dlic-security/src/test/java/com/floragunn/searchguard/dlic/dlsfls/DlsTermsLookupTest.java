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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class DlsTermsLookupTest extends AbstractDlsFlsTest{


    @Override
    protected void populateData(TransportClient tc) {

        tc.index(new IndexRequest("deals").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"amount\": 10, \"acodes\": [6,7]}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("deals").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"amount\": 1500, \"acodes\": [1]}", XContentType.JSON)).actionGet();
        
        tc.index(new IndexRequest("users").id("sg_dls_lookup_user1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"acode\": [1,2,4]}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("users").id("sg_dls_lookup_user2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"acode\": [2,3]}", XContentType.JSON)).actionGet();
    }
    
    protected void setupWithTLQAllowed() throws Exception {
    	setup(Settings.builder().put(ConfigConstants.SEARCHGUARD_UNSUPPORTED_ALLOW_TLQ_IN_DLS, true).build());
    }

    
    @Test
    public void testDlsWithTermsLookupMatchAll() throws Exception {

        setupWithTLQAllowed();

        HttpResponse res;
        
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/dea*/_search?pretty&size=0", encodeBasicHeader("admin", "admin"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 2,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/dea*/_search?pretty", encodeBasicHeader("sg_dls_lookup_user1", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

    }
    
    @Test
    public void testDlsWithTermsLookup() throws Exception {

        setupWithTLQAllowed();

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/dea*/_search?pretty&q=_index:dea*", encodeBasicHeader("sg_dls_lookup_user1", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody(), res.getBody().contains("\"value\" : 1,\n      \"relation"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("\"failed\" : 0"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/dea*/_search?pretty&q=_index:unknownindex", encodeBasicHeader("sg_dls_lookup_user1", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody(), res.getBody().contains("\"value\" : 0,\n      \"relation"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("\"failed\" : 0"));

    }

    @Test
    public void testDlsWithTermsLookupGet() throws Exception {

        setupWithTLQAllowed();

        HttpResponse res = null;
        
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, (res = rh.executeGetRequest("/deals/_doc/0?pretty", encodeBasicHeader("sg_dls_lookup_user1", "password"))).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, (res = rh.executeGetRequest("/deals/_doc/1?pretty", encodeBasicHeader("sg_dls_lookup_user1", "password"))).getStatusCode());
    
        Assert.assertTrue(res.getBody().contains("async actions are left after rewrite"));
    }
    
    @Test
    public void testDlsWithTermsLookupGetTLQNA() throws Exception {

        setup();

        HttpResponse res = null;
        
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, (res = rh.executeGetRequest("/deals/_doc/0?pretty", encodeBasicHeader("sg_dls_lookup_user1", "password"))).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, (res = rh.executeGetRequest("/deals/_doc/1?pretty", encodeBasicHeader("sg_dls_lookup_user1", "password"))).getStatusCode());
    
        Assert.assertTrue(res.getBody().contains("async actions are left after rewrite"));
    }
}