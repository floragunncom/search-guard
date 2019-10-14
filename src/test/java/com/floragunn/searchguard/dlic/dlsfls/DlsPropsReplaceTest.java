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
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class DlsPropsReplaceTest extends AbstractDlsFlsTest{

    @Override
    protected void populateData(TransportClient tc) {



        tc.index(new IndexRequest("prop1").type("_doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"prop_replace\": \"yes\", \"amount\": 1010}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("prop1").type("_doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"prop_replace\": \"no\", \"amount\": 2020}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("prop2").type("_doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"role\": \"prole1\", \"amount\": 3030}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("prop2").type("_doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"role\": \"prole2\", \"amount\": 4040}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("prop2").type("_doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"role\": \"prole3\", \"amount\": 5050}", XContentType.JSON)).actionGet();

    }


    @Test
    public void testDlsProps() throws Exception {

        setup();

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/prop1,prop2/_search?pretty&size=100", encodeBasicHeader("admin", "admin"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"value\" : 5,\n      \"relation"));

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/prop1,prop2/_search?pretty&size=100", encodeBasicHeader("prop_replace", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"value\" : 3,\n      \"relation"));
    }
}