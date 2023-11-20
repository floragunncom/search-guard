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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;

public class DlsScrollTest extends AbstractDlsFlsTest{

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @Override
    protected void populateData(Client tc) {

        tc.index(new IndexRequest("deals").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"amount\": 3}", XContentType.JSON)).actionGet(); //not in
        
        tc.index(new IndexRequest("deals").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"amount\": 10}", XContentType.JSON)).actionGet(); //not in
        
        tc.index(new IndexRequest("deals").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"amount\": 1500}", XContentType.JSON)).actionGet();
        
        tc.index(new IndexRequest("deals").id("4").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"amount\": 21500}", XContentType.JSON)).actionGet(); //not in

        for(int i=0; i<100; i++) {
            tc.index(new IndexRequest("deals").id("gen"+i).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"amount\": 1500}", XContentType.JSON)).actionGet();
        }
    }


    @Test
    public void testDlsScroll() throws Exception {

        setup();

        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executeGetRequest("/deals/_search?scroll=1m&pretty=true&size=5", encodeBasicHeader("dept_manager", "password"))).getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"value\" : 101,"));
        
        int c=0;
        
        while(true) {
            int start = res.getBody().indexOf("_scroll_id") + 15;
            String scrollid = res.getBody().substring(start, res.getBody().indexOf("\"", start+1));
            Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executePostRequest("/_search/scroll?pretty=true", "{\"scroll\" : \"1m\", \"scroll_id\" : \""+scrollid+"\"}", encodeBasicHeader("dept_manager", "password"))).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 101,"));
            Assert.assertFalse(res.getBody().contains("\"amount\" : 3"));
            Assert.assertFalse(res.getBody().contains("\"amount\" : 10"));
            System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("\"amount\" : 21500"));
            c++;
            
            if(res.getBody().contains("\"hits\" : [ ]")) {
                break;
            }
        }

        Assert.assertEquals(21, c);
    }
}