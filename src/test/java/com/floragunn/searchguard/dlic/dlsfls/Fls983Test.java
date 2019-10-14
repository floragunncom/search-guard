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

import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class Fls983Test extends AbstractDlsFlsTest{
    
    @Override
    protected void populateData(TransportClient tc) {
                
        tc.index(new IndexRequest(".kibana").type("config").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{}", XContentType.JSON)).actionGet();
    }
    
    @Test
    public void test() throws Exception {
        
        setup(new DynamicSgConfig().setSgRoles("sg_roles_983.yml"));

        HttpResponse res;
        
        String doc =  "{\"doc\" : {"+
            "\"x\" : \"y\""+
        "}}";
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePostRequest("/.kibana/config/0/_update?pretty", doc, encodeBasicHeader("human_resources_trainee", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("updated"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
    }
}