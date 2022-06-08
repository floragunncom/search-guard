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
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;

public class Fls983Test extends AbstractDlsFlsTest{
    
    @Override
    protected void populateData(Client tc) {
                
        tc.index(new IndexRequest(".kibana").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{}", XContentType.JSON)).actionGet();
    }
    
    @Test
    public void test() throws Exception {
        
        setup(new DynamicSgConfig().setSgRoles("sg_roles_983.yml"));
        
        String doc =  "{\"doc\" : {"+
            "\"x\" : \"y\""+
        "}}";
        
        HttpResponse res = rh.executePostRequest("/.kibana/_update/0?pretty", doc, encodeBasicHeader("human_resources_trainee", "password"));
        
        Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("updated"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
    }
}