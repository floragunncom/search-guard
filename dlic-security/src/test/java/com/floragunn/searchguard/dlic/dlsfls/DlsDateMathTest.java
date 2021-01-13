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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class DlsDateMathTest extends AbstractDlsFlsTest{


    @Override
    protected void populateData(TransportClient tc) {



        LocalDateTime yesterday = LocalDateTime.now(ZoneId.of("UTC")).minusDays(1);
        LocalDateTime today = LocalDateTime.now(ZoneId.of("UTC"));
        LocalDateTime tomorrow = LocalDateTime.now(ZoneId.of("UTC")).plusDays(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        
        tc.index(new IndexRequest("logstash").type("_doc").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"@timestamp\": \""+formatter.format(yesterday)+"\"}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash").type("_doc").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"@timestamp\": \""+formatter.format(today)+"\"}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash").type("_doc").id("3").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"@timestamp\": \""+formatter.format(tomorrow)+"\"}", XContentType.JSON)).actionGet();
    }

    
    @Test
    public void testDlsDateMathQuery() throws Exception {
        final Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_UNSUPPORTED_ALLOW_NOW_IN_DLS,true).build();
        setup(settings);

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/logstash/_search?pretty", encodeBasicHeader("date_math", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/logstash/_search?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"value\" : 3,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
    }
    
    @Test
    public void testDlsDateMathQueryNotAllowed() throws Exception {
        setup();

        HttpResponse res;

        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, (res = rh.executeGetRequest("/logstash/_search?pretty", encodeBasicHeader("date_math", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("'now' is not allowed in DLS queries"));
        Assert.assertTrue(res.getBody().contains("error"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/logstash/_search?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"value\" : 3,\n      \"relation"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
    }
}