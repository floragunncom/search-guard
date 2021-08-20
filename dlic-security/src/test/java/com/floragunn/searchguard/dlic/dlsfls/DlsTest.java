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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;

public class DlsTest {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().resources("dlsfls").build();

    @BeforeClass
    public static void setupTestData() {
        try (Client client = cluster.getInternalNodeClient()) {

            client.index(new IndexRequest("deals").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"amount\": 10}", XContentType.JSON))
                    .actionGet();
            client.index(new IndexRequest("deals").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"amount\": 1500}", XContentType.JSON))
                    .actionGet();
        }
    }

    @Test
    public void testModuleInfo() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("admin", "admin")) {
            HttpResponse response = client.get("/_searchguard/license");

            try {
                Assert.assertFalse(response.getBody(), response.toJsonNode().path("modules").path("DLSFLS").isMissingNode());
            } catch (Exception e) {
                System.err.println("Error while parsing: " + response.getBody());
                throw e;
            }
        }
    }

    @Test
    public void testDlsAggregations() throws Exception {

        String query = "{" + "\"query\" : {" + "\"match_all\": {}" + "}," + "\"aggs\" : {" + "\"thesum\" : { \"sum\" : { \"field\" : \"amount\" } }"
                + "}" + "}";

        try (GenericRestClient client = cluster.getRestClient("dept_manager", "password")) {

            GenericRestClient.HttpResponse response = client.postJson("/deals/_search?pretty", query);

            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("\"value\" : 1,\n      \"relation"));
            Assert.assertTrue(response.getBody().contains("\"value\" : 1500.0"));
            Assert.assertTrue(response.getBody().contains("\"failed\" : 0"));
        }

        try (GenericRestClient client = cluster.getRestClient("admin", "admin")) {

            GenericRestClient.HttpResponse response = client.postJson("/deals/_search?pretty", query);

            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("\"value\" : 2,\n      \"relation"));
            Assert.assertTrue(response.getBody().contains("\"value\" : 1510.0"));
            Assert.assertTrue(response.getBody().contains("\"failed\" : 0"));
        }
    }

    @Test
    public void testDlsTermVectors() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("dept_manager", "password")) {
            GenericRestClient.HttpResponse response = client.get("/deals/_doc/0/_termvectors?pretty=true");
            Assert.assertTrue(response.getBody().contains("\"found\" : false"));
        }

        try (GenericRestClient client = cluster.getRestClient("admin", "admin")) {
            GenericRestClient.HttpResponse response = client.get("/deals/_doc/0/_termvectors?pretty=true");
            Assert.assertTrue(response.getBody(), response.getBody().contains("\"found\" : true"));
        }
    }

    @Test
    public void testDls() throws Exception {

        try (GenericRestClient dmClient = cluster.getRestClient("dept_manager", "password");
                GenericRestClient adminClient = cluster.getRestClient("admin", "admin")) {
            GenericRestClient.HttpResponse res;

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.get("/deals/_search?pretty&size=0")).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.get("/deals/_search?pretty")).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            Assert.assertEquals(HttpStatus.SC_OK, (res = adminClient.get("/deals/_search?pretty")).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 2,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            Assert.assertEquals(HttpStatus.SC_OK, (res = adminClient.get("/deals/_search?pretty&size=0")).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 2,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            String query =

                    "{" + "\"query\": {" + "\"range\" : {" + "\"amount\" : {" + "\"gte\" : 8," + "\"lte\" : 20," + "\"boost\" : 3.0" + "}" + "}" + "}"
                            + "}";

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.postJson("/deals/_search?pretty", query)).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 0,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            query =

                    "{" + "\"query\": {" + "\"range\" : {" + "\"amount\" : {" + "\"gte\" : 100," + "\"lte\" : 2000," + "\"boost\" : 2.0" + "}" + "}"
                            + "}" + "}";

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.postJson("/deals/_search?pretty", query)).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            Assert.assertEquals(HttpStatus.SC_OK, (res = adminClient.postJson("/deals/_search?pretty", query)).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.postJson("/deals/_search?q=amount:10&pretty", query)).getStatusCode());
            Assert.assertTrue(res.getBody(), res.getBody().contains("\"value\" : 0,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            res = dmClient.get("/deals/_doc/0?pretty");
            Assert.assertTrue(res.getBody().contains("\"found\" : false"));

            res = dmClient.get("/deals/_doc/0?realtime=true&pretty");
            Assert.assertTrue(res.getBody().contains("\"found\" : false"));

            res = dmClient.get("/deals/_doc/1?pretty");
            Assert.assertTrue(res.getBody().contains("\"found\" : true"));

            Assert.assertEquals(HttpStatus.SC_OK, (res = adminClient.get("/deals/_count?pretty")).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"count\" : 2,"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.get("/deals/_count?pretty")).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"count\" : 1,"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            //mget
            //msearch
            String msearchBody = "{\"index\":\"deals\", \"ignore_unavailable\": true}" + System.lineSeparator()
                    + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator()
                    + "{\"index\":\"deals\", \"ignore_unavailable\": true}" + System.lineSeparator()
                    + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator();

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.postJson("_msearch?pretty", msearchBody)).getStatusCode());
            Assert.assertFalse(res.getBody().contains("_sg_dls_query"));
            Assert.assertFalse(res.getBody().contains("_sg_fls_fields"));
            Assert.assertTrue(res.getBody(), res.getBody().contains("\"amount\" : 1500"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            String mgetBody = "{" + "\"docs\" : [" + "{" + "\"_index\" : \"deals\"," + "\"_id\" : \"1\"" + " }," + " {" + "\"_index\" : \"deals\","
                    + " \"_id\" : \"2\"" + "}" + "]" + "}";

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.postJson("_mget?pretty", mgetBody)).getStatusCode());
            Assert.assertFalse(res.getBody().contains("_sg_dls_query"));
            Assert.assertFalse(res.getBody().contains("_sg_fls_fields"));
            Assert.assertTrue(res.getBody().contains("amount"));
            Assert.assertTrue(res.getBody().contains("\"found\" : false"));
        }

    }

    @Test
    public void testNonDls() throws Exception {

        try (GenericRestClient dmClient = cluster.getRestClient("dept_manager", "password")) {

            HttpResponse res;
            String query =

                    "{" + "\"_source\": false," + "\"query\": {" + "\"range\" : {" + "\"amount\" : {" + "\"gte\" : 100," + "\"lte\" : 2000,"
                            + "\"boost\" : 2.0" + "}" + "}" + "}" + "}";

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.postJson("/deals/_search?pretty", query)).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        }

    }

    @Test
    public void testDlsCache() throws Exception {

        try (GenericRestClient dmClient = cluster.getRestClient("dept_manager", "password");
                GenericRestClient adminClient = cluster.getRestClient("admin", "admin")) {

            HttpResponse res;
            Assert.assertEquals(HttpStatus.SC_OK, (res = adminClient.get("/deals/_search?pretty")).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 2,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            Assert.assertEquals(HttpStatus.SC_OK, (res = dmClient.get("/deals/_search?pretty")).getStatusCode());
            Assert.assertTrue(res.getBody().contains("\"value\" : 1,\n      \"relation"));
            Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));

            res = adminClient.get("/deals/_doc/0?pretty");
            Assert.assertTrue(res.getBody(), res.getBody().contains("\"found\" : true"));

            res = dmClient.get("/deals/_doc/0?pretty");
            Assert.assertTrue(res.getBody().contains("\"found\" : false"));
        }
    }
}