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
/*
 * Includes parts from https://github.com/opensearch-project/security/blob/c18a50ac4c5f7116e0e7c3411944d1438f9c44e9/src/test/java/org/opensearch/security/dlic/dlsfls/DlsTest.java
 * 
 * Copyright OpenSearch Contributors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class DlsTest {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().resources("dlsfls_legacy").enterpriseModulesEnabled().embedded().build();

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
                Assert.assertFalse(response.getBody(), response.getBodyAsDocNode().getAsNode("modules").getAsNode("DLSFLS").isNull());
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
            Assert.assertTrue(response.getBody(), response.getBody().contains("\"failed\" : 0"));
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
            GenericRestClient.HttpResponse response = client.get("/deals/_termvectors/0?pretty=true");
            Assert.assertTrue(response.getBody().contains("\"found\" : false"));
        }

        try (GenericRestClient client = cluster.getRestClient("admin", "admin")) {
            GenericRestClient.HttpResponse response = client.get("/deals/_termvectors/0?pretty=true");
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

    @Test
    public void testDlsWithMinDocCountZeroAggregations() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {
            client.admin().indices().create(new CreateIndexRequest("logs")
                    .mapping("_doc", ImmutableMap.of("properties", ImmutableMap.of("termX", ImmutableMap.of("type", "keyword"))))).actionGet();

            for (int i = 0; i < 3; i++) {
                client.index(new IndexRequest("logs").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("amount", i, "termX", "A", "timestamp",
                        "2022-01-06T09:05:00Z")).actionGet();
                client.index(new IndexRequest("logs").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("amount", i, "termX", "B", "timestamp",
                        "2022-01-06T09:08:00Z")).actionGet();
                client.index(new IndexRequest("logs").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("amount", i, "termX", "C", "timestamp",
                        "2022-01-06T09:09:00Z")).actionGet();
                client.index(new IndexRequest("logs").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("amount", i, "termX", "D", "timestamp",
                        "2022-01-06T09:10:00Z")).actionGet();
            }
            client.index(new IndexRequest("logs").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("amount", 0, "termX", "E", "timestamp",
                    "2022-01-06T09:11:00Z")).actionGet();
        }

        try (GenericRestClient dmClient = cluster.getRestClient("dept_manager", "password");
                GenericRestClient adminClient = cluster.getRestClient("admin", "admin")) {
            // Terms Aggregation
            // Non-admin user with setting "min_doc_count":0. Expected to get error message "min_doc_count 0 is not supported when DLS is activated".
            String query1 = "{\n"//
                    + "  \"size\":0,\n"//
                    + "  \"query\":{\n"//
                    + "    \"bool\":{\n"//
                    + "      \"must\":[\n"//
                    + "        {\n"//
                    + "          \"range\":{\n"//
                    + "            \"amount\":{\"gte\":1,\"lte\":100}\n"//
                    + "          }\n"//
                    + "        }\n"//
                    + "      ]\n"//
                    + "    }\n"//
                    + "  },\n"//
                    + "  \"aggs\":{\n"//
                    + "    \"a\": {\n"//
                    + "      \"terms\": {\n"//
                    + "        \"field\": \"termX\",\n"//
                    + "        \"min_doc_count\":0,\n"//
                    + "\"size\": 10,\n"//
                    + "\"order\": { \"_count\": \"desc\" }\n"//
                    + "      }\n"//
                    + "    }\n"//
                    + "  }\n"//
                    + "}";

            HttpResponse response1 = dmClient.postJson("logs*/_search", query1);

            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response1.getStatusCode());
            // Assert.assertTrue(response1.getBody(), response1.getBody().contains("min_doc_count 0 is not supported when DLS is activated"));

            // Non-admin user without setting "min_doc_count". Expected to only have access to buckets for dept_manager excluding E with 0 doc_count".
            String query2 = "{\n"//
                    + "  \"size\":0,\n"//
                    + "  \"query\":{\n"//
                    + "    \"bool\":{\n"//
                    + "      \"must\":[\n"//
                    + "        {\n"//
                    + "          \"range\":{\n"//
                    + "            \"amount\":{\"gte\":1,\"lte\":100}\n"//
                    + "          }\n"//
                    + "        }\n"//
                    + "      ]\n"//
                    + "    }\n"//
                    + "  },\n"//
                    + "  \"aggs\":{\n"//
                    + "    \"a\": {\n"//
                    + "      \"terms\": {\n"//
                    + "        \"field\": \"termX\",\n"//
                    + "\"size\": 10,\n"//
                    + "\"order\": { \"_count\": \"desc\" }\n"//
                    + "      }\n"//
                    + "    }\n"//
                    + "  }\n"//
                    + "}";

            HttpResponse response2 = dmClient.postJson("logs*/_search", query2);

            Assert.assertEquals(HttpStatus.SC_OK, response2.getStatusCode());
            Assert.assertTrue(response2.getBody(), response2.getBody().contains("\"key\":\"A\""));
            Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"B\""));
            Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"C\""));
            Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"D\""));
            Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"E\""));

            // Admin with setting "min_doc_count":0. Expected to have access to all buckets".
            HttpResponse response3 = adminClient.postJson("logs*/_search", query1);

            Assert.assertEquals(HttpStatus.SC_OK, response3.getStatusCode());
            Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"A\""));
            Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"B\""));
            Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"C\""));
            Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"D\""));
            Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"E\",\"doc_count\":0"));

            // Admin without setting "min_doc_count". Expected to have access to all buckets excluding E with 0 doc_count".
            HttpResponse response4 = adminClient.postJson("logs*/_search", query2);

            Assert.assertEquals(HttpStatus.SC_OK, response4.getStatusCode());
            Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"A\""));
            Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"B\""));
            Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"C\""));
            Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"D\""));
            Assert.assertFalse(response4.getBody(), response4.getBody().contains("\"key\":\"E\""));

            // Significant Text Aggregation is not impacted.
            // Non-admin user with setting "min_doc_count=0". Expected to only have access to buckets for dept_manager".
            String query3 = "{\"aggregations\":{\"significant_termX\":{\"significant_terms\":{\"field\":\"termX.keyword\",\"min_doc_count\":0}}}}";
            HttpResponse response5 = dmClient.postJson("logs*/_search", query3);

            Assert.assertEquals(HttpStatus.SC_OK, response5.getStatusCode());
            Assert.assertTrue(response5.getBody(), response5.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response5.getBody(), response5.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response5.getBody(), response5.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response5.getBody(), response5.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));

            // Non-admin user without setting "min_doc_count". Expected to only have access to buckets for dept_manager".
            String query4 = "{\"aggregations\":{\"significant_termX\":{\"significant_terms\":{\"field\":\"termX.keyword\"}}}}";

            HttpResponse response6 = dmClient.postJson("logs*/_search", query4);

            Assert.assertEquals(HttpStatus.SC_OK, response6.getStatusCode());
            Assert.assertTrue(response6.getBody(), response6.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));

            // Admin with setting "min_doc_count":0. Expected to have access to all buckets".
            HttpResponse response7 = adminClient.postJson("logs*/_search", query3);

            Assert.assertEquals(HttpStatus.SC_OK, response7.getStatusCode());
            Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"E\""));

            // Admin without setting "min_doc_count". Expected to have access to all buckets".
            HttpResponse response8 = adminClient.postJson("logs*/_search", query4);

            Assert.assertEquals(HttpStatus.SC_OK, response8.getStatusCode());
            Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"E\""));

            // Histogram Aggregation is not impacted.
            // Non-admin user with setting "min_doc_count=0". Expected to only have access to buckets for dept_manager".
            String query5 = "{\"aggs\":{\"amount\":{\"histogram\":{\"field\":\"amount\",\"interval\":1,\"min_doc_count\":0}}}}";

            HttpResponse response9 = dmClient.postJson("logs*/_search", query5);

            Assert.assertEquals(HttpStatus.SC_OK, response9.getStatusCode());
            Assert.assertTrue(response9.getBody(), response9.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response9.getBody(), response9.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response9.getBody(), response9.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response9.getBody(), response9.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));

            // Non-admin user without setting "min_doc_count". Expected to only have access to buckets for dept_manager".
            String query6 = "{\"aggs\":{\"amount\":{\"histogram\":{\"field\":\"amount\",\"interval\":1}}}}";

            HttpResponse response10 = dmClient.postJson("logs*/_search", query6);

            Assert.assertEquals(HttpStatus.SC_OK, response10.getStatusCode());
            Assert.assertTrue(response10.getBody(), response10.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response10.getBody(), response10.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response10.getBody(), response10.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response10.getBody(), response10.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));

            // Admin with setting "min_doc_count":0. Expected to have access to all buckets".
            HttpResponse response11 = adminClient.postJson("logs*/_search", query5);

            Assert.assertEquals(HttpStatus.SC_OK, response11.getStatusCode());
            Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"E\""));

            // Admin without setting "min_doc_count". Expected to have access to all buckets".
            HttpResponse response12 = adminClient.postJson("logs*/_search", query6);

            Assert.assertEquals(HttpStatus.SC_OK, response12.getStatusCode());
            Assert.assertTrue(response12.getBody(), response12.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response12.getBody(), response12.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response12.getBody(), response12.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response12.getBody(), response12.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));

            // Date Histogram Aggregation is not impacted.
            // Non-admin user with setting "min_doc_count=0". Expected to only have access to buckets for dept_manager".
            String query7 = "{\"aggs\":{\"timestamp\":{\"date_histogram\":{\"field\":\"timestamp\",\"calendar_interval\":\"month\",\"min_doc_count\":0}}}}";

            HttpResponse response13 = dmClient.postJson("logs*/_search", query7);

            Assert.assertEquals(HttpStatus.SC_OK, response13.getStatusCode());
            Assert.assertTrue(response13.getBody(), response13.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response13.getBody(), response13.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response13.getBody(), response13.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response13.getBody(), response13.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));

            // Non-admin user without setting "min_doc_count". Expected to only have access to buckets for dept_manager".
            String query8 = "{\"aggs\":{\"timestamp\":{\"date_histogram\":{\"field\":\"timestamp\",\"calendar_interval\":\"month\"}}}}";

            HttpResponse response14 = dmClient.postJson("logs*/_search", query8);

            Assert.assertEquals(HttpStatus.SC_OK, response14.getStatusCode());
            Assert.assertTrue(response14.getBody(), response14.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response14.getBody(), response14.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response14.getBody(), response14.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response14.getBody(), response14.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));

            // Admin with setting "min_doc_count":0. Expected to have access to all buckets".
            HttpResponse response15 = adminClient.postJson("logs*/_search", query7);

            Assert.assertEquals(HttpStatus.SC_OK, response15.getStatusCode());
            Assert.assertTrue(response15.getBody(), response15.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response15.getBody(), response15.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response15.getBody(), response15.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response15.getBody(), response15.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));

            // Admin without setting "min_doc_count". Expected to have access to all buckets".
            HttpResponse response16 = adminClient.postJson("logs*/_search", query8);

            Assert.assertEquals(HttpStatus.SC_OK, response16.getStatusCode());
            Assert.assertTrue(response16.getBody(), response16.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response16.getBody(), response16.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response16.getBody(), response16.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response16.getBody(), response16.getBody().contains("\"termX\":\"D\""));
            // TODO there seems to be some flakyness with the following assert
            // Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));
        }
    }

}