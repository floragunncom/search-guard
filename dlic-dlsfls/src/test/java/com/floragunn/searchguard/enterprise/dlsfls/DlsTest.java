/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.Collection;

import com.floragunn.fluent.collections.ImmutableMap;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format.UnknownDocTypeException;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestData.TestDocument;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class DlsTest {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    /**
     * Increase DOC_COUNT for manual test runs with bigger test data sets
     */
    static final int DOC_COUNT = 200;
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT) //
            .timestampColumnName("@timestamp") //
            .get();

    static final String INDEX_NAME_PREFIX = "logs";
    static final String INDEX_PATTERN = INDEX_NAME_PREFIX + "*";
    static final String INDEX_NORMAL_MODE = INDEX_NAME_PREFIX + "_normal_index_mode";
    static final String INDEX_LOGS_DB_MODE = INDEX_NAME_PREFIX + "_logs_db_index_mode";

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static final TestSgConfig.User DEPT_A_USER = new TestSgConfig.User("dept_a")
            .roles(new Role("dept_a").indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on(INDEX_PATTERN).clusterPermissions("*"));
    static final TestSgConfig.User DEPT_D_USER = new TestSgConfig.User("dept_d")
            .roles(new Role("dept_d").indexPermissions("SGS_READ").dls(DocNode.of("term.dept.value", "dept_d")).on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User LOGS_TWO_TERMX_A_USER = new TestSgConfig.User("logs_termx_a")
            .roles(new TestSgConfig.Role("logs_index_with_dls").indexPermissions("SGS_READ").dls(DocNode.of("bool.filter.term.termX", "A")).on("logs-2"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().useImpl("flx");

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, DEPT_A_USER, DEPT_D_USER, LOGS_TWO_TERMX_A_USER).resources("dlsfls").embedded().build();

    @BeforeClass
    public static void setupTestData() {
        Client client = cluster.getInternalNodeClient();
        Settings settings = Settings.builder().put("index.number_of_shards", 5).build();
        String indexMode = TEST_DATA.createIndex(client, INDEX_NORMAL_MODE, settings);
        // null means default mode which is currently normal
        assertThat(indexMode, anyOf(equalTo("normal"), nullValue()));
        settings = Settings.builder().put("index.number_of_shards", 5).put("index.mode", "logsdb").build();
        indexMode = TEST_DATA.createIndex(client, INDEX_LOGS_DB_MODE, settings);
        assertThat(indexMode, equalTo("logsdb"));
    }

    private final String indexName;

    public DlsTest(String indexName) {
        this.indexName = indexName;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] { INDEX_NORMAL_MODE, INDEX_LOGS_DB_MODE };
    }

    @Test
    public void search() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(DEPT_A_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dept =~ /dept_a.*/)]").size() == 10);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!(@._source.dept =~ /dept_a.*/))]").size() == 0);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!(@._source.dept =~ /dept_a.*/))]").size() != 0);
        }
    }

    @Test
    public void scroll() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(DEPT_A_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?scroll=1m&pretty=true&size=5");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dept =~ /dept_a.*/)]").size() == 5);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!(@._source.dept =~ /dept_a.*/))]").size() == 0);

            String scrollId = response.getBodyAsDocNode().getAsString("_scroll_id");

            for (;;) {
                GenericRestClient.HttpResponse scrollResponse = client.postJson("/_search/scroll?pretty=true",
                        DocNode.of("scroll", "1m", "scroll_id", scrollId));

                int hits = scrollResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits").size();

                if (hits == 0) {
                    break;
                }

                Assert.assertTrue(scrollResponse.getBody(),
                        scrollResponse.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dept =~ /dept_a.*/)]").size() == hits);
                Assert.assertTrue(scrollResponse.getBody(),
                        scrollResponse.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!(@._source.dept =~ /dept_a.*/))]").size() == 0);
            }

        }
    }

    @Test
    public void terms_aggregation() throws Exception {

        String query = "{" + "\"query\" : {" + "\"match_all\": {}" + "}," + "\"aggs\" : {"
                + "\"test_agg\" : { \"terms\" : { \"field\" : \"dept.keyword\" } }" + "}" + "}";

        int a1count;
        int a2count;

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("aggregations.test_agg.buckets[?(@.key == 'dept_d')]").size() == 1);

            a1count = getInt(response, "aggregations.test_agg.buckets[?(@.key == 'dept_a_1')].doc_count");
            a2count = getInt(response, "aggregations.test_agg.buckets[?(@.key == 'dept_a_2')].doc_count");
        }

        try (GenericRestClient client = cluster.getRestClient(DEPT_A_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("aggregations.test_agg.buckets[?(@.key == 'dept_d')]").size() == 0);

            Assert.assertEquals(response.getBody(), a1count, getInt(response, "aggregations.test_agg.buckets[?(@.key == 'dept_a_1')].doc_count"));
            Assert.assertEquals(response.getBody(), a2count, getInt(response, "aggregations.test_agg.buckets[?(@.key == 'dept_a_2')].doc_count"));
        }
    }

    @Test
    public void termvectors() throws Exception {
        TestDocument doc = TEST_DATA.anyDocumentForDepartment("dept_a_1");
        String docUrl = "/" + indexName + "//_termvectors/" + doc.getId() + "?pretty=true";

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), true, response.getBodyAsDocNode().get("found"));
        }

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), false, response.getBodyAsDocNode().get("found"));
        }

        TestDocument allowedDoc = TEST_DATA.anyDocumentForDepartment("dept_d");

        String allowedDocUrl = "/" + indexName + "/_termvectors/" + allowedDoc.getId() + "?pretty=true";

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_USER)) {
            GenericRestClient.HttpResponse response = client.get(allowedDocUrl);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), true, response.getBodyAsDocNode().get("found"));
        }
    }

    @Test
    //ported from com.floragunn.searchguard.enterprise.dlsfls.legacy.DlsTest
    public void testDlsWithMinDocCountZeroAggregations() throws Exception {
        if(!indexName.equals(INDEX_NORMAL_MODE)) {
            // TODO the test cannot be run twice
            return;
        }

        Client client = cluster.getInternalNodeClient();

        client.admin().indices().create(new CreateIndexRequest("logs-2").mapping(
                ImmutableMap.of("properties", ImmutableMap.of("termX", ImmutableMap.of("type", "keyword"))))).actionGet();

        for (int i = 0; i < 3; i++) {
            client.index(new IndexRequest("logs-2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source("amount", i, "termX", "A", "timestamp",
                    "2022-01-06T09:05:00Z")).actionGet();
            client.index(new IndexRequest("logs-2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source("amount", i, "termX", "B", "timestamp",
                    "2022-01-06T09:08:00Z")).actionGet();
            client.index(new IndexRequest("logs-2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source("amount", i, "termX", "C", "timestamp",
                    "2022-01-06T09:09:00Z")).actionGet();
            client.index(new IndexRequest("logs-2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source("amount", i, "termX", "D", "timestamp",
                    "2022-01-06T09:10:00Z")).actionGet();
        }
        client.index(new IndexRequest("logs-2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source("amount", 0, "termX", "E", "timestamp",
                "2022-01-06T09:11:00Z")).actionGet();

        try (GenericRestClient dmClient = cluster.getRestClient(LOGS_TWO_TERMX_A_USER);
             GenericRestClient adminClient = cluster.getRestClient(ADMIN);
             GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {

            // Terms Aggregation
            //force_min_doc_count_to_1 = false

            cluster.callAndRestoreConfig(DlsFlsConfig.TYPE, () -> {

                DocNode dlsConfig = DocNode.of("use_impl", "flx", "dls.force_min_doc_count_to_1", false);
                GenericRestClient.HttpResponse configResponse = adminCertClient.putJson("/_searchguard/config/authz_dlsfls", dlsConfig);
                Assert.assertEquals(HttpStatus.SC_OK, configResponse.getStatusCode());

                // Non-admin user with setting "min_doc_count":0 when force_min_doc_count_to_1 is disabled. Expected to get error message "min_doc_count 0 is not supported when DLS is activated".
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

                GenericRestClient.HttpResponse response1 = dmClient.postJson("logs-2*/_search", query1);

                Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response1.getStatusCode());

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

                GenericRestClient.HttpResponse response2 = dmClient.postJson("logs-2*/_search", query2);

                Assert.assertEquals(HttpStatus.SC_OK, response2.getStatusCode());
                Assert.assertTrue(response2.getBody(), response2.getBody().contains("\"key\":\"A\""));
                Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"B\""));
                Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"C\""));
                Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"D\""));
                Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"E\""));

                // Admin with setting "min_doc_count":0. Expected to have access to all buckets".
                GenericRestClient.HttpResponse response3 = adminClient.postJson("logs-2*/_search", query1);

                Assert.assertEquals(HttpStatus.SC_OK, response3.getStatusCode());
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"A\""));
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"B\""));
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"C\""));
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"D\""));
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"E\",\"doc_count\":0"));

                // Admin without setting "min_doc_count". Expected to have access to all buckets excluding E with 0 doc_count".
                GenericRestClient.HttpResponse response4 = adminClient.postJson("logs-2*/_search", query2);

                Assert.assertEquals(HttpStatus.SC_OK, response4.getStatusCode());
                Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"A\""));
                Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"B\""));
                Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"C\""));
                Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"D\""));
                Assert.assertFalse(response4.getBody(), response4.getBody().contains("\"key\":\"E\""));

                return null;
            });

            // Terms Aggregation
            //force_min_doc_count_to_1 = true

            cluster.callAndRestoreConfig(DlsFlsConfig.TYPE, () -> {

                DocNode dlsConfig = DocNode.of("use_impl", "flx", "dls.force_min_doc_count_to_1", true);
                GenericRestClient.HttpResponse configResponse = adminCertClient.putJson("/_searchguard/config/authz_dlsfls", dlsConfig);
                Assert.assertEquals(HttpStatus.SC_OK, configResponse.getStatusCode());

                // Non-admin user with setting "min_doc_count":0 when force_min_doc_count_to_1 is enabled. Expected to only have access to buckets for dept_manager excluding E with 0 doc_count".
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

                GenericRestClient.HttpResponse response1 = dmClient.postJson("logs-2*/_search", query1);

                Assert.assertEquals(HttpStatus.SC_OK, response1.getStatusCode());
                Assert.assertTrue(response1.getBody(), response1.getBody().contains("\"key\":\"A\""));
                Assert.assertFalse(response1.getBody(), response1.getBody().contains("\"key\":\"B\""));
                Assert.assertFalse(response1.getBody(), response1.getBody().contains("\"key\":\"C\""));
                Assert.assertFalse(response1.getBody(), response1.getBody().contains("\"key\":\"D\""));
                Assert.assertFalse(response1.getBody(), response1.getBody().contains("\"key\":\"E\""));

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

                GenericRestClient.HttpResponse response2 = dmClient.postJson("logs-2*/_search", query2);

                Assert.assertEquals(HttpStatus.SC_OK, response2.getStatusCode());
                Assert.assertTrue(response2.getBody(), response2.getBody().contains("\"key\":\"A\""));
                Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"B\""));
                Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"C\""));
                Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"D\""));
                Assert.assertFalse(response2.getBody(), response2.getBody().contains("\"key\":\"E\""));

                // Admin with setting "min_doc_count":0. Expected to have access to all buckets".
                GenericRestClient.HttpResponse response3 = adminClient.postJson("logs*/_search", query1);

                Assert.assertEquals(HttpStatus.SC_OK, response3.getStatusCode());
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"A\""));
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"B\""));
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"C\""));
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"D\""));
                Assert.assertTrue(response3.getBody(), response3.getBody().contains("\"key\":\"E\",\"doc_count\":0"));

                // Admin without setting "min_doc_count". Expected to have access to all buckets excluding E with 0 doc_count".
                GenericRestClient.HttpResponse response4 = adminClient.postJson("logs-2*/_search", query2);

                Assert.assertEquals(HttpStatus.SC_OK, response4.getStatusCode());
                Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"A\""));
                Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"B\""));
                Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"C\""));
                Assert.assertTrue(response4.getBody(), response4.getBody().contains("\"key\":\"D\""));
                Assert.assertFalse(response4.getBody(), response4.getBody().contains("\"key\":\"E\""));

                return null;
            });

            // Significant Text Aggregation is not impacted.
            // Non-admin user with setting "min_doc_count=0". Expected to only have access to buckets for dept_manager".
            String query3 = "{\"size\":20,\"aggregations\":{\"significant_termX\":{\"significant_terms\":{\"field\":\"termX.keyword\",\"min_doc_count\":0}}}}";
            GenericRestClient.HttpResponse response5 = dmClient.postJson("logs-2*/_search", query3);

            Assert.assertEquals(HttpStatus.SC_OK, response5.getStatusCode());
            Assert.assertTrue(response5.getBody(), response5.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response5.getBody(), response5.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response5.getBody(), response5.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response5.getBody(), response5.getBody().contains("\"termX\":\"D\""));
            Assert.assertFalse(response5.getBody(), response5.getBody().contains("\"termX\":\"E\""));

            // Non-admin user without setting "min_doc_count". Expected to only have access to buckets for dept_manager".
            String query4 = "{\"size\":20,\"aggregations\":{\"significant_termX\":{\"significant_terms\":{\"field\":\"termX.keyword\"}}}}";

            GenericRestClient.HttpResponse response6 = dmClient.postJson("logs-2*/_search", query4);

            Assert.assertEquals(HttpStatus.SC_OK, response6.getStatusCode());
            Assert.assertTrue(response6.getBody(), response6.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"D\""));
            Assert.assertFalse(response6.getBody(), response6.getBody().contains("\"termX\":\"E\""));

            // Admin with setting "min_doc_count":0. Expected to have access to all buckets".
            GenericRestClient.HttpResponse response7 = adminClient.postJson("logs-2*/_search", query3);

            Assert.assertEquals(HttpStatus.SC_OK, response7.getStatusCode());
            Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"D\""));
            Assert.assertTrue(response7.getBody(), response7.getBody().contains("\"termX\":\"E\""));

            // Admin without setting "min_doc_count". Expected to have access to all buckets".
            GenericRestClient.HttpResponse response8 = adminClient.postJson("logs-2*/_search", query4);

            Assert.assertEquals(HttpStatus.SC_OK, response8.getStatusCode());
            Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"D\""));
            Assert.assertTrue(response8.getBody(), response8.getBody().contains("\"termX\":\"E\""));

            // Histogram Aggregation is not impacted.
            // Non-admin user with setting "min_doc_count=0". Expected to only have access to buckets for dept_manager".
            String query5 = "{\"size\":20,\"aggs\":{\"amount\":{\"histogram\":{\"field\":\"amount\",\"interval\":1,\"min_doc_count\":0}}}}";

            GenericRestClient.HttpResponse response9 = dmClient.postJson("logs-2*/_search", query5);

            Assert.assertEquals(HttpStatus.SC_OK, response9.getStatusCode());
            Assert.assertTrue(response9.getBody(), response9.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response9.getBody(), response9.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response9.getBody(), response9.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response9.getBody(), response9.getBody().contains("\"termX\":\"D\""));
            Assert.assertFalse(response9.getBody(), response9.getBody().contains("\"termX\":\"E\""));

            // Non-admin user without setting "min_doc_count". Expected to only have access to buckets for dept_manager".
            String query6 = "{\"size\":20,\"aggs\":{\"amount\":{\"histogram\":{\"field\":\"amount\",\"interval\":1}}}}";

            GenericRestClient.HttpResponse response10 = dmClient.postJson("logs-2*/_search", query6);

            Assert.assertEquals(HttpStatus.SC_OK, response10.getStatusCode());
            Assert.assertTrue(response10.getBody(), response10.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response10.getBody(), response10.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response10.getBody(), response10.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response10.getBody(), response10.getBody().contains("\"termX\":\"D\""));
            Assert.assertFalse(response10.getBody(), response10.getBody().contains("\"termX\":\"E\""));

            // Admin with setting "min_doc_count":0. Expected to have access to all buckets".
            GenericRestClient.HttpResponse response11 = adminClient.postJson("logs-2*/_search", query5);

            Assert.assertEquals(HttpStatus.SC_OK, response11.getStatusCode());
            Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"D\""));
            Assert.assertTrue(response11.getBody(), response11.getBody().contains("\"termX\":\"E\""));

            // Admin without setting "min_doc_count". Expected to have access to all buckets".
            GenericRestClient.HttpResponse response12 = adminClient.postJson("logs-2*/_search", query6);

            Assert.assertEquals(HttpStatus.SC_OK, response12.getStatusCode());
            Assert.assertTrue(response12.getBody(), response12.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response12.getBody(), response12.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response12.getBody(), response12.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response12.getBody(), response12.getBody().contains("\"termX\":\"D\""));
            Assert.assertTrue(response12.getBody(), response12.getBody().contains("\"termX\":\"E\""));

            // Date Histogram Aggregation is not impacted.
            // Non-admin user with setting "min_doc_count=0". Expected to only have access to buckets for dept_manager".
            String query7 = "{\"size\":20,\"aggs\":{\"timestamp\":{\"date_histogram\":{\"field\":\"timestamp\",\"calendar_interval\":\"month\",\"min_doc_count\":0}}}}";

            GenericRestClient.HttpResponse response13 = dmClient.postJson("logs-2*/_search", query7);

            Assert.assertEquals(HttpStatus.SC_OK, response13.getStatusCode());
            Assert.assertTrue(response13.getBody(), response13.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response13.getBody(), response13.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response13.getBody(), response13.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response13.getBody(), response13.getBody().contains("\"termX\":\"D\""));
            Assert.assertFalse(response13.getBody(), response13.getBody().contains("\"termX\":\"E\""));

            // Non-admin user without setting "min_doc_count". Expected to only have access to buckets for dept_manager".
            String query8 = "{\"size\":20,\"aggs\":{\"timestamp\":{\"date_histogram\":{\"field\":\"timestamp\",\"calendar_interval\":\"month\"}}}}";

            GenericRestClient.HttpResponse response14 = dmClient.postJson("logs-2*/_search", query8);

            Assert.assertEquals(HttpStatus.SC_OK, response14.getStatusCode());
            Assert.assertTrue(response14.getBody(), response14.getBody().contains("\"termX\":\"A\""));
            Assert.assertFalse(response14.getBody(), response14.getBody().contains("\"termX\":\"B\""));
            Assert.assertFalse(response14.getBody(), response14.getBody().contains("\"termX\":\"C\""));
            Assert.assertFalse(response14.getBody(), response14.getBody().contains("\"termX\":\"D\""));
            Assert.assertFalse(response14.getBody(), response14.getBody().contains("\"termX\":\"E\""));

            // Admin with setting "min_doc_count":0. Expected to have access to all buckets".
            GenericRestClient.HttpResponse response15 = adminClient.postJson("logs-2*/_search", query7);

            Assert.assertEquals(HttpStatus.SC_OK, response15.getStatusCode());
            Assert.assertTrue(response15.getBody(), response15.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response15.getBody(), response15.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response15.getBody(), response15.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response15.getBody(), response15.getBody().contains("\"termX\":\"D\""));
            Assert.assertTrue(response15.getBody(), response15.getBody().contains("\"termX\":\"E\""));

            // Admin without setting "min_doc_count". Expected to have access to all buckets".
            GenericRestClient.HttpResponse response16 = adminClient.postJson("logs-2*/_search", query8);

            Assert.assertEquals(HttpStatus.SC_OK, response16.getStatusCode());
            Assert.assertTrue(response16.getBody(), response16.getBody().contains("\"termX\":\"A\""));
            Assert.assertTrue(response16.getBody(), response16.getBody().contains("\"termX\":\"B\""));
            Assert.assertTrue(response16.getBody(), response16.getBody().contains("\"termX\":\"C\""));
            Assert.assertTrue(response16.getBody(), response16.getBody().contains("\"termX\":\"D\""));
            Assert.assertTrue(response16.getBody(), response16.getBody().contains("\"termX\":\"E\""));
        }
    }

    private static int getInt(GenericRestClient.HttpResponse response, String jsonPath) throws DocumentParseException, UnknownDocTypeException {
        Object object = response.getBodyAsDocNode().findSingleNodeByJsonPath("aggregations.test_agg.buckets[?(@.key == 'dept_a_1')].doc_count")
                .toBasicObject();

        if (object instanceof Collection) {
            object = ((Collection<?>) object).iterator().next();
        }

        if (object instanceof Number) {
            return ((Number) object).intValue();
        } else {
            throw new RuntimeException("Invalid value for " + jsonPath + ": " + object);
        }
    }
}