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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.Collection;

import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.test.helper.PitHolder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;

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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class DlsIntTest {

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
    static final TestSgConfig.User DEPT_D_TERMS_LOOKUP_USER = new TestSgConfig.User("dept_d_terms_lookup_user")
            .roles(new Role("dept_d").indexPermissions("SGS_READ")
                    .dls(DocNode.of("terms", DocNode.of("dept", DocNode.of("index", "user_dept_terms_lookup", "id", "${user.name}", "path", "dept"))))
                    .on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().useImpl("flx").metrics("detailed");

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, DEPT_A_USER, DEPT_D_USER, DEPT_D_TERMS_LOOKUP_USER).resources("dlsfls").embedded().build();

    @BeforeClass
    public static void setupTestData() {
        Client client = cluster.getInternalNodeClient();
        Settings settings = Settings.builder().put("index.number_of_shards", 5).build();
        TEST_DATA.createIndex(client, INDEX_NORMAL_MODE, settings);
        settings = Settings.builder().put("index.number_of_shards", 5).put("index.mode", "logsdb").build();
        TEST_DATA.createIndex(client, INDEX_LOGS_DB_MODE, settings);

        client.index(new IndexRequest("user_dept_terms_lookup").id("dept_d_terms_lookup_user").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("dept", "dept_d")).actionGet();

        String indexMode = TEST_DATA.getIndexMode(client, INDEX_NORMAL_MODE);
        // null means default mode which is currently normal
        assertThat(indexMode, anyOf(equalTo("normal"), nullValue()));
        indexMode = TEST_DATA.getIndexMode(client, INDEX_LOGS_DB_MODE);
        assertThat(indexMode, equalTo("logsdb"));
    }

    private final String indexName;

    public DlsIntTest(String indexName) {
        this.indexName = indexName;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] { INDEX_NORMAL_MODE, INDEX_LOGS_DB_MODE };
    }

    @Test
    public void get() throws Exception {
        TestDocument testDocumentA1 = TEST_DATA.anyDocumentForDepartment("dept_a_1");
        String documentUriA1 = "/" + indexName + "/_doc/" + testDocumentA1.getId();
        TestDocument testDocumentD = TEST_DATA.anyDocumentForDepartment("dept_d");
        String documentUriD = "/" + indexName + "/_doc/" + testDocumentD.getId();

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_USER)) {
            GenericRestClient.HttpResponse response = client.get(documentUriA1);
            Assert.assertEquals(response.getBody(), 404, response.getStatusCode());
            response = client.get(documentUriD);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(documentUriA1);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }
    }

    @Test
    public void get_termsLookup() throws Exception {
        TestDocument testDocumentA1 = TEST_DATA.anyDocumentForDepartment("dept_a_1");
        TestDocument testDocumentD = TEST_DATA.anyDocumentForDepartment("dept_d");
        String documentUriA1 = "/" + indexName + "/_doc/" + testDocumentA1.getId();
        String documentUriD = "/" + indexName + "/_doc/" + testDocumentD.getId();

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_TERMS_LOOKUP_USER)) {
            GenericRestClient.HttpResponse response = client.get(documentUriA1);
            Assert.assertEquals(response.getBody(), 404, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_TERMS_LOOKUP_USER)) {
            GenericRestClient.HttpResponse response = client.get(documentUriD);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(documentUriA1);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }
    }

    @Test
    public void mget() throws Exception {
        TestDocument testDocumentA1 = TEST_DATA.anyDocumentForDepartment("dept_a_1");
        TestDocument testDocumentD = TEST_DATA.anyDocumentForDepartment("dept_d");

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_mget",
                    DocNode.of("docs", DocNode.array(DocNode.of("_id", testDocumentA1.getId()), DocNode.of("_id", testDocumentD.getId()))));
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("docs[?(@._source.dept =~ /dept_a.*/)]").size() == 0);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("docs[?(@._source.dept =~ /dept_d.*/)]").size() == 1);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_mget",
                    DocNode.of("docs", DocNode.array(DocNode.of("_id", testDocumentA1.getId()), DocNode.of("_id", testDocumentD.getId()))));
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("docs[?(@._source.dept =~ /dept_a.*/)]").size() == 1);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("docs[?(@._source.dept =~ /dept_d.*/)]").size() == 1);
        }
    }

    @Test
    public void mget_termsLookup() throws Exception {
        TestDocument testDocumentA1 = TEST_DATA.anyDocumentForDepartment("dept_a_1");
        TestDocument testDocumentD = TEST_DATA.anyDocumentForDepartment("dept_d");

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_TERMS_LOOKUP_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_mget",
                    DocNode.of("docs", DocNode.array(DocNode.of("_id", testDocumentA1.getId()), DocNode.of("_id", testDocumentD.getId()))));
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("docs[?(@._source.dept =~ /dept_a.*/)]").size() == 0);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("docs[?(@._source.dept =~ /dept_d.*/)]").size() == 1);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_mget",
                    DocNode.of("docs", DocNode.array(DocNode.of("_id", testDocumentA1.getId()), DocNode.of("_id", testDocumentD.getId()))));
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("docs[?(@._source.dept =~ /dept_a.*/)]").size() == 1);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("docs[?(@._source.dept =~ /dept_d.*/)]").size() == 1);
        }
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
    public void search_withPit() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(DEPT_A_USER);
            PitHolder pitHolder = PitHolder.of(client).post("/" + indexName + "/_pit?keep_alive=1m")) {

            GenericRestClient.HttpResponse response = client.postJson("/_search?pretty", DocNode.of("pit.id", pitHolder.getPitId()));
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dept =~ /dept_a.*/)]").size() == 10);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!(@._source.dept =~ /dept_a.*/))]").size() == 0);

        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN);
            PitHolder pitHolder = PitHolder.of(client).post("/" + indexName + "/_pit?keep_alive=1m")) {

            GenericRestClient.HttpResponse response = client.postJson("/_search?pretty", DocNode.of("pit.id", pitHolder.getPitId()));
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!(@._source.dept =~ /dept_a.*/))]").size() != 0);
        }
    }

    @Test
    public void search_termsLookup() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DEPT_D_TERMS_LOOKUP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dept =~ /dept_d.*/)]").size() == 10);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!(@._source.dept =~ /dept_d.*/))]").size() == 0);
        }
    }

    @Test
    public void search_suggest() throws Exception {
        // TOOD test with exclusive term

        DocNode query = DocNode.of("suggest", DocNode.of("suggestion", DocNode.of("text", "rahnsthla", "term.field", "source_loc")));

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }
    }

    @Test
    public void search_suggest_withPit() throws Exception {
        // TOOD test with exclusive term

        DocNode query = DocNode.of("suggest", DocNode.of("suggestion", DocNode.of("text", "rahnsthla", "term.field", "source_loc")));

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_USER);
            PitHolder pitHolder = PitHolder.of(client).post("/" + indexName + "/_pit?keep_alive=1m")) {

            GenericRestClient.HttpResponse response = client.postJson("/_search?pretty", query.with(DocNode.of("pit.id", pitHolder.getPitId())));

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN);
            PitHolder pitHolder = PitHolder.of(client).post("/" + indexName + "/_pit?keep_alive=1m")) {

            GenericRestClient.HttpResponse response = client.postJson("/_search?pretty", query.with(DocNode.of("pit.id", pitHolder.getPitId())));

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
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

            System.out.println(response.getBody());
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
    public void scroll_withPit() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(DEPT_A_USER);
            PitHolder pitHolder = PitHolder.of(client).post("/" + indexName + "/_pit?keep_alive=1m")) {

            GenericRestClient.HttpResponse response = client.postJson("/_search?scroll=1m&pretty=true&size=5", DocNode.of("pit.id", pitHolder.getPitId()));
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode()); //using point in time is not allowed in a scroll context
        }
    }

    @Test
    public void scroll_termsLookup() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_TERMS_LOOKUP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?scroll=1m&pretty=true&size=5");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dept =~ /dept_d.*/)]").size() == 5);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!(@._source.dept =~ /dept_d.*/))]").size() == 0);

            System.out.println(response.getBody());
            String scrollId = response.getBodyAsDocNode().getAsString("_scroll_id");

            for (;;) {
                GenericRestClient.HttpResponse scrollResponse = client.postJson("/_search/scroll?pretty=true",
                        DocNode.of("scroll", "1m", "scroll_id", scrollId));

                int hits = scrollResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits").size();

                if (hits == 0) {
                    break;
                }

                Assert.assertTrue(scrollResponse.getBody(),
                        scrollResponse.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dept =~ /dept_d.*/)]").size() == hits);
                Assert.assertTrue(response.getBody(),
                        scrollResponse.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!(@._source.dept =~ /dept_d.*/))]").size() == 0);
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
    public void terms_aggregation_withPit() throws Exception {

        DocNode query = DocNode.parse(Format.JSON).from("{" + "\"query\" : {" + "\"match_all\": {}" + "}," + "\"aggs\" : {"
                + "\"test_agg\" : { \"terms\" : { \"field\" : \"dept.keyword\" } }" + "}" + "}");

        int a1count;
        int a2count;

        try (GenericRestClient client = cluster.getRestClient(ADMIN);
            PitHolder pitHolder = PitHolder.of(client).post("/" + indexName + "/_pit?keep_alive=1m")) {

            GenericRestClient.HttpResponse response = client.postJson("/_search?pretty", query.with(DocNode.of("pit.id", pitHolder.getPitId())));

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().findNodesByJsonPath("aggregations.test_agg.buckets[?(@.key == 'dept_d')]").size() == 1);

            a1count = getInt(response, "aggregations.test_agg.buckets[?(@.key == 'dept_a_1')].doc_count");
            a2count = getInt(response, "aggregations.test_agg.buckets[?(@.key == 'dept_a_2')].doc_count");
        }

        try (GenericRestClient client = cluster.getRestClient(DEPT_A_USER);
            PitHolder pitHolder = PitHolder.of(client).post("/" + indexName + "/_pit?keep_alive=1m")) {

            GenericRestClient.HttpResponse response = client.postJson("/_search?pretty", query.with(DocNode.of("pit.id", pitHolder.getPitId())));

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
        String docUrl = "/" + indexName + "/_termvectors/" + doc.getId() + "?pretty=true";

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

    @AfterClass
    public static void clusterState() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            System.out.println(client.get("/_searchguard/component/dlsfls/_health?pretty=true").getBody());
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