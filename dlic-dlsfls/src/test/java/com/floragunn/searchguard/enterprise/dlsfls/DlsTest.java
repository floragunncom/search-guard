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

import java.io.IOException;
import java.util.Collection;


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
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class DlsTest {

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

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, DEPT_A_USER, DEPT_D_USER).resources("dlsfls")
            // An external process cluster is used due to the use of LogsDB indices, which requires an additional native library.
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void setupTestData() throws IOException {
        Settings settings = Settings.builder().put("index.number_of_shards", 5).build();
        try(GenericRestClient adminCertRestClient = cluster.getAdminCertRestClient()) {
            String indexMode = TEST_DATA.createIndex(adminCertRestClient, INDEX_NORMAL_MODE, settings);
            // null means default mode which is currently normal
            assertThat(indexMode, anyOf(equalTo("normal"), nullValue()));
            settings = Settings.builder().put("index.number_of_shards", 5).put("index.mode", "logsdb").build();
            indexMode = TEST_DATA.createIndex(adminCertRestClient, INDEX_LOGS_DB_MODE, settings);
            assertThat(indexMode, equalTo("logsdb"));
        }
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