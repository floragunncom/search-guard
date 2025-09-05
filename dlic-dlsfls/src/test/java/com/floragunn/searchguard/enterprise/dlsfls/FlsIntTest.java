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
 * Includes code from https://github.com/opensearch-project/security/blob/70591197c705ca6f42f765186a05837813f80ff3/src/integrationTest/java/org/opensearch/security/privileges/dlsfls/FlsFmIntegrationTests.java
 * which is Copyright OpenSearch Contributors
 */
package com.floragunn.searchguard.enterprise.dlsfls;

import java.io.IOException;
import java.util.Set;

import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestData.TestDocument;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.jayway.jsonpath.Configuration.Defaults;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class FlsIntTest {
    static {
        // TODO properly initialize JsonPath defaults
        com.jayway.jsonpath.Configuration.setDefaults(new Defaults() {

            @Override
            public JsonProvider jsonProvider() {
                return BasicJsonPathDefaultConfiguration.defaultConfiguration().jsonProvider();
            }

            @Override
            public Set<Option> options() {
                return BasicJsonPathDefaultConfiguration.defaultConfiguration().getOptions();
            }

            @Override
            public MappingProvider mappingProvider() {
                return BasicJsonPathDefaultConfiguration.defaultConfiguration().mappingProvider();
            }

        });
    }

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

    static final TestSgConfig.User EXCLUDE_IP_USER = new TestSgConfig.User("exclude_ip")
            .roles(new Role("exclude_ip").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("~*_ip").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User EXCLUDE_OBJECT_USER = new TestSgConfig.User("exclude_object")
            .roles(new Role("exclude_object").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("~object").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User INCLUDE_OBJECT_USER = new TestSgConfig.User("include_object")
            .roles(new Role("include_object").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("object").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User EXCLUDE_OBJECT_FIELD_USER = new TestSgConfig.User("exclude_object_field")
            .roles(new Role("exclude_object_field").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("~object.integer_field").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User INCLUDE_OBJECT_FIELD_USER = new TestSgConfig.User("include_object_field")
            .roles(new Role("include_object_field").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("object.integer_field").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User INCLUDE_LOC_USER = new TestSgConfig.User("include_loc").roles(new Role("include_loc")
            .indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("*_loc", "timestamp").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User EXCLUDE_LOC_USER = new TestSgConfig.User("exclude_loc").roles(new Role("exclude_loc")
            .indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("~*_loc", "timestamp").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User MULTI_ROLE_USER = new TestSgConfig.User("multi_role").roles(
            new Role("role1").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("~*_ip").on(INDEX_PATTERN).clusterPermissions("*"),
            new Role("role2").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("source_ip").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled() //
            .authc(AUTHC).dlsFls(DLSFLS) //
            .users(ADMIN, EXCLUDE_IP_USER, EXCLUDE_OBJECT_USER, INCLUDE_OBJECT_USER, EXCLUDE_OBJECT_FIELD_USER, //
                    INCLUDE_OBJECT_FIELD_USER, INCLUDE_LOC_USER, MULTI_ROLE_USER, EXCLUDE_LOC_USER) //
            .resources("dlsfls")
            // An external process cluster is used due to the use of LogsDB indices, which requires an additional native library.
            .useExternalProcessCluster().build();

    private final String indexName;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] { INDEX_NORMAL_MODE, INDEX_LOGS_DB_MODE};
    }

    public FlsIntTest(String indexName) {
        this.indexName = indexName;
    }

    @BeforeClass
    public static void setupTestData() throws IOException {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            Settings settings = Settings.builder().put("index.number_of_shards", 5).build();
            String indexMode = TEST_DATA.createIndex(client, INDEX_NORMAL_MODE, settings);
            // null means default mode which is currently normal
            assertThat(indexMode, anyOf(equalTo("normal"), nullValue()));
            settings = Settings.builder().put("index.number_of_shards", 5).put("index.mode", "logsdb").build();
            indexMode = TEST_DATA.createIndex(client, INDEX_LOGS_DB_MODE, settings);
            assertThat(indexMode, equalTo("logsdb"));
        }
    }

    @Test
    public void search_exclude() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() == 0);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!@._source.source_ip)]").size() == 10);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() != 0);
        }
    }

    @Test
    public void search_object_exclude() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.object)]").size() != 0);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.object.integer_field)]").size() != 0);
        }

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_OBJECT_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.object)]").size() == 0); // none null value of _source.object field
        }

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_OBJECT_FIELD_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.object)]").size() != 0);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.object.integer_field)]").size() == 0);
        }
    }

    @Test
    public void search_object_int_range() throws Exception {
        int docObjectInt = DocNode.wrap(TEST_DATA.anyDocument().getContent()).getAsNode("object").getNumber("integer_field").intValue();
        String query = String.format(
                """
                {
                  "query" : {
                    "range" : {
                      "object.integer_field" : {
                        "gte" : "%d",
                        "lte" : "%d"
                      }
                    }
                  }
                }
                """, docObjectInt - 1, docObjectInt + 1
        );
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").getNumber("value").intValue() != 0);
        }

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_OBJECT_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);
            assertThat(response, isOk());
            Assert.assertEquals(response.getBody(), 0, response.getBodyAsDocNode().get("hits", "total", "value"));
        }

        try (GenericRestClient client = cluster.getRestClient(INCLUDE_OBJECT_FIELD_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").getNumber("value").intValue() != 0);
        }

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_OBJECT_FIELD_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);
            assertThat(response, isOk());
            Assert.assertEquals(response.getBody(), 0, response.getBodyAsDocNode().get("hits", "total", "value"));
        }
    }

    @Test
    public void search_object_integer_term() throws Exception {
        int docObjectInt = DocNode.wrap(TEST_DATA.anyDocument().getContent()).getAsNode("object").getNumber("integer_field").intValue();
        String query = String.format(
                """
                        {
                          "query": {
                            "term": {
                              "object.integer_field": %d
                            }
                          }
                        }
                """, docObjectInt
        );
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").getNumber("value").intValue() != 0);
        }

        try (GenericRestClient client = cluster.getRestClient(INCLUDE_OBJECT_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").getNumber("value").intValue() != 0);
        }

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_OBJECT_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);
            assertThat(response, isOk());
            Assert.assertEquals(response.getBody(), 0, response.getBodyAsDocNode().get("hits", "total", "value"));
        }

        try (GenericRestClient client = cluster.getRestClient(INCLUDE_OBJECT_FIELD_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").getNumber("value").intValue() != 0);
        }

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_OBJECT_FIELD_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", query);
            assertThat(response, isOk());
            Assert.assertEquals(response.getBody(), 0, response.getBodyAsDocNode().get("hits", "total", "value"));
        }
    }

    @Test
    public void search_include() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(INCLUDE_LOC_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() == 0);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!@._source.source_ip)]").size() == 10);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_loc)]").size() == 10);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() == 10);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_loc)]").size() == 10);
        }
    }

    @Test
    public void search_onFilteredField() throws Exception {
        TestDocument testDocument = TEST_DATA.anyDocument();

        DocNode searchBody = DocNode.of("query.term.source_ip.value", testDocument.getContent().get("source_ip"));

        try (GenericRestClient client = cluster.getRestClient(INCLUDE_LOC_USER)) {

            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", searchBody);
            assertThat(response, isOk());
            Assert.assertEquals(response.getBody(), 0, response.getBodyAsDocNode().get("hits", "total", "value"));
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexName + "/_search?pretty", searchBody);
            assertThat(response, isOk());
            // testDocument.getContent().get("source_ip") returns 1 or 2 for a random IP. The TestData class generates random data, therefore,
            // duplicates may or may not be present.
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").getNumber("value").intValue() > 0);
        }
    }

    @Test
    public void search_multiRole() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(MULTI_ROLE_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() == 10);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dest_ip)]").size() == 0);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_search?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() != 0);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dest_ip)]").size() != 0);
        }
    }

    @Test
    public void get_exclude() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/" + indexName + "/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_ip") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_loc") != null);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_ip") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_loc") != null);
        }
    }

    @Test
    public void get_object_exclude() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/" + indexName + "/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("object") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_loc") != null);
        }

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_OBJECT_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("object") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_loc") != null);
        }

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_OBJECT_FIELD_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("object") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("object.integer_field") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_loc") != null);
        }
    }

    @Test
    public void get_include() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/" + indexName + "/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(INCLUDE_LOC_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_ip") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_loc") != null);
        }

        try (GenericRestClient client = cluster.getRestClient(INCLUDE_OBJECT_FIELD_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_ip") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").getAsNode("object").get("integer_field") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").getAsNode("object").get("dept") == null);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_ip") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_loc") != null);
        }
    }

    @Test
    public void get_multiRole() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/" + indexName + "/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(MULTI_ROLE_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_ip") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("dest_ip") == null);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("source_ip") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("_source").get("dest_ip") != null);
        }
    }

    @Test
    public void mapping() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_mapping?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().getAsNode(indexName).getAsNode("mappings").getAsNode("properties").get("source_ip") == null);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().getAsNode(indexName).getAsNode("mappings").getAsNode("properties").get("source_loc") != null);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_mapping?pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().getAsNode(indexName).getAsNode("mappings").getAsNode("properties").get("source_ip") != null);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().getAsNode(indexName).getAsNode("mappings").getAsNode("properties").get("source_loc") != null);
        }
    }

    @Test
    public void fieldCaps() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_field_caps?fields=*&pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_ip") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_loc") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_loc.keyword") != null);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexName + "/_field_caps?fields=*&pretty");
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_ip") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_loc") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_loc.keyword") != null);
        }
    }

    @Test
    public void termvectors() throws Exception {
        TestDocument doc = TEST_DATA.anyDocumentForDepartment("dept_a_1");
        String docUrl = "/" + indexName + "/_termvectors/" + doc.getId() + "?pretty=true&fields=*";

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_LOC_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_loc") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_loc.keyword") == null);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            assertThat(response, isOk());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_loc") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_loc.keyword") != null);
        }

    }

}
