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
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.jayway.jsonpath.Configuration.Defaults;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

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

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    /**
     * Increase DOC_COUNT for manual test runs with bigger test data sets
     */
    static final int DOC_COUNT = 200;
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT).get();
    static final String INDEX = "logs";

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static final TestSgConfig.User EXCLUDE_IP_USER = new TestSgConfig.User("exclude_ip")
            .roles(new Role("exclude_ip").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("~*_ip").on(INDEX).clusterPermissions("*"));

    static final TestSgConfig.User INCLUDE_LOC_USER = new TestSgConfig.User("include_loc").roles(new Role("include_loc")
            .indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("*_loc", "timestamp").on(INDEX).clusterPermissions("*"));

    static final TestSgConfig.User MULTI_ROLE_USER = new TestSgConfig.User("multi_role").roles(
            new Role("role1").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("~*_ip").on(INDEX).clusterPermissions("*"),
            new Role("role2").indexPermissions("SGS_READ", "indices:admin/mappings/get").fls("source_ip").on(INDEX).clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().useImpl("flx");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, EXCLUDE_IP_USER, INCLUDE_LOC_USER, MULTI_ROLE_USER).resources("dlsfls").build();

    @BeforeClass
    public static void setupTestData() throws IOException {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            TEST_DATA.createIndex(client, INDEX, Settings.builder().put("index.number_of_shards", 5).build());
        }
    }

    @Test
    public void search_exclude() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/logs/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() == 0);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!@._source.source_ip)]").size() == 10);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/logs/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() != 0);
        }
    }

    @Test
    public void search_include() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(INCLUDE_LOC_USER)) {
            GenericRestClient.HttpResponse response = client.get("/logs/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() == 0);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(!@._source.source_ip)]").size() == 10);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_loc)]").size() == 10);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/logs/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() == 10);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_loc)]").size() == 10);
        }
    }

    @Test
    public void search_onFilteredField() throws Exception {
        TestDocument testDocument = TEST_DATA.anyDocument();

        DocNode searchBody = DocNode.of("query.term.source_ip.value", testDocument.getContent().get("source_ip"));

        try (GenericRestClient client = cluster.getRestClient(INCLUDE_LOC_USER)) {

            GenericRestClient.HttpResponse response = client.postJson("/logs/_search?pretty", searchBody);
            System.out.println(response.getBody());
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), 0, response.getBodyAsDocNode().get("hits", "total", "value"));
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/logs/_search?pretty", searchBody);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), 1, response.getBodyAsDocNode().get("hits", "total", "value"));
        }
    }

    @Test
    public void search_multiRole() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(MULTI_ROLE_USER)) {
            GenericRestClient.HttpResponse response = client.get("/logs/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() == 10);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dest_ip)]").size() == 0);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/logs/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.source_ip)]").size() != 0);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[?(@._source.dest_ip)]").size() != 0);
        }
    }

    @Test
    public void get_exclude() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/logs/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
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
    public void get_include() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/logs/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(INCLUDE_LOC_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
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
    public void get_multiRole() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/logs/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(MULTI_ROLE_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
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
            GenericRestClient.HttpResponse response = client.get("/logs/_mapping?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().getAsNode("logs").getAsNode("mappings").getAsNode("properties").get("source_ip") == null);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().getAsNode("logs").getAsNode("mappings").getAsNode("properties").get("source_loc") != null);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/logs/_mapping?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().getAsNode("logs").getAsNode("mappings").getAsNode("properties").get("source_ip") != null);
            Assert.assertTrue(response.getBody(),
                    response.getBodyAsDocNode().getAsNode("logs").getAsNode("mappings").getAsNode("properties").get("source_loc") != null);
        }
    }

    @Test
    public void fieldCaps() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/logs/_field_caps?fields=*&pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_ip") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_ip.keyword") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_loc") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_loc.keyword") != null);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/logs/_field_caps?fields=*&pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_ip") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_ip.keyword") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_loc") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("fields").get("source_loc.keyword") != null);
        }
    }

    @Test
    public void termvectors() throws Exception {
        TestDocument doc = TEST_DATA.anyDocumentForDepartment("dept_a_1");
        String docUrl = "/logs/_termvectors/" + doc.getId() + "?pretty=true&fields=*";

        try (GenericRestClient client = cluster.getRestClient(EXCLUDE_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_ip") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_ip.keyword") == null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_loc") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_loc.keyword") != null);
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_ip") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_ip.keyword") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_loc") != null);
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsNode("term_vectors").get("source_loc.keyword") != null);
        }

    }

}
