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
import java.util.regex.Pattern;

import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.jayway.jsonpath.Configuration.Defaults;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class FmLogsDbIntTest {
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
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT).timestampColumnName("@timestamp").get();
    static final String INDEX_NAME = "logs";
    static final String INDEX_PATTERN = INDEX_NAME + "*";
  
    static final Pattern HEX_HASH_PATTERN = Pattern.compile("[0-9a-f]+");
    static final Pattern IP_ADDRESS_PATTERN = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static final TestSgConfig.User HASHED_IP_USER = new TestSgConfig.User("hashed_ip").roles(
            new Role("hashed_ip").indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_ip").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User HASHED_LOC_USER = new TestSgConfig.User("hashed_loc").roles(
            new Role("hashed_ip").indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_loc").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled() //
            .authc(AUTHC).dlsFls(DLSFLS).users(ADMIN, HASHED_IP_USER, HASHED_LOC_USER).resources("dlsfls") //
            // An external process cluster is used due to the use of LogsDB indices, which requires an additional native library.
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void setupTestData() throws IOException {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            // null means default mode which is currently normal
            Settings settings = Settings.builder().put("index.number_of_shards", 5).put("index.mode", "logsdb").build();
            String indexMode = TEST_DATA.createIndex(client, INDEX_NAME, settings);
            assertThat(indexMode, equalTo("logsdb"));
        }
    }

    @Test
    public void search_hashed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(HASHED_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + INDEX_NAME + "/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]._source.source_ip")
                    .map((n) -> n.toString()).forAllApplies((s) -> HEX_HASH_PATTERN.matcher(s).matches()));
            Assert.assertFalse(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]._source.source_ip")
                    .map((n) -> n.toString()).forAllApplies((s) -> IP_ADDRESS_PATTERN.matcher(s).matches()));
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + INDEX_NAME + "/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertFalse(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]._source.source_ip")
                    .map((n) -> n.toString()).forAllApplies((s) -> HEX_HASH_PATTERN.matcher(s).matches()));
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]._source.source_ip")
                    .map((n) -> n.toString()).forAllApplies((s) -> IP_ADDRESS_PATTERN.matcher(s).matches()));
        }
    }

    @Test
    public void get_hashed() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/" + INDEX_NAME + "/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(HASHED_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    HEX_HASH_PATTERN.matcher(response.getBodyAsDocNode().getAsNode("_source").getAsString("source_ip")).matches());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);
            Assert.assertTrue(response.getBody(),
                    IP_ADDRESS_PATTERN.matcher(response.getBodyAsDocNode().getAsNode("_source").getAsString("source_ip")).matches());
        }
    }

    @Test
    public void terms_aggregation() throws Exception {

        String query = "{" + "\"query\" : {" + "\"match_all\": {}" + "}," + "\"aggs\" : {"
                + "\"test_agg\" : { \"terms\" : { \"field\" : \"source_loc.keyword\" } }" + "}" + "}";

        try (GenericRestClient client = cluster.getRestClient(HASHED_LOC_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + INDEX_NAME + "/_search?pretty", query);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            ImmutableList<String> keys = response.getBodyAsDocNode().findNodesByJsonPath("aggregations.test_agg.buckets[*].key")
                    .map((docNode) -> (String) docNode.toBasicObject());
            Assert.assertTrue(response.getBody(), keys.forAllApplies((k) -> k.matches("[0-9a-f]{64}")));
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + INDEX_NAME + "/_search?pretty", query);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            ImmutableList<String> keys = response.getBodyAsDocNode().findNodesByJsonPath("aggregations.test_agg.buckets[*].key")
                    .map((docNode) -> (String) docNode.toBasicObject());
            Assert.assertFalse(response.getBody(), keys.forAllApplies((k) -> k.matches("[0-9a-f]{64}")));
        }
    }

    @Test
    public void search_masked_terms() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + INDEX_NAME + "/_search?pretty&q=source_ip:102.101.145.140");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            System.out.println(response.getBody());
            Assert.assertFalse(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]").isEmpty());
        }

        try (GenericRestClient client = cluster.getRestClient(HASHED_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + INDEX_NAME + "/_search?pretty&q=source_ip:102.101.145.140");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            System.out.println(response.getBody());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]").isEmpty());
        }
    }
}
