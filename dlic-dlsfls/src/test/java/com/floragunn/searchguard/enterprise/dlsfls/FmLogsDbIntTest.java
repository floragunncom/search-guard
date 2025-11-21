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

import com.floragunn.codova.documents.DocNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.noValueAt;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

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

    private static final Logger log = LogManager.getLogger(FmLogsDbIntTest.class);

    /**
     * Increase DOC_COUNT for manual test runs with bigger test data sets
     */
    static final int DOC_COUNT = 200;
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT) //
            .seed(1) //
            .timestampColumnName("@timestamp") //
            .get();
    static final String LOGS_DB_INDEX_NAME = "logs";
    static final String INDEX_PATTERN = LOGS_DB_INDEX_NAME + "*";
    private final static String REGULAR_INDEX_NAME = "my-regular-index-000001";
  
    static final Pattern HEX_HASH_PATTERN = Pattern.compile("[0-9a-f]+");
    static final Pattern IP_ADDRESS_PATTERN = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static final TestSgConfig.User HASHED_IP_USER = new TestSgConfig.User("hashed_ip").roles(
            new Role("hashed_ip").indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_ip").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User HASHED_LOC_USER = new TestSgConfig.User("hashed_loc").roles(
            new Role("hashed_ip").indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_loc").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User A_TEST_USER = new TestSgConfig.User("a_test_user") //
            .roles(new Role("a_test_role") //
                    .clusterPermissions("CLUSTER_COMPOSITE_OPS_RO") //
                    .indexPermissions("SGS_INDICES_ALL") //
                    .maskedFields("location") //
                    .on("my*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled() //
            .authc(AUTHC).dlsFls(DLSFLS).users(ADMIN, HASHED_IP_USER, HASHED_LOC_USER, A_TEST_USER).resources("dlsfls") //
            .nodeSettings("indices.id_field_data.enabled", true) // needed for sorting by document _id
            // An external process cluster is used due to the use of LogsDB indices, which requires an additional native library.
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            // null means default mode which is currently normal
            Settings settings = Settings.builder().put("index.number_of_shards", 5).put("index.mode", "logsdb").build();
            String indexMode = TEST_DATA.createIndex(client, LOGS_DB_INDEX_NAME, settings);
            assertThat(indexMode, equalTo("logsdb"));

            // Data related to geo point masking tests
            DocNode mappings = DocNode.of("mappings.properties.location.type", "geo_point");
            client.putJson("/" + REGULAR_INDEX_NAME, mappings);


            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/1?refresh=true", DocNode.of(
                    "text", "Geopoint as an object using GeoJSON format",
                    "location", DocNode.of("type", "Point", "coordinates", ImmutableList.of(-71.34, 41.12))));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/2?refresh=true", DocNode.of(
                    "text", "Geopoint as a WKT POINT primitive",
                    "location", "POINT (-71.34 41.12)"
            ));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/3?refresh=true", DocNode.of(
                    "text", "Geopoint as an object with 'lat' and 'lon' keys",
                    "location", DocNode.of("lat", 41.12, "lon", -71.34)
            ));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/4?refresh=true", DocNode.of(
                    "text", "Geopoint as an array",
                    "location", ImmutableList.of(-71.34, 41.12)
            ));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/5?refresh=true", DocNode.of(
                    "text", "Geopoint as a string",
                    "location", "41.12,-71.34"
            ));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/6?refresh=true", DocNode.of(
                    "text", "Geopoint as a geohash",
                    "location", "drm3btev3e86"
            ));
        }
    }

    @Test
    public void search_hashed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(HASHED_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + LOGS_DB_INDEX_NAME + "/_search?pretty");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]._source.source_ip")
                    .map((n) -> n.toString()).forAllApplies((s) -> HEX_HASH_PATTERN.matcher(s).matches()));
            Assert.assertFalse(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]._source.source_ip")
                    .map((n) -> n.toString()).forAllApplies((s) -> IP_ADDRESS_PATTERN.matcher(s).matches()));
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + LOGS_DB_INDEX_NAME + "/_search?pretty");
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
        String docUrl = "/" + LOGS_DB_INDEX_NAME + "/_doc/" + docId + "?pretty";

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
            GenericRestClient.HttpResponse response = client.postJson("/" + LOGS_DB_INDEX_NAME + "/_search?pretty", query);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            ImmutableList<String> keys = response.getBodyAsDocNode().findNodesByJsonPath("aggregations.test_agg.buckets[*].key")
                    .map((docNode) -> (String) docNode.toBasicObject());
            Assert.assertTrue(response.getBody(), keys.forAllApplies((k) -> k.matches("[0-9a-f]{64}")));
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + LOGS_DB_INDEX_NAME + "/_search?pretty", query);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            ImmutableList<String> keys = response.getBodyAsDocNode().findNodesByJsonPath("aggregations.test_agg.buckets[*].key")
                    .map((docNode) -> (String) docNode.toBasicObject());
            Assert.assertFalse(response.getBody(), keys.forAllApplies((k) -> k.matches("[0-9a-f]{64}")));
        }
    }

    @Test
    public void search_masked_terms() throws Exception {
        String sourceIp = TEST_DATA.anyDocument().getContent().get("source_ip").toString();

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + LOGS_DB_INDEX_NAME + "/_search?pretty&q=source_ip:" + sourceIp);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            System.out.println(response.getBody());
            Assert.assertFalse(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]").isEmpty());
        }

        try (GenericRestClient client = cluster.getRestClient(HASHED_IP_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + LOGS_DB_INDEX_NAME + "/_search?pretty&q=source_ip:" + sourceIp);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            System.out.println(response.getBody());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]").isEmpty());
        }
    }

    @Test
    public void shouldMaskGeoPoint() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(A_TEST_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + REGULAR_INDEX_NAME + "/_search?pretty&sort=_id:asc");
            log.info("Response status code: {} and body '{}'",response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._source.text", everyItem(containsString("Geopoint")))));
            assertThat(response, json(noValueAt("hits.hits[0]._source.location")));
            assertThat(response, json(nodeAt("hits.hits[1]._source.location", is("14d582bf2558ca29eb5abbdc65893f8ff7f3b74d760c0344fb78ae595751b443"))));
            assertThat(response, json(noValueAt("hits.hits[2]._source.location")));
            assertThat(response, json(noValueAt("hits.hits[3]._source.location")));
            assertThat(response, json(nodeAt("hits.hits[4]._source.location", is("9c39927fff39ffbcc038768751f540f149a84c05c7f8b87545bca79d410a1225"))));
            assertThat(response, json(nodeAt("hits.hits[5]._source.location", is("2246bc1683251cfed0145a3145b5d39b08991790b55ea934688520f778202764"))));
        }
    }
}
