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
import java.util.Set;
import java.util.regex.Pattern;

import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestIndexLike;
import com.floragunn.searchguard.test.TestIndexTemplate;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.jayway.jsonpath.Configuration.Defaults;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;

@RunWith(Parameterized.class)
public class FmIntTest {
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
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT).timestampAttributeName("@timestamp").deletedDocumentFraction(0).get();
    static final String INDEX = "index_logs";
    static final TestIndex TEST_INDEX = new TestIndex(INDEX, Settings.builder().put("index.number_of_shards", 5).build(), TEST_DATA);
    static final TestAlias TEST_ALIAS = new TestAlias("alias_logs", TEST_INDEX);
    static final TestDataStream TEST_DATA_STREAM = new TestDataStream("web_data_stream_logs", TEST_DATA);


    static final Pattern HEX_HASH_PATTERN = Pattern.compile("[0-9a-f]+");
    static final Pattern IP_ADDRESS_PATTERN = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").dataStreamPermissions("*").on("*").clusterPermissions("*"))
            .addFieldValueMatcher("source_ip", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("source_ip", false, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN));

    static final TestSgConfig.User HASHED_IP_INDEX_USER = new TestSgConfig.User("hashed_ip_index_user").roles(
            new Role("hashed_ip_index_role").indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_ip").on(INDEX).clusterPermissions("*"))
            .addFieldValueMatcher("source_ip", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
            .addFieldValueMatcher("source_ip", false, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)));

    static final TestSgConfig.User HASHED_IP_ALIAS_USER = new TestSgConfig.User("hashed_ip_alias_user").roles(
            new Role("hashed_ip_alias_role").aliasPermissions("SGS_READ", "indices:admin/mappings/get", "indices:data/read/get", "indices:data/read/search").maskedFields("*_ip").on(TEST_ALIAS.getName()).clusterPermissions("*"))
        .addFieldValueMatcher("source_ip", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher("source_ip", false, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)));

    static final TestSgConfig.User HASHED_IP_DATA_STREAM_USER = new TestSgConfig.User("hashed_ip_data_stream_user").roles(
            new Role("hashed_ip_data_stream_role").dataStreamPermissions("SGS_READ", "indices:admin/mappings/get", "indices:data/read/get").maskedFields("*_ip").on(TEST_DATA_STREAM.getName()).clusterPermissions("*"))
        .addFieldValueMatcher("source_ip", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher("source_ip", false, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)));

    static final TestSgConfig.User HASHED_LOC_USER = new TestSgConfig.User("hashed_loc").roles(
            new Role("hashed_ip").indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_loc").on(INDEX).clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().useImpl("flx");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, HASHED_IP_INDEX_USER, HASHED_LOC_USER, HASHED_IP_ALIAS_USER, HASHED_IP_DATA_STREAM_USER)
        .indices(TEST_INDEX)
        .aliases(TEST_ALIAS)
        .indexTemplates(new TestIndexTemplate("ds_test", TEST_DATA_STREAM.getName() + "*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
        .dataStreams(TEST_DATA_STREAM)
        .resources("dlsfls")
        .useExternalProcessCluster().build();

    private final TestIndexLike indexLikeName;
    private final TestSgConfig.User user;

    @Parameterized.Parameters(name = "{0} {1}")
    public static Collection<Object[]> params() {
        return ImmutableList.of(
            new Object[] {TEST_INDEX, HASHED_IP_INDEX_USER},
            new Object[] {TEST_INDEX, ADMIN },
            new Object[] {TEST_DATA_STREAM, HASHED_IP_DATA_STREAM_USER},
            new Object[] {TEST_DATA_STREAM, ADMIN },
            new Object[] {TEST_ALIAS, HASHED_IP_ALIAS_USER },
            new Object[] {TEST_ALIAS, ADMIN }
        );
    }

    public FmIntTest(TestIndexLike indexLikeName, TestSgConfig.User user) {
        this.indexLikeName = indexLikeName;
        this.user = user;
    }

    @Test
    public void search_hashed() throws Exception {// TODO these tests works correctly always when indices are empty?
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexLikeName.getName() + "/_search?pretty");

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            assertThat(response, user.matcherForField("source_ip", true));
        }
    }

    @Test
    public void get_hashed() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/" + indexLikeName.getName() + "/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            //todo confirm that the below assertion works correctly
            assertThat(response, user.matcherForField("source_ip",false));
        }
    }

    @Test
    // TODO refactor to parametrized test to use aliases and data streams
    public void terms_aggregation() throws Exception {

        String query = "{" + "\"query\" : {" + "\"match_all\": {}" + "}," + "\"aggs\" : {"
                + "\"test_agg\" : { \"terms\" : { \"field\" : \"source_loc.keyword\" } }" + "}" + "}";

        try (GenericRestClient client = cluster.getRestClient(HASHED_LOC_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexLikeName.getName() + "/_search?pretty", query);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            ImmutableList<String> keys = response.getBodyAsDocNode().findNodesByJsonPath("aggregations.test_agg.buckets[*].key")
                    .map((docNode) -> (String) docNode.toBasicObject());
            Assert.assertTrue(response.getBody(), keys.forAllApplies((k) -> k.matches("[0-9a-f]{64}")));
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + indexLikeName.getName() + "/_search?pretty", query);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            ImmutableList<String> keys = response.getBodyAsDocNode().findNodesByJsonPath("aggregations.test_agg.buckets[*].key")
                    .map((docNode) -> (String) docNode.toBasicObject());
            Assert.assertFalse(response.getBody(), keys.forAllApplies((k) -> k.matches("[0-9a-f]{64}")));
        }
    }

    @Test
    // TODO refactor to parametrized test to use aliases and data streams
    public void search_masked_terms() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexLikeName.getName() + "/_search?pretty&q=source_ip:102.101.145.140");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            System.out.println(response.getBody());
            Assert.assertFalse(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]").isEmpty());
        }

        try (GenericRestClient client = cluster.getRestClient(HASHED_IP_INDEX_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + indexLikeName.getName() + "/_search?pretty&q=source_ip:102.101.145.140");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            System.out.println(response.getBody());
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]").isEmpty());
        }
    }
}
