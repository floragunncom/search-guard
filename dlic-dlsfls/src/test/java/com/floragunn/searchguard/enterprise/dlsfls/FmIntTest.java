/*
 * Copyright 2016-2024 by floragunn GmbH - All rights reserved
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
import java.util.regex.Pattern;

import com.floragunn.searchguard.test.RestMatchers;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestIndexLike;
import com.floragunn.searchguard.test.TestIndexTemplate;
import org.elasticsearch.common.settings.Settings;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;

/**
 * The test checks if field masking works as expected for indices, aliases and data stream. The test data contains information about IPs and
 * locations. Various users have assigned distinct roles which impose IP and location field masking or not. The test outcome is verified with regular
 * expressions, which checks if the returned information was properly hashed. The test uses a parametrized runner, which groups an index-like object
 * and a user. The user object contains matchers that are used to verify whether SG returned data which was properly hashed. Matchers executed to
 * users are based on defined regexps.
 */
@RunWith(Parameterized.class)
public class FmIntTest {

    /**
     * Increase DOC_COUNT for manual test runs with bigger test data sets
     */
    static final int DOC_COUNT = 200;
    public static final String SOURCE_IP = "102.101.145.140";
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT).timestampColumnName("@timestamp").deletedDocumentFraction(0).get();
    static final String INDEX = "index_logs";
    static final TestIndex TEST_INDEX = new TestIndex(INDEX, Settings.builder().put("index.number_of_shards", 5).build(), TEST_DATA);
    static final TestAlias TEST_ALIAS = new TestAlias("alias_logs", TEST_INDEX);
    static final TestDataStream TEST_DATA_STREAM = new TestDataStream("web_data_stream_logs", TEST_DATA);

    static final Pattern HEX_HASH_PATTERN = Pattern.compile("[0-9a-f]{64}");
    static final Pattern IP_ADDRESS_PATTERN = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");
    static final Pattern LOCATION_PATTERN = Pattern.compile("[A-Za-zöü]+");

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").dataStreamPermissions("*").on("*").clusterPermissions("*"))
            .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("_source.source_ip", false, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
           .addFieldValueMatcher("hits.hits[*]._source.source_loc", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
            .addFieldValueMatcher("hits.total.value", false, equalTo(1));

    static final TestSgConfig.User HASHED_IP_INDEX_USER = new TestSgConfig.User("hashed_ip_index_user").roles(
            new Role("hashed_ip_index_role").indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_ip").on(INDEX).clusterPermissions("*"))
            .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
            .addFieldValueMatcher("_source.source_ip", false, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
            .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
            .addFieldValueMatcher("hits.total.value", false, equalTo(0));

    static final TestSgConfig.User HASHED_IP_ALIAS_USER = new TestSgConfig.User("hashed_ip_alias_user").roles(
            new Role("hashed_ip_alias_role").aliasPermissions("SGS_READ", "indices:admin/mappings/get", "indices:data/read/get", "indices:data/read/search").maskedFields("*_ip").on(TEST_ALIAS.getName()).clusterPermissions("*"))
        .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher("_source.source_ip", false, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
        .addFieldValueMatcher("hits.total.value", false, equalTo(0));

    static final TestSgConfig.User HASHED_IP_DATA_STREAM_USER = new TestSgConfig.User("hashed_ip_data_stream_user").roles(
            new Role("hashed_ip_data_stream_role").dataStreamPermissions("SGS_READ", "indices:admin/mappings/get", "indices:data/read/get").maskedFields("*_ip").on(TEST_DATA_STREAM.getName()).clusterPermissions("*"))
        .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher("_source.source_ip", false, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
        .addFieldValueMatcher("hits.total.value", false, equalTo(0));

    static final TestSgConfig.User HASHED_LOC_USER = new TestSgConfig.User("hashed_loc").roles(
            new Role("hashed_ip")
                .indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_loc").on(INDEX)
                .dataStreamPermissions("SGS_READ", "indices:admin/mappings/get", "indices:data/read/get").maskedFields("*_loc").on(TEST_DATA_STREAM.getName())
                .clusterPermissions("*"))
        .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
        .addFieldValueMatcher("_source.source_ip", false, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
        .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(LOCATION_PATTERN)))
        .addFieldValueMatcher("hits.total.value", false, equalTo(1));

    /**
     * The SUPER_UNLIMITED_USER authenticates with an admin cert, which will cause all access control code to be skipped.
     * This serves as a base for comparison with the default behavior.
     */
    static TestSgConfig.User SUPER_UNLIMITED_USER = new TestSgConfig.User("super_unlimited_user")//
            .description("super unlimited (admin cert)")//
            .adminCertUser()//
            .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("_source.source_ip", false, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
           .addFieldValueMatcher("hits.hits[*]._source.source_loc", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
            .addFieldValueMatcher("hits.total.value", false, equalTo(1));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, HASHED_IP_INDEX_USER, HASHED_LOC_USER, HASHED_IP_ALIAS_USER, HASHED_IP_DATA_STREAM_USER, SUPER_UNLIMITED_USER)
        .indices(TEST_INDEX)
        .aliases(TEST_ALIAS)
        .indexTemplates(new TestIndexTemplate("ds_test", TEST_DATA_STREAM.getName() + "*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
        .dataStreams(TEST_DATA_STREAM)
        .resources("dlsfls")
        .useExternalProcessCluster().build();

    private final TestIndexLike testIndexLike;
    private final TestSgConfig.User user;

    @Parameterized.Parameters(name = "{0} {1}")
    public static Collection<Object[]> params() {
        return ImmutableList.of(
            new Object[] {TEST_INDEX, HASHED_IP_INDEX_USER},
            new Object[] {TEST_INDEX, ADMIN },
            new Object[] {TEST_INDEX, SUPER_UNLIMITED_USER },
            new Object[] {TEST_INDEX, HASHED_LOC_USER },
            new Object[] {TEST_DATA_STREAM, HASHED_IP_DATA_STREAM_USER},
            new Object[] {TEST_DATA_STREAM, ADMIN },
            new Object[] {TEST_DATA_STREAM, SUPER_UNLIMITED_USER },
            new Object[] {TEST_DATA_STREAM, HASHED_LOC_USER },
            new Object[] {TEST_ALIAS, HASHED_IP_ALIAS_USER },
            new Object[] {TEST_ALIAS, ADMIN },
            new Object[] {TEST_ALIAS, SUPER_UNLIMITED_USER },
            new Object[] {TEST_ALIAS, HASHED_LOC_USER }
        );
    }

    public FmIntTest(TestIndexLike testIndexLike, TestSgConfig.User user) {
        this.testIndexLike = testIndexLike;
        this.user = user;
    }

    @Test
    public void search_hashed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.get("/" + testIndexLike.getName() + "/_search?pretty");

            assertThat(response, RestMatchers.isOk());
            assertThat(response, user.matcherForField("hits.hits[*]._source.source_ip"));
        }
    }

    @Test
    public void get_hashed() throws Exception {
        String docId = TEST_DATA.anyDocument().getId();
        String docUrl = "/" + testIndexLike.getName() + "/_doc/" + docId + "?pretty";

        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.get(docUrl);

            if(testIndexLike instanceof TestDataStream) {
                // it seems that ES does not support GET document operation for data streams
                assertThat(response, RestMatchers.isNotFound());
            } else {
                assertThat(response, RestMatchers.isOk());
                assertThat(response, user.matcherForField("_source.source_ip"));
            }
        }
    }

    @Test
    public void terms_aggregation() throws Exception {

        String query = "{" + "\"query\" : {" + "\"match_all\": {}" + "}," + "\"aggs\" : {"
                + "\"test_agg\" : { \"terms\" : { \"field\" : \"source_loc.keyword\" } }" + "}" + "}";

        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.postJson("/" + testIndexLike.getName() + "/_search?size=0&pretty", query);
            assertThat(response, RestMatchers.isOk());

            assertThat(response, user.matcherForField("aggregations.test_agg.buckets[*].key"));
        }
    }

    @Test
    public void search_masked_terms() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.get("/" + testIndexLike.getName() + "/_search?pretty&q=source_ip:" + SOURCE_IP);
            assertThat(response, RestMatchers.isOk());
            assertThat(response, user.matcherForField("hits.total.value"));
        }
    }
}
