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
/*
 * Includes code from https://github.com/opensearch-project/security/blob/70591197c705ca6f42f765186a05837813f80ff3/src/integrationTest/java/org/opensearch/security/privileges/dlsfls/FlsFmIntegrationTests.java
 * which is Copyright OpenSearch Contributors
 */

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.RestMatchers;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestIndexLike;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.google.common.net.InetAddresses;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matcher;
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
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
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

    private static final Logger log = LogManager.getLogger(FmIntTest.class);

    /**
     * Increase DOC_COUNT for manual test runs with bigger test data sets
     */
    static final int DOC_COUNT = 200;
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT) //
            .timestampColumnName("@timestamp") //
            .seed(1)
            .deletedDocumentFraction(0).get();
    static final String INDEX = "index_logs";
    static final TestIndex TEST_INDEX = new TestIndex(INDEX, Settings.builder().put("index.number_of_shards", 5).build(), TEST_DATA);
    static final TestAlias TEST_ALIAS = new TestAlias("alias_logs", TEST_INDEX);
    static final TestDataStream TEST_DATA_STREAM = new TestDataStream("web_data_stream_logs", TEST_DATA, false, 0);

    static final Pattern HEX_HASH_PATTERN = Pattern.compile("anonymized_[0-9a-f]{64}");
    static final Pattern IP_ADDRESS_PATTERN = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");
    static final Pattern LOCATION_PATTERN = Pattern.compile("[A-Za-zöüß]+");

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").dataStreamPermissions("*").on("*").clusterPermissions("*"))
            .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("hits.hits[*]._source.geo_point", true, not(matchesPattern(HEX_HASH_PATTERN)))
            .addFieldValueMatcher("_source.source_ip", false, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher( "_source.geo_point", false, not(matchesPattern(HEX_HASH_PATTERN)))
            .addFieldValueMatcher("hits.hits[*]._source.source_loc", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
            .addFieldValueMatcher("hits.total.value", false, equalTo(1))
            .addFieldValueMatcher("$.hits.total.value", false, greaterThan(0))
            .addFieldValueMatcher("$.hits.hits[*].fields", true,
                    //in case of index/alias geo_points have coordinates field
                    hasEntry(equalTo("geo_point"), everyItem(hasEntry(equalTo("coordinates"), everyItem(not(matchesPattern(HEX_HASH_PATTERN)))))),
                    hasEntry(equalTo("source_ip"), everyItem(allOf(not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))))
            );

    static final TestSgConfig.User HASHED_IP_INDEX_USER = new TestSgConfig.User("hashed_ip_index_user").roles(
            new Role("hashed_ip_index_role").indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_ip").on(INDEX).clusterPermissions("*"))
            .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
            .addFieldValueMatcher("hits.hits[*]._source.geo_point", true, not(matchesPattern(HEX_HASH_PATTERN)))
            .addFieldValueMatcher("_source.source_ip", false, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
            .addFieldValueMatcher( "_source.geo_point", false, not(matchesPattern(HEX_HASH_PATTERN)))
            .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
            .addFieldValueMatcher("hits.total.value", false, equalTo(0))
            .addFieldValueMatcher("$.hits.total.value", false, equalTo(0))
            .addFieldValueMatcher("$.hits.hits[*].fields", true,
                    hasEntry(equalTo("geo_point"), everyItem(hasEntry(equalTo("coordinates"), everyItem(not(matchesPattern(HEX_HASH_PATTERN)))))),
                    not(hasEntry(equalTo("source_ip"), everyItem(matchesPattern(IP_ADDRESS_PATTERN))))
            );

    static final TestSgConfig.User HASHED_IP_ALIAS_USER = new TestSgConfig.User("hashed_ip_alias_user").roles(
            new Role("hashed_ip_alias_role").aliasPermissions("SGS_READ", "indices:admin/mappings/get", "indices:data/read/get", "indices:data/read/search").maskedFields("*_ip").on(TEST_ALIAS.getName()).clusterPermissions("*"))
        .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher("hits.hits[*]._source.geo_point", true, not(matchesPattern(HEX_HASH_PATTERN)))
        .addFieldValueMatcher("_source.source_ip", false, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher( "_source.geo_point", false, not(matchesPattern(HEX_HASH_PATTERN)))
        .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
        .addFieldValueMatcher("hits.total.value", false, equalTo(0))
        .addFieldValueMatcher("$.hits.total.value", false, equalTo(0))
        .addFieldValueMatcher("$.hits.hits[*].fields", true,
                hasEntry(equalTo("geo_point"), everyItem(hasEntry(equalTo("coordinates"), everyItem(not(matchesPattern(HEX_HASH_PATTERN)))))),
                not(hasEntry(equalTo("source_ip"), everyItem(allOf(matchesPattern(IP_ADDRESS_PATTERN)))))
        );

    static final TestSgConfig.User HASHED_IP_DATA_STREAM_USER = new TestSgConfig.User("hashed_ip_data_stream_user").roles(
            new Role("hashed_ip_data_stream_role").dataStreamPermissions("SGS_READ", "indices:admin/mappings/get", "indices:data/read/get").maskedFields("*_ip").on(TEST_DATA_STREAM.getName()).clusterPermissions("*"))
        .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher("hits.hits[*]._source.geo_point", true, not(matchesPattern(HEX_HASH_PATTERN)))
        .addFieldValueMatcher("_source.source_ip", false, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(IP_ADDRESS_PATTERN)))
        .addFieldValueMatcher("_source.geo_point", false, not(matchesPattern(HEX_HASH_PATTERN)))
        .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
        .addFieldValueMatcher("hits.total.value", false, equalTo(0))
        .addFieldValueMatcher("$.hits.total.value", false, equalTo(0))
        .addFieldValueMatcher("$.hits.hits[*].fields", true,
                hasEntry(equalTo("geo_point"), everyItem(hasEntry(equalTo("coordinates"), everyItem(not(matchesPattern(HEX_HASH_PATTERN)))))),
                not(hasEntry(equalTo("source_ip"), everyItem(matchesPattern(IP_ADDRESS_PATTERN))))
        );

    static final TestSgConfig.User HASHED_LOC_USER = new TestSgConfig.User("hashed_loc").roles(
            new Role("hashed_ip")
                .indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("*_loc").on(INDEX)
                .dataStreamPermissions("SGS_READ", "indices:admin/mappings/get", "indices:data/read/get").maskedFields("*_loc").on(TEST_DATA_STREAM.getName())
                .clusterPermissions("*"))
        .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
        .addFieldValueMatcher( "hits.hits[*]._source.geo_point", true, not(matchesPattern(HEX_HASH_PATTERN)))
        .addFieldValueMatcher( "_source.source_ip", false, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
        .addFieldValueMatcher( "_source.geo_point", false, not(matchesPattern(HEX_HASH_PATTERN)))
        .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, matchesPattern(HEX_HASH_PATTERN), not(matchesPattern(LOCATION_PATTERN)))
        .addFieldValueMatcher("hits.total.value", false, equalTo(1))
        .addFieldValueMatcher("$.hits.total.value", false, greaterThan(0))
        .addFieldValueMatcher("$.hits.hits[*].fields", true,
                hasEntry(equalTo("geo_point"), everyItem(hasEntry(equalTo("coordinates"), everyItem(not(matchesPattern(HEX_HASH_PATTERN)))))),
                hasEntry(equalTo("source_ip"), everyItem(allOf(not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))))
        );

    static final TestSgConfig.User HASHED_GEO_POINT_USER = new TestSgConfig.User("hashed_geo_point").roles(
            new Role("hashed_geo_point")
                    // please see: https://git.floragunn.com/search-guard/confidential/search-guard-suite-enterprise/-/merge_requests/13#note_34253
                .indexPermissions("SGS_READ", "indices:admin/mappings/get").maskedFields("geo_point").on(INDEX)
                .dataStreamPermissions("SGS_READ", "indices:admin/mappings/get", "indices:data/read/get").maskedFields("geo_point").on(TEST_DATA_STREAM.getName())
                .clusterPermissions("*"))
        .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
        .addFieldValueMatcher("hits.hits[*]._source.geo_point", true, matchesPattern(HEX_HASH_PATTERN))
        .addFieldValueMatcher("_source.source_ip", false, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
        .addFieldValueMatcher( "_source.geo_point", false, matchesPattern(HEX_HASH_PATTERN))
        .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
        .addFieldValueMatcher("hits.total.value", false, equalTo(1))
        .addFieldValueMatcher("$.hits.total.value", false, greaterThan(0))
        .addFieldValueMatcher("$.hits.hits[*].fields", true,
                //in case of index/alias some documents don't contain geo_point field at all, while others contain it with unmasked value
                not(hasEntry(equalTo("geo_point"), everyItem(anything()))),
                hasEntry(equalTo("source_ip"), everyItem(allOf(not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))))
        );

    /**
     * The SUPER_UNLIMITED_USER authenticates with an admin cert, which will cause all access control code to be skipped.
     * This serves as a base for comparison with the default behavior.
     */
    static TestSgConfig.User SUPER_UNLIMITED_USER = new TestSgConfig.User("super_unlimited_user")//
            .description("super unlimited (admin cert)")//
            .adminCertUser()//
            .addFieldValueMatcher("hits.hits[*]._source.source_ip", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("hits.hits[*]._source.geo_point", true, not(matchesPattern(HEX_HASH_PATTERN)))
            .addFieldValueMatcher("_source.source_ip", false, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("_source.geo_point", false, not(matchesPattern(HEX_HASH_PATTERN)))
            .addFieldValueMatcher("hits.hits[*]._source.source_loc", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))
            .addFieldValueMatcher("aggregations.test_agg.buckets[*].key", true, not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(LOCATION_PATTERN))
            .addFieldValueMatcher("hits.total.value", false, equalTo(1))
            .addFieldValueMatcher("$.hits.total.value", false, greaterThan(0))
            .addFieldValueMatcher("$.hits.hits[*].fields", true,
                    // Masking geo_points is tricky: https://git.floragunn.com/search-guard/confidential/search-guard-suite-enterprise/-/merge_requests/13#note_34253
                    // The global prefix for anonymized fields "anonymized_" is configured to ensure consistent behaviour during tests
                    hasEntry(equalTo("geo_point"), everyItem(hasEntry(equalTo("coordinates"), everyItem(not(matchesPattern(HEX_HASH_PATTERN)))))),
                    hasEntry(equalTo("source_ip"), everyItem(allOf(not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(IP_ADDRESS_PATTERN))))
            );

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    // we need a prefix here to ensure consistent geo_point masking:
    // https://git.floragunn.com/search-guard/confidential/search-guard-suite-enterprise/-/merge_requests/13#note_34253
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().fieldAnonymizationPrefix("anonymized_");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, HASHED_IP_INDEX_USER, HASHED_LOC_USER, HASHED_IP_ALIAS_USER, HASHED_IP_DATA_STREAM_USER, SUPER_UNLIMITED_USER, HASHED_GEO_POINT_USER)
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
            new Object[] {TEST_ALIAS, HASHED_LOC_USER },
            new Object[] {TEST_INDEX, HASHED_GEO_POINT_USER },
            new Object[] {TEST_ALIAS, HASHED_GEO_POINT_USER },
            new Object[] {TEST_DATA_STREAM, HASHED_GEO_POINT_USER }
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
            assertThat(response, user.matcherForField("hits.hits[*]._source.geo_point"));
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
                assertThat(response, user.matcherForField("_source.geo_point"));
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
            GenericRestClient.HttpResponse response = client.get("/" + testIndexLike.getName() + "/_search?pretty&q=source_ip:" + getAnySourceIp());
            assertThat(response, RestMatchers.isOk());
            assertThat(response, user.matcherForField("hits.total.value"));
        }
    }

    @Test
    public void search_masked_terms_dsl() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            String sourceIp = getAnySourceIp();
            DocNode query = DocNode.of("query.term.source_ip", sourceIp);
            GenericRestClient.HttpResponse response = client.postJson("/" + testIndexLike.getName() + "/_search?pretty", query);
            Matcher<GenericRestClient.HttpResponse> matcher = user.matcherForField("hits.total.value");
            assertThat(response, RestMatchers.isOk());
            assertThat(response, matcher);
        }
    }

    private String getAnySourceIp() {
        return testIndexLike.firstDocument() //
                .get("source_ip") //
                .toString();
    }

    @Test
    public void search_query_range_ip() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            String docIp = DocNode.wrap(TEST_DATA.anyDocument().getContent()).getAsString("source_ip");
            String nextIp = InetAddresses.increment(InetAddresses.forString(docIp)).getHostAddress();
            String prevIp = InetAddresses.decrement(InetAddresses.forString(docIp)).getHostAddress();
            DocNode query = DocNode.of("query.range.source_ip.gt", prevIp, "query.range.source_ip.lt", nextIp);
            GenericRestClient.HttpResponse response = client.postJson("/" + testIndexLike.getName() + "/_search?pretty", query);
            assertThat(response, RestMatchers.isOk());
            assertThat(response, user.matcherForField("$.hits.total.value"));

        }
    }

    @Test
    public void search_masked_fields() throws Exception {
//        user "hashed_ip_index_user" - hashed ip is returned in ignored_field_values field e.g.
//          {
//        "_index" : "index_logs",
//        "_id" : "ZhqHQXjYp8Otz8tgdOghEw",
//        "_score" : 1.0,
//        "fields" : {
//          "geo_point" : [
//            {
//              "coordinates" : [
//                -121.60911392828541,
//                -33.1063037764124
//              ],
//              "type" : "Point"
//            }
//          ]
//        },
//        "ignored_field_values" : {
//          "source_ip" : [
//            "25f111f4b6f86ea7387fe7cde0d371da09dbbe17828a3606d66f2f0cc470fe54"
//          ]
//        }
//      }
//
//      This might be by ES design, please see https://github.com/elastic/elasticsearch/blob/b98606e71237bee596883b4741ed56982b666b53/docs/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#L329
//      source_ip is of type IP whereas hashed field value 25f111f4b6f86ea7387fe7cde0d371da09dbbe17828a3606d66f2f0cc470fe54 is not an IP address...
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse mappingsResponse = client.get("/" + testIndexLike.getName() + "/_mappings?pretty");
            log.debug("Mappings for index like '{}' response with status code '{}' and body '{}'", testIndexLike.getName(), mappingsResponse.getStatusCode(), mappingsResponse.getBody());

            DocNode body = DocNode.of("fields", Arrays.asList("source_ip", "geo_point"), "_source", false);
            GenericRestClient.HttpResponse response = client.postJson("/" + testIndexLike.getName() + "/_search?pretty", body);
            assertThat(response, RestMatchers.isOk());
            Matcher<GenericRestClient.HttpResponse> matcher = user.matcherForField("$.hits.hits[*].fields");
            log.debug("search_masked_fields test: User '{}' received response with code '{}' selected matcher '{}' and response body '{}'", user.getName(), response.getStatusCode(), matcher, response.getBody());
            assertThat(response, matcher);

        }
    }
}
