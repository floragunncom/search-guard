package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.util.encoders.Hex;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class FieldMaskingAggregationTest {

    /**
     * Increase DOC_COUNT for manual test runs with bigger test data sets
     */
    private static final int DOC_COUNT = 1000;

    private final static TestSgConfig.User MASKED_TEST_USER = new TestSgConfig.User("masked_test")
            .roles(new Role("mask").indexPermissions("*").maskedFields("*ip::/[0-9]{1,3}$/::XXX", "source_loc").on("ip").clusterPermissions("*"));

    private final static TestSgConfig.User UNMASKED_TEST_USER = new TestSgConfig.User("unmasked_test")
            .roles(new Role("allaccess").indexPermissions("*").on("ip").clusterPermissions("*"));

    private final static byte[] salt = ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT_DEFAULT.getBytes(StandardCharsets.UTF_8);

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled()
            .users(MASKED_TEST_USER, UNMASKED_TEST_USER).resources("dlsfls_legacy").embedded().build();

    /**
     * This table also aggregates the test data and serves as reference for the tests
     */
    private static ReferenceAggregationTable referenceAggregationTable = new ReferenceAggregationTable()//
            .maskingFunction("source_loc", "hash", Masks::blake2bHash)//
            .maskingFunction("source_ip", "masked", (v) -> Masks.regexReplace(v, Pattern.compile("[0-9]{1,3}$"), "XXX"));

    private static final Logger log = LogManager.getLogger(FieldMaskingAggregationTest.class);

    @BeforeClass
    public static void setupTestData() {
        try (Client client = cluster.getInternalNodeClient()) {
            TestData testData = TestData.documentCount(DOC_COUNT).get();
            testData.createIndex(client, "ip", Settings.builder().put("index.number_of_shards", 5).build());
            referenceAggregationTable.add(testData.getRetainedDocuments().values());
        }
    }

    @Test
    public void testPartiallyMaskedField() throws Exception {
        // we need to set shardSize to DOC_COUNT in order to get precise results which allow matching on the reference table

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(MASKED_TEST_USER)) {
            SearchResponse makedSearchResponse = client.search(
                    new SearchRequest("ip").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10)
                            .aggregation(AggregationBuilders.terms("source_ip_terms").field("source_ip.keyword").size(100).shardSize(DOC_COUNT))),
                    RequestOptions.DEFAULT);

            log.info(Strings.toString(makedSearchResponse, true, true));

            ParsedStringTerms maskedAggregation = (ParsedStringTerms) makedSearchResponse.getAggregations().asList().get(0);

            for (int i = 0; i < maskedAggregation.getBuckets().size(); i++) {
                Terms.Bucket maskedBucket = maskedAggregation.getBuckets().get(i);

                Assert.assertEquals("Bucket " + i + ":\n" + toxToString(maskedBucket),
                        referenceAggregationTable.getCount("source_ip:masked", maskedBucket.getKeyAsString()), maskedBucket.getDocCount());
            }
        }
    }

    @Test
    public void testHashMaskedField() throws Exception {
        SearchRequest searchRequest = new SearchRequest("ip").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10)
                .aggregation(AggregationBuilders.terms("source_loc_terms").field("source_loc.keyword").size(1000)));
        SearchResponse makedSearchResponse;
        SearchResponse unmakedSearchResponse;

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(UNMASKED_TEST_USER)) {
            unmakedSearchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            log.info(Strings.toString(unmakedSearchResponse, true, true));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(MASKED_TEST_USER)) {
            makedSearchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            log.info(Strings.toString(makedSearchResponse, true, true));
        }

        compareHashedBuckets(makedSearchResponse, unmakedSearchResponse);
    }

    @Test
    public void testHashMaskedFieldWithShardSizeParam() throws Exception {
        SearchRequest searchRequest = new SearchRequest("ip").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10)
                .aggregation(AggregationBuilders.terms("source_loc_terms").field("source_loc.keyword").size(100).shardSize(1000)));
        SearchResponse makedSearchResponse;
        SearchResponse unmakedSearchResponse;

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(UNMASKED_TEST_USER)) {
            unmakedSearchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            log.info(Strings.toString(unmakedSearchResponse, true, true));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(MASKED_TEST_USER)) {
            makedSearchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            log.info(Strings.toString(makedSearchResponse, true, true));
        }

        compareHashedBuckets(makedSearchResponse, unmakedSearchResponse);
    }

    @Test
    public void testHashMaskedFieldOrderedByKey() throws Exception {
        // we need to set shardSize to DOC_COUNT in order to get precise results which allow matching on the reference table

        SearchRequest searchRequest = new SearchRequest("ip").source(
                new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10).aggregation(AggregationBuilders.terms("source_loc_terms")
                        .field("source_loc.keyword").order(BucketOrder.key(true)).size(100).shardSize(DOC_COUNT).showTermDocCountError(true)));
        SearchResponse makedSearchResponse;
        SearchResponse unmakedSearchResponse;

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(UNMASKED_TEST_USER)) {
            unmakedSearchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            log.info(Strings.toString(unmakedSearchResponse, true, true));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(MASKED_TEST_USER)) {
            makedSearchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            log.info(Strings.toString(makedSearchResponse, true, true));
        }

        ParsedStringTerms maskedAggregation = (ParsedStringTerms) makedSearchResponse.getAggregations().asList().get(0);

        for (int i = 0; i < maskedAggregation.getBuckets().size(); i++) {
            Terms.Bucket maskedBucket = maskedAggregation.getBuckets().get(i);

            Assert.assertEquals("Bucket " + i + ":\n" + toxToString(maskedBucket),
                    referenceAggregationTable.getCount("source_loc:hash", maskedBucket.getKeyAsString()), maskedBucket.getDocCount());
        }

    }

    private void compareHashedBuckets(SearchResponse makedSearchResponse, SearchResponse unmakedSearchResponse) {
        // Assume hashing does not map different location strings to one hash

        ParsedStringTerms maskedAggregation = (ParsedStringTerms) makedSearchResponse.getAggregations().asList().get(0);
        ParsedStringTerms unmaskedAggregation = (ParsedStringTerms) unmakedSearchResponse.getAggregations().asList().get(0);

        Assert.assertEquals(unmaskedAggregation.getBuckets().size(), maskedAggregation.getBuckets().size());

        // As terms with equal count may change their order between masked and unmaked states, we have to collect them before comparing
        Set<String> groupedUnmaskedTermsByCount = new HashSet<>();
        Set<String> groupedMaskedTermsByCount = new HashSet<>();
        Terms.Bucket prevUnmaskedBucket = null;
        int groupStart = 0;

        for (int i = 0; i < unmaskedAggregation.getBuckets().size(); i++) {

            Terms.Bucket unmaskedBucket = unmaskedAggregation.getBuckets().get(i);
            Terms.Bucket maskedBucket = maskedAggregation.getBuckets().get(i);

            if (prevUnmaskedBucket != null && prevUnmaskedBucket.getDocCount() != unmaskedBucket.getDocCount()) {
                Assert.assertEquals(
                        "Buckets at " + groupStart + " to " + (i - 1) + ":\n" + toxToString(unmaskedBucket) + "\n" + toxToString(maskedBucket),
                        groupedUnmaskedTermsByCount, groupedMaskedTermsByCount);

                groupedUnmaskedTermsByCount.clear();
                groupedMaskedTermsByCount.clear();
                groupStart = 1;
            }

            Assert.assertEquals("Bucket " + i + ":\n" + toxToString(unmaskedBucket) + "\n" + toxToString(maskedBucket), unmaskedBucket.getDocCount(),
                    unmaskedBucket.getDocCount());

            groupedUnmaskedTermsByCount.add(Masks.blake2bHash(unmaskedBucket.getKeyAsString()));
            groupedMaskedTermsByCount.add(maskedBucket.getKeyAsString());

            prevUnmaskedBucket = unmaskedBucket;

        }

    }

    private static String toxToString(ToXContent toXContentObject) {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().humanReadable(true);
            toXContentObject.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class ReferenceAggregationTable {
        private Map<String, Map<String, Integer>> aggregatedAttributeCounts = new HashMap<>();
        private Map<String, Map<String, Function<String, String>>> maskingFunctions = new HashMap<>();

        ReferenceAggregationTable maskingFunction(String attribute, String functionName, Function<String, String> function) {

            Map<String, Function<String, String>> attributeMap = maskingFunctions.computeIfAbsent(attribute, (k) -> new HashMap<>());
            attributeMap.put(functionName, function);

            return this;
        }

        void add(Collection<Map<String, ?>> documents) {
            for (Map<String, ?> document : documents) {
                add(document);
            }
        }

        void add(Map<String, ?> document) {

            for (Map.Entry<String, ?> entry : document.entrySet()) {
                Map<String, Integer> countMap = aggregatedAttributeCounts.computeIfAbsent(entry.getKey(), (k) -> new HashMap<>());

                countMap.compute(String.valueOf(entry.getValue()), (k, v) -> v == null ? 1 : v + 1);

                addMaskedValues(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        int getCount(String attribute, String value) {
            Map<String, Integer> valueMap = aggregatedAttributeCounts.get(attribute);

            if (valueMap == null) {
                throw new IllegalArgumentException("Unknown attribute " + attribute + "; available: " + aggregatedAttributeCounts.keySet());
            }

            Integer count = valueMap.get(value);

            if (count != null) {
                return count;
            } else {
                return 0;
            }
        }

        private void addMaskedValues(String key, String value) {
            Map<String, Function<String, String>> maskingFunctionsForAttribute = maskingFunctions.get(key);

            if (maskingFunctionsForAttribute != null) {
                for (Map.Entry<String, Function<String, String>> entry : maskingFunctionsForAttribute.entrySet()) {
                    Map<String, Integer> countMap = aggregatedAttributeCounts.computeIfAbsent(key + ":" + entry.getKey(), (k) -> new HashMap<>());

                    countMap.compute(entry.getValue().apply(value), (k, v) -> v == null ? 1 : v + 1);
                }
            }
        }

    }

    static class Masks {
        static String blake2bHash(String in) {
            return new String(blake2bHash(in.getBytes()));
        }

        static byte[] blake2bHash(byte[] in) {
            final Blake2bDigest hash = new Blake2bDigest(null, 32, null, salt);
            hash.update(in, 0, in.length);
            final byte[] out = new byte[hash.getDigestSize()];
            hash.doFinal(out, 0);

            return Hex.encode(out);
        }

        static String regexReplace(String in, Pattern pattern, String replacement) {
            Matcher matcher = pattern.matcher(in);

            return matcher.replaceAll(replacement);
        }
    }
}
