package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.util.NamedValue;
import com.floragunn.searchguard.client.RestHighLevelClient;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.util.encoders.Hex;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

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

public class FieldMaskingAggregationTest {

    /**
     * Increase DOC_COUNT for manual test runs with bigger test data sets
     */
    private static final int DOC_COUNT = 1000;

    private final static TestSgConfig.User MASKED_TEST_USER = new TestSgConfig.User("masked_test")
            .roles(new TestSgConfig.Role("mask").indexPermissions("*").maskedFields("*ip::/[0-9]{1,3}$/::XXX", "source_loc").on("ip").clusterPermissions("*"));

    private final static TestSgConfig.User UNMASKED_TEST_USER = new TestSgConfig.User("unmasked_test")
            .roles(new TestSgConfig.Role("allaccess").indexPermissions("*").on("ip").clusterPermissions("*"));

    private final static byte[] salt = ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT_DEFAULT.getBytes(StandardCharsets.UTF_8);

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled()
            .users(MASKED_TEST_USER, UNMASKED_TEST_USER).resources("dlsfls_legacy").build();

    /**
     * This table also aggregates the test data and serves as reference for the tests
     */
    private static ReferenceAggregationTable referenceAggregationTable = new ReferenceAggregationTable()//
            .maskingFunction("source_loc", "hash", Masks::blake2bHash)//
            .maskingFunction("source_ip", "masked", (v) -> Masks.regexReplace(v, Pattern.compile("[0-9]{1,3}$"), "XXX"));

    private static final Logger log = LogManager.getLogger(FieldMaskingAggregationTest.class);

    @BeforeClass
    public static void setupTestData() {
        Client client = cluster.getInternalNodeClient();
        TestData testData = TestData.documentCount(DOC_COUNT).get();
        testData.createIndex(client, "ip", Settings.builder().put("index.number_of_shards", 5).build());
        referenceAggregationTable.add(testData.getRetainedDocuments().values());
    }

    @Test
    public void testPartiallyMaskedField() throws Exception {
        // we need to set shardSize to DOC_COUNT in order to get precise results which allow matching on the reference table

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(MASKED_TEST_USER)) {

            co.elastic.clients.elasticsearch.core.SearchResponse<Map> maskedSearchResponse = client.getJavaClient().search(s->s.size(10).index("ip").aggregations("source_ip_terms", a->a.terms(t->t.field("source_ip.keyword").size(100).shardSize(DOC_COUNT))), Map.class);


            /*SearchResponse maskedSearchResponse = client.search(
                    new SearchRequest("ip").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10)
                            .aggregation(AggregationBuilders.terms("source_ip_terms").field("source_ip.keyword").size(100).shardSize(DOC_COUNT))),
                    RequestOptions.DEFAULT);*/

            //log.info(Strings.toString(maskedSearchResponse, true, true));

            StringTermsAggregate maskedAggregation = maskedSearchResponse.aggregations().get("source_ip_terms").sterms();

            Assert.assertEquals(100, maskedAggregation.buckets().array().size());

            for (StringTermsBucket maskedBucket: maskedAggregation.buckets().array()) {
                Assert.assertEquals("Bucket " + maskedBucket.key() + ":\n" + maskedBucket.aggregations(),
                        referenceAggregationTable.getCount("source_ip:masked", maskedBucket.key().stringValue()), maskedBucket.docCount());
            }
        }
    }

    @Test
    public void testHashMaskedField() throws Exception {

        co.elastic.clients.elasticsearch.core.SearchRequest searchRequest = new co.elastic.clients.elasticsearch.core.SearchRequest.Builder()
                .index("ip")
                //.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10)
                .aggregations("source_loc_terms", a->a.terms(ta->ta.field("source_loc.keyword").size(1000))).build();

        co.elastic.clients.elasticsearch.core.SearchResponse<Map> maskedSearchResponse;
        co.elastic.clients.elasticsearch.core.SearchResponse<Map> unmaskedSearchResponse;

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(UNMASKED_TEST_USER)) {
            unmaskedSearchResponse = client.getJavaClient().search(searchRequest, Map.class);

            //log.info(Strings.toString(unmaskedSearchResponse, true, true));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(MASKED_TEST_USER)) {
            maskedSearchResponse = client.getJavaClient().search(searchRequest, Map.class);

            //log.info(Strings.toString(maskedSearchResponse, true, true));
        }

        compareHashedBuckets(maskedSearchResponse, unmaskedSearchResponse, "source_loc_terms");
    }

    @Test
    public void testHashMaskedFieldWithShardSizeParam() throws Exception {

        co.elastic.clients.elasticsearch.core.SearchRequest searchRequest = new co.elastic.clients.elasticsearch.core.SearchRequest.Builder()
                .index("ip")
                //.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10)
                .aggregations("source_loc_terms", a->a.terms(ta->ta.field("source_loc.keyword").size(100).shardSize(1000))).build();

        co.elastic.clients.elasticsearch.core.SearchResponse<Map> maskedSearchResponse;
        co.elastic.clients.elasticsearch.core.SearchResponse<Map> unmaskedSearchResponse;

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(UNMASKED_TEST_USER)) {
            unmaskedSearchResponse = client.getJavaClient().search(searchRequest, Map.class);

            //log.info(Strings.toString(unmaskedSearchResponse, true, true));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(MASKED_TEST_USER)) {
            maskedSearchResponse = client.getJavaClient().search(searchRequest, Map.class);

            //log.info(Strings.toString(maskedSearchResponse, true, true));
        }

        compareHashedBuckets(maskedSearchResponse, unmaskedSearchResponse, "source_loc_terms");
    }

    @Test
    public void testHashMaskedFieldOrderedByKey() throws Exception {
        // we need to set shardSize to DOC_COUNT in order to get precise results which allow matching on the reference table

        co.elastic.clients.elasticsearch.core.SearchRequest searchRequest = new co.elastic.clients.elasticsearch.core.SearchRequest.Builder()
                .index("ip")
                .size(10)
                .aggregations("source_loc_terms", a->a
                        .terms(ta->ta
                            .field("source_loc.keyword")
                            .order(Lists.newArrayList(new NamedValue<SortOrder>("_key", SortOrder.Asc)))
                            .size(100)
                            .shardSize(DOC_COUNT)
                            //.showTermDocCountError(true) //TODO with this test fails with "Make sure the request has 'typed_keys' set"
                        )
                )
                .build();

        co.elastic.clients.elasticsearch.core.SearchResponse<Map> maskedSearchResponse;
        co.elastic.clients.elasticsearch.core.SearchResponse<Map> unmaskedSearchResponse;

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(UNMASKED_TEST_USER)) {
            //TODO why unmaskedSearchResponse is not further used here?
            unmaskedSearchResponse = client.getJavaClient().search(searchRequest, Map.class);

            //log.info(Strings.toString(unmaskedSearchResponse, true, true));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(MASKED_TEST_USER)) {
            maskedSearchResponse = client.getJavaClient().search(searchRequest, Map.class);

            //log.info(Strings.toString(maskedSearchResponse, true, true));
        }

        StringTermsAggregate maskedAggregation = maskedSearchResponse.aggregations().get("source_loc_terms").sterms();

        for (int i = 0; i < maskedAggregation.buckets().array().size(); i++) {
            StringTermsBucket maskedBucket = maskedAggregation.buckets().array().get(i);

            Assert.assertEquals("Bucket " + i + ":\n" + (maskedBucket),
                    referenceAggregationTable.getCount("source_loc:hash", maskedBucket.key().stringValue()), maskedBucket.docCount());
        }

        //TODO we do nothing with unmaskedSearchResponse here

    }

    private void compareHashedBuckets(co.elastic.clients.elasticsearch.core.SearchResponse<Map> maskedSearchResponse, co.elastic.clients.elasticsearch.core.SearchResponse<Map> unmaskedSearchResponse, String key) {
        // Assume hashing does not map different location strings to one hash

        StringTermsAggregate maskedAggregation = maskedSearchResponse.aggregations().get(key).sterms();
        StringTermsAggregate unmaskedAggregation = unmaskedSearchResponse.aggregations().get(key).sterms();

        Assert.assertEquals(unmaskedAggregation.buckets().array().size(), maskedAggregation.buckets().array().size());

        // As terms with equal count may change their order between masked and unmasked states, we have to collect them before comparing
        Set<String> groupedUnmaskedTermsByCount = new HashSet<>();
        Set<String> groupedMaskedTermsByCount = new HashSet<>();
        StringTermsBucket prevUnmaskedBucket = null;
        int groupStart = 0;

        for (int i = 0; i < unmaskedAggregation.buckets().array().size(); i++) {

            StringTermsBucket unmaskedBucket = unmaskedAggregation.buckets().array().get(i);
            StringTermsBucket maskedBucket = maskedAggregation.buckets().array().get(i);

            if (prevUnmaskedBucket != null && prevUnmaskedBucket.docCount() != unmaskedBucket.docCount()) {
                Assert.assertEquals(
                        "Buckets at " + groupStart + " to " + (i - 1) + ":\n" + (unmaskedBucket) + "\n" + (maskedBucket),
                        groupedUnmaskedTermsByCount, groupedMaskedTermsByCount);

                groupedUnmaskedTermsByCount.clear();
                groupedMaskedTermsByCount.clear();
                groupStart = 1;
            }

            Assert.assertEquals("Bucket " + i + ":\n" + (unmaskedBucket) + "\n" + (maskedBucket), unmaskedBucket.docCount(),
                    unmaskedBucket.docCount());

            groupedUnmaskedTermsByCount.add(Masks.blake2bHash(unmaskedBucket.key().stringValue()));
            groupedMaskedTermsByCount.add(maskedBucket.key().stringValue());

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
