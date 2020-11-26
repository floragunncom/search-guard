package com.floragunn.searchguard.dlic.dlsfls;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.util.encoders.Hex;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.google.common.collect.ImmutableMap;

public class FieldMaskingAggregationTest {

    /**
     * Increase DOC_COUNT for manual test runs with bigger test data sets
     */
    private static final int DOC_COUNT = 1000;
    private static final int DELETED_DOC_COUNT = (int) (DOC_COUNT * 0.06);
    private static final int SEGMENT_COUNT = 17;
    private static final int REFRESH_AFTER = DOC_COUNT / SEGMENT_COUNT;
    private static final int SEED = 1234;

    private final static TestSgConfig.User MASKED_TEST_USER = new TestSgConfig.User("masked_test")
            .roles(new Role("mask").indexPermissions("*").maskedFields("*ip::/[0-9]{1,3}$/::XXX", "source_loc").on("ip").clusterPermissions("*"));

    private final static TestSgConfig.User UNMASKED_TEST_USER = new TestSgConfig.User("unmasked_test")
            .roles(new Role("allaccess").indexPermissions("*").on("ip").clusterPermissions("*"));

    private final static byte[] salt = ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT_DEFAULT.getBytes(StandardCharsets.UTF_8);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().users(MASKED_TEST_USER, UNMASKED_TEST_USER).build();

    private static Random random = new Random(SEED);
    private static String[] testDataIpAddresses = createRandomIpAddresses();
    private static String[] testDataLocationNames = createRandomLocationNames();

    /**
     * This table also aggregates the test data and serves as reference for the tests
     */
    private static ReferenceAggregationTable referenceAggregationTable = new ReferenceAggregationTable()//
            .maskingFunction("source_loc", "hash", Masks::blake2bHash)//
            .maskingFunction("source_ip", "masked", (v) -> Masks.regexReplace(v, Pattern.compile("[0-9]{1,3}$"), "XXX"));

    private static final Logger log = LogManager.getLogger(FieldMaskingAggregationTest.class);

    @BeforeClass
    public static void setupTestData() {

        log.info("Creating test data");
        createTestIndex("ip", DOC_COUNT, DELETED_DOC_COUNT, REFRESH_AFTER, Settings.builder().put("index.number_of_shards", 5).build());
        log.info("Creating test data finished");

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
                .aggregation(AggregationBuilders.terms("source_loc_terms").field("source_loc.keyword").size(100)));
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

    /**
     * Creates an index with test data. The index will consist of segments of roughly the size specified by the refreshAfter parameter. 
     * The actual segment size will be however randomized, it is possible to have segments with only one document and segments with twice the specified size.
     * The function will also randomly delete documents after they were created. The amount of deleted documents is specified by the deletedDocumentCount parameter.
     * 
     * The index will contain these attributes:
     * - source_ip, dest_ip: Strings with random IP addresses with equal distribution
     * - source_loc, dest_loc: Strings with random location names with Gaussian distribution
     */
    private static void createTestIndex(String name, int size, int deletedDocumentCount, int refreshAfter, Settings settings) {
        log.info("creating test index " + name + "; size: " + size + "; deletedDocumentCount: " + deletedDocumentCount + "; refreshAfter: "
                + refreshAfter);

        try (Client client = cluster.getInternalClient()) {

            Map<String, Map<String, ?>> createdDocuments = new HashMap<>(size);

            client.admin().indices().create(new CreateIndexRequest(name).settings(settings)).actionGet();

            int nextRefresh = (int) Math.floor((random.nextGaussian() * 0.5 + 0.5) * refreshAfter);

            for (int i = 0; i < size; i++) {
                Map<String, ?> document = ImmutableMap.of("source_ip", randomIpAddress(), "dest_ip", randomIpAddress(), "source_loc",
                        randomLocationName(), "dest_loc", randomLocationName());

                IndexResponse indexResponse = client.index(new IndexRequest(name).source(document, XContentType.JSON)).actionGet();

                createdDocuments.put(indexResponse.getId(), document);

                if (i > nextRefresh) {
                    client.admin().indices().refresh(new RefreshRequest(name)).actionGet();
                    double g = random.nextGaussian();

                    nextRefresh = (int) Math.floor((g * 0.5 + 1) * refreshAfter) + i + 1;
                    log.debug("refresh at " + i + " " + g + " " + (g * 0.5 + 1));
                }
            }

            client.admin().indices().refresh(new RefreshRequest(name)).actionGet();

            List<String> createdDocIds = new ArrayList<>(createdDocuments.keySet());

            Collections.shuffle(createdDocIds, random);

            for (int i = 0; i < deletedDocumentCount; i++) {
                String key = createdDocIds.get(i);
                client.delete(new DeleteRequest(name, key)).actionGet();
                createdDocuments.remove(key);
            }

            client.admin().indices().refresh(new RefreshRequest(name)).actionGet();

            referenceAggregationTable.add(createdDocuments.values());
        }
    }

    private static String[] createRandomIpAddresses() {
        String[] result = new String[2000];

        for (int i = 0; i < result.length; i++) {
            result[i] = (random.nextInt(10) + 100) + "." + (random.nextInt(5) + 100) + "." + random.nextInt(255) + "." + random.nextInt(255);
        }

        return result;
    }

    private static String[] createRandomLocationNames() {
        String[] p1 = new String[] { "Schön", "Schöner", "Tempel", "Friedens", "Friedrichs", "Blanken", "Rosen", "Charlotten", "Malch", "Lichten",
                "Lichter", "Hasel", "Kreuz", "Pank", "Marien", "Adlers", "Zehlen", "Haken", "Witten", "Jungfern", "Hellers", "Finster", "Birken",
                "Falken", "Freders", "Karls", "Grün", "Wilmers", "Heiners", "Lieben", "Marien", "Wiesen", "Biesen", "Schmachten", "Rahns", "Rangs",
                "Herms", "Rüders", "Wuster", "Hoppe" };
        String[] p2 = new String[] { "au", "ow", "berg", "feld", "felde", "tal", "thal", "höhe", "burg", "horst", "hausen", "dorf", "hof", "heide",
                "weide", "hain", "walde", "linde", "hagen", "eiche", "witz", "rade", "werder", "see", "fließ", "krug", "mark" };

        ArrayList<String> result = new ArrayList<>(p1.length * p2.length);

        for (int i = 0; i < p1.length; i++) {
            for (int k = 0; k < p2.length; k++) {
                result.add(p1[i] + p2[k]);
            }
        }

        Collections.shuffle(result, random);

        return result.toArray(new String[result.size()]);
    }

    private static String randomIpAddress() {
        return testDataIpAddresses[random.nextInt(testDataIpAddresses.length)];
    }

    private static String randomLocationName() {
        int i = (int) Math.floor(random.nextGaussian() * testDataLocationNames.length * 0.333 + testDataLocationNames.length);

        if (i < 0 || i >= testDataLocationNames.length) {
            i = random.nextInt(testDataLocationNames.length);
        }

        return testDataLocationNames[i];
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
