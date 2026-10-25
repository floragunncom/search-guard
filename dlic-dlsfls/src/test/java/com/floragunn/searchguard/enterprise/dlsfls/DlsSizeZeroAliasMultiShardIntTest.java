/*
 * Copyright 2026 by floragunn GmbH - All rights reserved
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

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Multi shard variant of DlsSizeZeroAliasIntTest. It exists to cover the batched query phase, which Elasticsearch 9.5 enables
 * by default (search.batched_query_phase) and which the other DLS size=0 tests do not reach:
 *
 * <ul>
 * <li>SearchQueryThenFetchAsyncAction only sends a NodeQueryRequest (transport action
 * indices:data/read/search[query][n]) to a data node which is not the coordinating node and which holds at least two shards
 * of the search. Shards of the coordinating node are always executed individually, and a remote node with a single shard
 * falls back to an individual shard request as well.</li>
 * <li>DlsSizeZeroAliasIntTest uses one shard per index, so whether batched execution happens at all depends on how three
 * shards are distributed over two data nodes. DlsRequestCachePoisoningIntTest runs on a single node, where every shard is
 * local and batched execution never happens.</li>
 * </ul>
 *
 * This test therefore uses the default cluster (one master and two data nodes) with five shards per index, which guarantees
 * that the non-coordinating data node holds several shards of every search. setupTestData() asserts that precondition.
 *
 * The scenarios are the ones which matter on that path: size=0 totals and terms aggregations for an unrestricted user, a
 * user with DLS and a user whose access is granted through an alias with a write index, each also in the order which used to
 * poison the shard request cache. All of these are expected to pass.
 */
public class DlsSizeZeroAliasMultiShardIntTest {

    static final String ALIAS = "ms";
    static final String PATTERN = "ms-*";
    static final String[] INDICES = new String[] { "ms-000001", "ms-000002", "ms-000003" };
    static final String[] LEVELS = new String[] { "info", "warn", "error", "info", "debug" };
    static final int SHARDS_PER_INDEX = 5;
    static final long TOTAL_DOCS = 15;
    static final long INFO_DOCS = 6;
    static final int DEFAULT_SIZE = 10;

    static final Map<String, Long> ALL_LEVELS = levelCounts(6, 3, 3, 3);
    static final Map<String, Long> ONLY_INFO = levelCounts(6, 0, 0, 0);

    static final DocNode DLS_INFO_ONLY = DocNode.of("term.level.value", "info");
    static final String TERMS_AGGREGATION = "{\"aggs\":{\"levels\":{\"terms\":{\"field\":\"level\",\"size\":10}}}}";

    /** Mirrors the admin:admin basic auth user of the issue report. Not an admin cert user. */
    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin").password("admin")
            .roles(new Role("all_access").clusterPermissions("*").indexPermissions("*").on("*").aliasPermissions("*").on("*"));

    /** Control: admin certificate, bypasses all authorization code */
    static final TestSgConfig.User ADMIN_CERT = new TestSgConfig.User("admin_cert").adminCertUser();

    /** Non-admin without DLS */
    static final TestSgConfig.User READ_USER = new TestSgConfig.User("read_user")
            .roles(new Role("read_ms").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_READ").on(PATTERN));

    /** Non-admin with DLS: sees only level=info, i.e. 2 docs per index */
    static final TestSgConfig.User DLS_USER = new TestSgConfig.User("dls_user").roles(
            new Role("dls_ms").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_READ").dls(DLS_INFO_ONLY).on(PATTERN));

    /** Non-admin whose access is granted only via the alias, which has a write index. This is the constellation of the issue report. */
    static final TestSgConfig.User ALIAS_USER = new TestSgConfig.User("alias_user")
            .roles(new Role("alias_ms").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").aliasPermissions("SGS_READ").on(ALIAS));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().metrics("detailed");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, ADMIN_CERT, READ_USER, DLS_USER, ALIAS_USER).resources("dlsfls").build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            for (int indexNo = 0; indexNo < INDICES.length; indexNo++) {
                boolean writeIndex = indexNo == INDICES.length - 1;
                HttpResponse response = client.putJson("/" + INDICES[indexNo],
                        DocNode.of("settings.index.number_of_shards", SHARDS_PER_INDEX, "settings.index.number_of_replicas", 0,
                                "mappings.properties.level.type", "keyword", "mappings.properties.message.type", "text",
                                "mappings.properties.ts.type", "date", "aliases." + ALIAS + ".is_write_index", writeIndex));
                assertThat(response, isOk());
            }

            List<DocNode> bulk = new ArrayList<>();

            for (int indexNo = 0; indexNo < INDICES.length; indexNo++) {
                for (int docNo = 0; docNo < LEVELS.length; docNo++) {
                    bulk.add(DocNode.of("index._index", INDICES[indexNo], "index._id", INDICES[indexNo] + "-" + (docNo + 1)));
                    bulk.add(DocNode.of("message", "log " + (indexNo + 1) + "-" + (docNo + 1), "level", LEVELS[docNo], "ts",
                            "2024-01-0" + (indexNo + 1) + "T0" + docNo + ":00:00Z"));
                }
            }

            HttpResponse response = client.putNdJson("/_bulk?refresh=true", bulk.toArray(new DocNode[0]));
            assertThat(response, isOk());
            assertFalse(response.getBody(), response.getBodyAsDocNode().getBoolean("errors"));

            response = client.get("/" + PATTERN + "/_count");
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode().getNumber("count").longValue(), is(TOTAL_DOCS));

            // Fail fast if the cluster topology does not give us batched query execution; the tests would then silently
            // cover less than they are meant to.
            assertBatchedQueryPhasePrecondition(client);
        }
    }

    @Before
    public void clearRequestCache() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.post("/" + PATTERN + "/_cache/clear?request=true");
            assertThat(response, isOk());
        }
    }

    /**
     * Documents the precondition of this test class: the batched query phase is enabled and the shards are distributed such
     * that at least one data node holds several shards of a search on the alias.
     */
    @Test
    public void batchedQueryPhase_precondition() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            assertBatchedQueryPhasePrecondition(client);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The sequence of the issue report, on the batched query path
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void aliasUser_then_admin_alias_size0() throws Exception {
        assertSearch(ALIAS_USER, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void aliasUser_then_admin_alias_size0_adminOnly() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ALIAS_USER)) {
            assertThat(client.get("/" + ALIAS + "/_search?size=0"), isOk());
        }

        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
        assertSearch(ADMIN, PATTERN, "?size=0", TOTAL_DOCS, 0, null);
        assertSearch(ADMIN, ALIAS, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void dlsUser_then_admin_alias_size0() throws Exception {
        // Control: a detected DLS restriction disables the request cache for the request
        assertSearch(DLS_USER, ALIAS, "?size=0", INFO_DOCS, 0, null);
        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void readUser_then_admin_alias_size0() throws Exception {
        // Control: a user granted via index patterns
        assertSearch(READ_USER, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    /**
     * With an explicit request_cache=true, Elasticsearch also caches size &gt; 0 requests. Both requests must use the same flag
     * value, as it is part of the cache key.
     */
    @Test
    public void aliasUser_then_admin_alias_requestCacheTrue_sizeGreaterThanZero() throws Exception {
        assertSearch(ALIAS_USER, ALIAS, "?size=5&request_cache=true", TOTAL_DOCS, 5, null);
        assertSearch(ADMIN, ALIAS, "?size=5&request_cache=true", TOTAL_DOCS, 5, null);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Totals per user and target
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void admin_alias_size0() throws Exception {
        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void admin_alias_size0_trackTotalHitsTrue() throws Exception {
        assertSearch(ADMIN, ALIAS, "?size=0&track_total_hits=true", TOTAL_DOCS, 0, null);
    }

    @Test
    public void admin_alias_sizeGreaterThanZero() throws Exception {
        assertSearch(ADMIN, ALIAS, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(ADMIN, ALIAS, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void admin_pattern_size0() throws Exception {
        assertSearch(ADMIN, PATTERN, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void admin_count() throws Exception {
        assertCount(ADMIN, ALIAS, TOTAL_DOCS);
        assertCount(ADMIN, PATTERN, TOTAL_DOCS);
    }

    @Test
    public void adminCert_alias_size0() throws Exception {
        assertSearch(ADMIN_CERT, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void adminCert_alias_sizeGreaterThanZero() throws Exception {
        assertSearch(ADMIN_CERT, ALIAS, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(ADMIN_CERT, ALIAS, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void readUser_alias_size0() throws Exception {
        assertSearch(READ_USER, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void readUser_alias_sizeGreaterThanZero() throws Exception {
        assertSearch(READ_USER, ALIAS, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(READ_USER, ALIAS, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void readUser_count() throws Exception {
        assertCount(READ_USER, ALIAS, TOTAL_DOCS);
        assertCount(READ_USER, PATTERN, TOTAL_DOCS);
    }

    @Test
    public void dlsUser_alias_size0() throws Exception {
        assertSearch(DLS_USER, ALIAS, "?size=0", INFO_DOCS, 0, null);
    }

    @Test
    public void dlsUser_alias_size0_trackTotalHitsTrue() throws Exception {
        assertSearch(DLS_USER, ALIAS, "?size=0&track_total_hits=true", INFO_DOCS, 0, null);
    }

    @Test
    public void dlsUser_alias_sizeGreaterThanZero() throws Exception {
        assertSearch(DLS_USER, ALIAS, "", INFO_DOCS, (int) INFO_DOCS, "info");
        assertSearch(DLS_USER, ALIAS, "?size=100", INFO_DOCS, (int) INFO_DOCS, "info");
    }

    @Test
    public void dlsUser_pattern_size0() throws Exception {
        assertSearch(DLS_USER, PATTERN, "?size=0", INFO_DOCS, 0, null);
    }

    @Test
    public void dlsUser_pattern_sizeGreaterThanZero() throws Exception {
        assertSearch(DLS_USER, PATTERN, "", INFO_DOCS, (int) INFO_DOCS, "info");
        assertSearch(DLS_USER, PATTERN, "?size=100", INFO_DOCS, (int) INFO_DOCS, "info");
    }

    @Test
    public void dlsUser_count() throws Exception {
        assertCount(DLS_USER, ALIAS, INFO_DOCS);
        assertCount(DLS_USER, PATTERN, INFO_DOCS);
    }

    @Test
    public void aliasUser_alias_size0() throws Exception {
        assertSearch(ALIAS_USER, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void aliasUser_alias_size0_trackTotalHitsTrue() throws Exception {
        assertSearch(ALIAS_USER, ALIAS, "?size=0&track_total_hits=true", TOTAL_DOCS, 0, null);
    }

    @Test
    public void aliasUser_alias_sizeGreaterThanZero() throws Exception {
        assertSearch(ALIAS_USER, ALIAS, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(ALIAS_USER, ALIAS, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void aliasUser_count() throws Exception {
        assertCount(ALIAS_USER, ALIAS, TOTAL_DOCS);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Aggregations: the usual reason for a size=0 search, and the part of the response which is cached
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void admin_alias_size0_aggregation() throws Exception {
        assertAggregation(ADMIN, ALIAS, TOTAL_DOCS, ALL_LEVELS);
    }

    @Test
    public void readUser_alias_size0_aggregation() throws Exception {
        assertAggregation(READ_USER, ALIAS, TOTAL_DOCS, ALL_LEVELS);
    }

    @Test
    public void dlsUser_alias_size0_aggregation() throws Exception {
        assertAggregation(DLS_USER, ALIAS, INFO_DOCS, ONLY_INFO);
    }

    @Test
    public void aliasUser_alias_size0_aggregation() throws Exception {
        assertAggregation(ALIAS_USER, ALIAS, TOTAL_DOCS, ALL_LEVELS);
    }

    @Test
    public void aliasUser_then_admin_alias_size0_aggregation() throws Exception {
        assertAggregation(ALIAS_USER, ALIAS, TOTAL_DOCS, ALL_LEVELS);
        assertAggregation(ADMIN, ALIAS, TOTAL_DOCS, ALL_LEVELS);
    }

    @Test
    public void dlsUser_then_admin_alias_size0_aggregation() throws Exception {
        assertAggregation(DLS_USER, ALIAS, INFO_DOCS, ONLY_INFO);
        assertAggregation(ADMIN, ALIAS, TOTAL_DOCS, ALL_LEVELS);
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static void assertSearch(TestSgConfig.User user, String target, String queryString, long expectedTotal, int expectedHits,
            String expectedLevelOrNull) throws Exception {
        String path = "/" + target + "/_search" + queryString;

        try (GenericRestClient client = cluster.getRestClient(user)) {
            HttpResponse response = client.get(path);
            String context = "[" + user.getName() + "] GET " + path + " -> " + response.getBody();

            assertThat(context, response, isOk());

            DocNode body = response.getBodyAsDocNode();
            DocNode total = body.getAsNode("hits").getAsNode("total");

            assertEquals(context, expectedTotal, total.getNumber("value").longValue());
            assertEquals(context, "eq", total.getAsString("relation"));

            List<DocNode> hits = body.getAsNode("hits").getAsListOfNodes("hits");
            assertEquals(context, expectedHits, hits.size());

            if (expectedLevelOrNull != null) {
                for (DocNode hit : hits) {
                    assertEquals(context, expectedLevelOrNull, hit.getAsNode("_source").getAsString("level"));
                }
            }
        }
    }

    private static void assertCount(TestSgConfig.User user, String target, long expected) throws Exception {
        String path = "/" + target + "/_count";

        try (GenericRestClient client = cluster.getRestClient(user)) {
            HttpResponse response = client.get(path);
            String context = "[" + user.getName() + "] GET " + path + " -> " + response.getBody();

            assertThat(context, response, isOk());
            assertEquals(context, expected, response.getBodyAsDocNode().getNumber("count").longValue());
        }
    }

    /**
     * Runs a size=0 search with a terms aggregation on the level field and compares the buckets. Levels with an expected count
     * of 0 must not appear as a bucket at all.
     */
    private static void assertAggregation(TestSgConfig.User user, String target, long expectedTotal, Map<String, Long> expectedBuckets)
            throws Exception {
        String path = "/" + target + "/_search?size=0";

        try (GenericRestClient client = cluster.getRestClient(user)) {
            HttpResponse response = client.postJson(path, TERMS_AGGREGATION);
            String context = "[" + user.getName() + "] POST " + path + " " + TERMS_AGGREGATION + " -> " + response.getBody();

            assertThat(context, response, isOk());

            DocNode body = response.getBodyAsDocNode();
            assertEquals(context, expectedTotal, body.getAsNode("hits").getAsNode("total").getNumber("value").longValue());

            Map<String, Long> actualBuckets = new LinkedHashMap<>();

            for (DocNode bucket : body.getAsNode("aggregations").getAsNode("levels").getAsListOfNodes("buckets")) {
                actualBuckets.put(bucket.getAsString("key"), bucket.getNumber("doc_count").longValue());
            }

            Map<String, Long> expectedNonEmptyBuckets = new LinkedHashMap<>();
            expectedBuckets.forEach((level, count) -> {
                if (count.longValue() != 0) {
                    expectedNonEmptyBuckets.put(level, count);
                }
            });

            assertEquals(context, expectedNonEmptyBuckets, actualBuckets);
        }
    }

    /**
     * Asserts that the batched query phase is enabled and that the shards of this test's indices are distributed over several
     * nodes with several shards per node. Only then Elasticsearch sends a NodeQueryRequest instead of individual shard
     * requests.
     */
    private static void assertBatchedQueryPhasePrecondition(GenericRestClient adminCertClient) throws Exception {
        HttpResponse response = adminCertClient
                .get("/_cluster/settings?include_defaults=true&flat_settings=true&filter_path=**.search.batched_query_phase");
        assertThat(response, isOk());
        assertFalse("search.batched_query_phase must not be disabled, otherwise this test does not cover the batched query path: "
                + response.getBody(), response.getBody().contains("false"));

        response = adminCertClient.get("/_cat/shards/" + PATTERN + "?h=index,shard,node");
        assertThat(response, isOk());

        Map<String, Integer> shardsPerNode = new HashMap<>();

        for (String line : response.getBody().split("\n")) {
            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                continue;
            }

            // index shard node; the node name is the remainder after the first two columns
            String[] columns = trimmed.split("\\s+", 3);
            assertEquals("Unexpected _cat/shards line: " + trimmed, 3, columns.length);
            shardsPerNode.merge(columns[2], 1, Integer::sum);
        }

        int maxShardsPerNode = shardsPerNode.values().stream().mapToInt(Integer::intValue).max().orElse(0);

        assertEquals("Expected " + (INDICES.length * SHARDS_PER_INDEX) + " shards in total: " + shardsPerNode,
                INDICES.length * SHARDS_PER_INDEX, shardsPerNode.values().stream().mapToInt(Integer::intValue).sum());
        assertTrue("The shards must be distributed over at least two nodes, otherwise every shard is local to the coordinating "
                + "node and Elasticsearch never uses batched query execution: " + shardsPerNode, shardsPerNode.size() >= 2);
        assertTrue("At least one node must hold at least two shards, otherwise Elasticsearch falls back to individual shard "
                + "requests: " + shardsPerNode, maxShardsPerNode >= 2);
    }

    private static Map<String, Long> levelCounts(long info, long warn, long error, long debug) {
        Map<String, Long> result = new LinkedHashMap<>();
        result.put("info", info);
        result.put("warn", warn);
        result.put("error", error);
        result.put("debug", debug);
        return result;
    }
}
