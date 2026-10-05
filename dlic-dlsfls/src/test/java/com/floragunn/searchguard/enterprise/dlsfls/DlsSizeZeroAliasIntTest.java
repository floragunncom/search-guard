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

import java.util.ArrayList;
import java.util.List;

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
 * Literal reproducer of the sequence from the issue report "GET /new/_search?size=0 returns 5 instead of 15": three indices
 * created together with their alias (one of them as write index), five documents each, searched with size=0 by the basic
 * auth admin user. Extended by non-admin users (with and without DLS), by searches without the alias and by size unset /
 * size > 0 for every combination.
 *
 * Each test starts with a cleared shard request cache, so the tests do not influence each other via cached size=0 results.
 * The literal sequence of the issue report (a user whose access is granted via the alias searches with size=0, then admin
 * searches with size=0) is aliasUser_then_admin_alias_size0.
 *
 * Before the fix, all aliasUser_* tests failed (the write index of the alias was not visible to the alias grant on the shard
 * level, see DlsStaleAliasSnapshotInvariantTest) as well as aliasUser_then_admin_alias_size0 (the alias user's size=0 search
 * wrote the restricted result of the write index shard into the shard request cache, admin read it). The mechanism is covered
 * in detail by DlsRequestCachePoisoningIntTest.
 */
public class DlsSizeZeroAliasIntTest {

    static final String ALIAS = "new";
    static final String PATTERN = "old-*";
    static final String EXPLICIT_LIST = "old-000001,old-000002,old-000003";
    static final String[] INDICES = new String[] { "old-000001", "old-000002", "old-000003" };
    static final String[] LEVELS = new String[] { "info", "warn", "error", "info", "debug" };
    static final long TOTAL_DOCS = 15;
    static final long INFO_DOCS = 6;
    static final int DEFAULT_SIZE = 10;

    static final DocNode DLS_INFO_ONLY = DocNode.of("term.level.value", "info");

    /** Mirrors the admin:admin basic auth user of the issue report. Not an admin cert user. */
    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin").password("admin")
            .roles(new Role("all_access").clusterPermissions("*").indexPermissions("*").on("*").aliasPermissions("*").on("*"));

    /** Control: admin certificate, bypasses all authorization code */
    static final TestSgConfig.User ADMIN_CERT = new TestSgConfig.User("admin_cert").adminCertUser();

    /** Non-admin without DLS. The index pattern covers all alias members, so searching the alias is allowed. */
    static final TestSgConfig.User READ_USER = new TestSgConfig.User("read_user")
            .roles(new Role("read_old").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_READ").on(PATTERN));

    /** Non-admin with DLS: sees only level=info, i.e. 2 docs per index */
    static final TestSgConfig.User DLS_USER = new TestSgConfig.User("dls_user").roles(
            new Role("dls_old").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_READ").dls(DLS_INFO_ONLY).on(PATTERN));

    /** Non-admin whose access is granted only via the alias. Can only search the alias. */
    static final TestSgConfig.User ALIAS_USER = new TestSgConfig.User("alias_user")
            .roles(new Role("alias_new").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").aliasPermissions("SGS_READ").on(ALIAS));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().metrics("detailed");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, ADMIN_CERT, READ_USER, DLS_USER, ALIAS_USER).resources("dlsfls").build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            // Exactly like the curl sequence of the issue: each index is created together with its alias membership.
            // Created via REST because TestAlias does not carry is_write_index on an external process cluster.
            for (int indexNo = 0; indexNo < INDICES.length; indexNo++) {
                boolean writeIndex = indexNo == INDICES.length - 1;
                HttpResponse response = client.putJson("/" + INDICES[indexNo],
                        DocNode.of("settings.index.number_of_shards", 1, "settings.index.number_of_replicas", 0,
                                "mappings.properties.level.type", "keyword", "mappings.properties.message.type", "text",
                                "mappings.properties.ts.type", "date", "aliases." + ALIAS + ".is_write_index", writeIndex));
                assertThat(response, isOk());
            }

            for (int indexNo = 0; indexNo < INDICES.length; indexNo++) {
                List<DocNode> bulk = new ArrayList<>();

                for (int docNo = 0; docNo < LEVELS.length; docNo++) {
                    bulk.add(DocNode.of("index", DocNode.EMPTY));
                    bulk.add(DocNode.of("message", "log " + (indexNo + 1) + "-" + (docNo + 1), "level", LEVELS[docNo], "ts",
                            "2024-01-0" + (indexNo + 1) + "T0" + docNo + ":00:00Z"));
                }

                HttpResponse response = client.putNdJson("/" + INDICES[indexNo] + "/_bulk?refresh=true", bulk.toArray(new DocNode[0]));
                assertThat(response, isOk());
                assertFalse(response.getBody(), response.getBodyAsDocNode().getBoolean("errors"));
            }

            HttpResponse response = client.get("/" + PATTERN + "/_count");
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode().getNumber("count").longValue(), is(TOTAL_DOCS));
        }
    }

    @Before
    public void clearRequestCache() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.post("/" + PATTERN + "/_cache/clear?request=true");
            assertThat(response, isOk());
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The literal sequence of the issue report
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void aliasUser_then_admin_alias_size0() throws Exception {
        assertSearch(ALIAS_USER, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void aliasUser_then_admin_alias_size0_adminOnly() throws Exception {
        // Like aliasUser_then_admin_alias_size0, but without asserting on the alias user's response. Shows the effect on admin alone.
        try (GenericRestClient client = cluster.getRestClient(ALIAS_USER)) {
            assertThat(client.get("/" + ALIAS + "/_search?size=0"), isOk());
        }

        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
        assertSearch(ADMIN, PATTERN, "?size=0", TOTAL_DOCS, 0, null);
        assertSearch(ADMIN, ALIAS, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void readUser_then_admin_alias_size0() throws Exception {
        // Control: a user granted via index patterns does not have the problem
        assertSearch(READ_USER, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void dlsUser_then_admin_alias_size0() throws Exception {
        // Control: a detected DLS restriction disables the request cache for the request
        assertSearch(DLS_USER, ALIAS, "?size=0", INFO_DOCS, 0, null);
        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // admin (basic auth, all access): the case from the issue report
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void admin_alias_size0() throws Exception {
        assertSearch(ADMIN, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void admin_alias_size0_postBody() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            HttpResponse response = client.postJson("/" + ALIAS + "/_search", "{\"size\":0}");
            assertThat(response, isOk());
            assertEquals("[admin] POST /new/_search {\"size\":0} -> " + response.getBody(), TOTAL_DOCS,
                    response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").getNumber("value").longValue());
        }
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
    public void admin_pattern_sizeGreaterThanZero() throws Exception {
        assertSearch(ADMIN, PATTERN, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(ADMIN, PATTERN, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void admin_explicitList_size0() throws Exception {
        assertSearch(ADMIN, EXPLICIT_LIST, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void admin_explicitList_sizeGreaterThanZero() throws Exception {
        assertSearch(ADMIN, EXPLICIT_LIST, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(ADMIN, EXPLICIT_LIST, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void admin_count() throws Exception {
        assertCount(ADMIN, ALIAS, TOTAL_DOCS);
        assertCount(ADMIN, PATTERN, TOTAL_DOCS);
        assertCount(ADMIN, EXPLICIT_LIST, TOTAL_DOCS);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // admin certificate: control, no authorization code involved
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void adminCert_alias_size0() throws Exception {
        assertSearch(ADMIN_CERT, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void adminCert_alias_size0_trackTotalHitsTrue() throws Exception {
        assertSearch(ADMIN_CERT, ALIAS, "?size=0&track_total_hits=true", TOTAL_DOCS, 0, null);
    }

    @Test
    public void adminCert_alias_sizeGreaterThanZero() throws Exception {
        assertSearch(ADMIN_CERT, ALIAS, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(ADMIN_CERT, ALIAS, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void adminCert_pattern_size0() throws Exception {
        assertSearch(ADMIN_CERT, PATTERN, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void adminCert_pattern_sizeGreaterThanZero() throws Exception {
        assertSearch(ADMIN_CERT, PATTERN, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(ADMIN_CERT, PATTERN, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void adminCert_explicitList_size0() throws Exception {
        assertSearch(ADMIN_CERT, EXPLICIT_LIST, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void adminCert_explicitList_sizeGreaterThanZero() throws Exception {
        assertSearch(ADMIN_CERT, EXPLICIT_LIST, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(ADMIN_CERT, EXPLICIT_LIST, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void adminCert_count() throws Exception {
        assertCount(ADMIN_CERT, ALIAS, TOTAL_DOCS);
        assertCount(ADMIN_CERT, PATTERN, TOTAL_DOCS);
        assertCount(ADMIN_CERT, EXPLICIT_LIST, TOTAL_DOCS);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // non-admin without DLS
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void readUser_alias_size0() throws Exception {
        assertSearch(READ_USER, ALIAS, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void readUser_alias_size0_trackTotalHitsTrue() throws Exception {
        assertSearch(READ_USER, ALIAS, "?size=0&track_total_hits=true", TOTAL_DOCS, 0, null);
    }

    @Test
    public void readUser_alias_sizeGreaterThanZero() throws Exception {
        assertSearch(READ_USER, ALIAS, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(READ_USER, ALIAS, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void readUser_pattern_size0() throws Exception {
        assertSearch(READ_USER, PATTERN, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void readUser_pattern_sizeGreaterThanZero() throws Exception {
        assertSearch(READ_USER, PATTERN, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(READ_USER, PATTERN, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void readUser_explicitList_size0() throws Exception {
        assertSearch(READ_USER, EXPLICIT_LIST, "?size=0", TOTAL_DOCS, 0, null);
    }

    @Test
    public void readUser_explicitList_sizeGreaterThanZero() throws Exception {
        assertSearch(READ_USER, EXPLICIT_LIST, "", TOTAL_DOCS, DEFAULT_SIZE, null);
        assertSearch(READ_USER, EXPLICIT_LIST, "?size=100", TOTAL_DOCS, (int) TOTAL_DOCS, null);
    }

    @Test
    public void readUser_count() throws Exception {
        assertCount(READ_USER, ALIAS, TOTAL_DOCS);
        assertCount(READ_USER, PATTERN, TOTAL_DOCS);
        assertCount(READ_USER, EXPLICIT_LIST, TOTAL_DOCS);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // non-admin with DLS (level=info only)
    // ---------------------------------------------------------------------------------------------------------------

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
    public void dlsUser_explicitList_size0() throws Exception {
        assertSearch(DLS_USER, EXPLICIT_LIST, "?size=0", INFO_DOCS, 0, null);
    }

    @Test
    public void dlsUser_explicitList_sizeGreaterThanZero() throws Exception {
        assertSearch(DLS_USER, EXPLICIT_LIST, "", INFO_DOCS, (int) INFO_DOCS, "info");
        assertSearch(DLS_USER, EXPLICIT_LIST, "?size=100", INFO_DOCS, (int) INFO_DOCS, "info");
    }

    @Test
    public void dlsUser_count() throws Exception {
        assertCount(DLS_USER, ALIAS, INFO_DOCS);
        assertCount(DLS_USER, PATTERN, INFO_DOCS);
        assertCount(DLS_USER, EXPLICIT_LIST, INFO_DOCS);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // non-admin with access only via the alias
    // ---------------------------------------------------------------------------------------------------------------

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
}
