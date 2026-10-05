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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.meta.Meta;

/**
 * End-to-end reproducer for the "wrong hits.total with size=0 on an alias search" issue.
 *
 * Mechanism (see DlsStaleAliasSnapshotInvariantTest for the unit-level version):
 *
 * <ul>
 * <li>Elasticsearch caches shard-level search results in the shard request cache for size=0 requests. The cache key does not
 * contain anything user-related.</li>
 * <li>The coordinator-side DlsFlsValve disables the request cache only if hasRestrictions() reports a restriction for the
 * requesting user on the requested alias.</li>
 * <li>The shard-side DlsFlsSearchOperationListener decides independently per member index whether to inject a DLS query. It
 * does so before Elasticsearch computes the cache key.</li>
 * <li>If the valve says "unrestricted" (cache on) while the shard side says "restricted", the restricted per-shard result lands
 * in the cache and is served to every subsequent size=0 request with the same bytes, including the ones of an unrestricted
 * admin.</li>
 * </ul>
 *
 * Two situations make the decisions diverge; both are covered here with separate index sets:
 *
 * <ul>
 * <li>Index set old-000001..3 with alias "new", old-000003 being the write index (exactly like in the issue report). The
 * metadata model does not see the write index as a member of the alias, so the alias grant does not reach old-000003 on the
 * shard level. Nothing else is needed; this happens with an up-to-date "stateful rules" snapshot.</li>
 * <li>Index set plain-000001..3 with alias "plain" (no write index). Here the test makes the stateful rules snapshot stale by
 * replacing it with one which does not know the alias. This is the state between an alias change and the asynchronous rebuild
 * of the snapshot.</li>
 * </ul>
 *
 * The *_poisonsCache_* tests are expected to FAIL on the current code base. The control tests pass.
 */
public class DlsRequestCachePoisoningIntTest {

    static final String ALIAS = "new";
    static final String[] INDICES = new String[] { "old-000001", "old-000002", "old-000003" };

    static final String PLAIN_ALIAS = "plain";
    static final String[] PLAIN_INDICES = new String[] { "plain-000001", "plain-000002", "plain-000003" };

    static final String[] LEVELS = new String[] { "info", "warn", "error", "info", "debug" };
    static final long TOTAL_DOCS = 15;
    static final long INFO_DOCS = 6;

    /** A snapshot which knows all indices but no alias */
    static final Meta STALE_META = Meta.Mock.indices("old-000001", "old-000002", "old-000003", "plain-000001", "plain-000002", "plain-000003");

    static final DocNode DLS_INFO_ONLY = DocNode.of("term.level.value", "info");

    static final Role ALL_ACCESS_ROLE = new Role("all_access").clusterPermissions("*").indexPermissions("*").on("*").aliasPermissions("*").on("*");
    static final Role ALIAS_NEW_ROLE = new Role("alias_new").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").aliasPermissions("SGS_READ")
            .on(ALIAS);
    static final Role ALIAS_PLAIN_ROLE = new Role("alias_plain").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").aliasPermissions("SGS_READ")
            .on(PLAIN_ALIAS);
    static final Role DLS_PLAIN_ROLE = new Role("dls_plain").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_READ")
            .dls(DLS_INFO_ONLY).on("plain-*");

    /** Mirrors the admin:admin basic auth user of the issue report. Not an admin cert user. */
    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin").password("admin").roles(ALL_ACCESS_ROLE);

    /** Access only via alias "new" (which has a write index), no DLS anywhere. */
    static final TestSgConfig.User ALIAS_USER = new TestSgConfig.User("alias_user").roles(ALIAS_NEW_ROLE);

    /** Access only via alias "plain", no DLS anywhere. On the stale snapshot the shard side ends in "fully restricted". */
    static final TestSgConfig.User ALIAS_PLAIN_USER = new TestSgConfig.User("alias_plain_user").roles(ALIAS_PLAIN_ROLE);

    /** Unrestricted via alias "plain" plus a DLS role on its members. On the stale snapshot the shard side applies the DLS query. */
    static final TestSgConfig.User DLS_ALIAS_PLAIN_USER = new TestSgConfig.User("dls_alias_plain_user").roles(ALIAS_PLAIN_ROLE, DLS_PLAIN_ROLE);

    /** Only the DLS role. The valve detects the restriction and disables the cache: control case. */
    static final TestSgConfig.User DLS_PLAIN_USER = new TestSgConfig.User("dls_plain_user").roles(DLS_PLAIN_ROLE);

    // TestSgConfig prefixes the roles declared on a user with user_<name>__
    static final PrivilegesEvaluationContext ALIAS_USER_CONTEXT = context("alias_user", "user_alias_user__alias_new");
    static final PrivilegesEvaluationContext ALIAS_PLAIN_USER_CONTEXT = context("alias_plain_user", "user_alias_plain_user__alias_plain");
    static final PrivilegesEvaluationContext DLS_ALIAS_PLAIN_USER_CONTEXT = context("dls_alias_plain_user",
            "user_dls_alias_plain_user__alias_plain", "user_dls_alias_plain_user__dls_plain");

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().metrics("detailed");

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, ALIAS_USER, ALIAS_PLAIN_USER, DLS_ALIAS_PLAIN_USER, DLS_PLAIN_USER).resources("dlsfls").singleNode().embedded().build();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            // Index set 1: exactly like the curl sequence of the issue report, each index is created together with its alias
            // membership, the last one is the write index.
            for (int i = 0; i < INDICES.length; i++) {
                createIndex(client, INDICES[i], ALIAS, i == INDICES.length - 1);
            }

            // Index set 2: alias without a write index (an alias with more than one member and no explicit write index has none)
            for (String index : PLAIN_INDICES) {
                createIndex(client, index, PLAIN_ALIAS, false);
            }

            List<DocNode> bulk = new ArrayList<>();
            addDocuments(bulk, INDICES);
            addDocuments(bulk, PLAIN_INDICES);

            HttpResponse response = client.putNdJson("/_bulk?refresh=true", bulk.toArray(new DocNode[0]));
            assertThat(response, isOk());
            assertFalse(response.getBody(), response.getBodyAsDocNode().getBoolean("errors"));

            for (String pattern : new String[] { "old-*", "plain-*" }) {
                response = client.get("/" + pattern + "/_count");
                assertThat(response, isOk());
                assertThat(response.getBody(), response.getBodyAsDocNode().getNumber("count").longValue(), is(TOTAL_DOCS));
            }
        }
    }

    /**
     * Every test starts with an empty request cache and a snapshot which is consistent with the cluster state.
     */
    @Before
    public void resetState() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.post("/old-*,plain-*/_cache/clear?request=true");
            assertThat(response, isOk());
        }

        RoleBasedDocumentAuthorization documentAuthorization = liveDocumentAuthorization();
        documentAuthorization.updateIndices(freshMeta());

        // Only checked for the alias without write index; for the alias with write index the shard side is inconsistent even
        // with a fresh snapshot (see DlsStaleAliasSnapshotInvariantTest)
        Awaitility.await("stateful DLS rules consistent with cluster state").atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertConsistent(documentAuthorization, ALIAS_PLAIN_USER_CONTEXT, PLAIN_ALIAS, PLAIN_INDICES));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Scenario 1: alias with write index, up-to-date snapshot. This is the issue report.
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void writeIndexAlias_aliasUser_poisonsCache_adminSize0GetsWrongTotal() throws Exception {
        assertDivergence(ALIAS_USER_CONTEXT, ALIAS, INDICES[2]);
        String statsBefore = requestCacheStats("old-*");

        // This request is evaluated as unrestricted by the valve (cache stays on) but as fully restricted on the write index shard.
        SearchResult poisoner = search(ALIAS_USER, "/" + ALIAS + "/_search?size=0");
        collector.checkThat("alias_user GET /new/_search?size=0 (the request which writes the cache entries); request cache before: "
                + statsBefore + "; response: " + poisoner.body, poisoner.total, is(TOTAL_DOCS));

        String statsAfterPoisoner = requestCacheStats("old-*");

        SearchResult admin = search(ADMIN, "/" + ALIAS + "/_search?size=0");
        String statsAfterAdmin = requestCacheStats("old-*");
        collector.checkThat("admin GET /new/_search?size=0 after alias_user's size=0 search; request cache after alias_user: "
                + statsAfterPoisoner + "; after admin: " + statsAfterAdmin + "; response: " + admin.body, admin.total, is(TOTAL_DOCS));

        SearchResult adminPattern = search(ADMIN, "/old-*/_search?size=0");
        collector.checkThat("admin GET /old-*/_search?size=0 after alias_user's size=0 search on the alias; response: " + adminPattern.body,
                adminPattern.total, is(TOTAL_DOCS));

        SearchResult adminExplicit = search(ADMIN, "/old-000001,old-000002,old-000003/_search?size=0");
        collector.checkThat("admin GET /old-000001,old-000002,old-000003/_search?size=0 after alias_user's size=0 search on the alias; response: "
                + adminExplicit.body, adminExplicit.total, is(TOTAL_DOCS));

        // size > 0 is not served from the request cache by default
        SearchResult adminSize100 = search(ADMIN, "/" + ALIAS + "/_search?size=100");
        collector.checkThat("admin GET /new/_search?size=100 total; response: " + adminSize100.body, adminSize100.total, is(TOTAL_DOCS));
        collector.checkThat("admin GET /new/_search?size=100 hits; response: " + adminSize100.body, adminSize100.hits, is((int) TOTAL_DOCS));

        SearchResult adminUnsetSize = search(ADMIN, "/" + ALIAS + "/_search");
        collector.checkThat("admin GET /new/_search total; response: " + adminUnsetSize.body, adminUnsetSize.total, is(TOTAL_DOCS));
        collector.checkThat("admin GET /new/_search hits; response: " + adminUnsetSize.body, adminUnsetSize.hits, is(10));

        // Bypassing the request cache explicitly
        SearchResult adminNoCache = search(ADMIN, "/" + ALIAS + "/_search?size=0&request_cache=false");
        collector.checkThat("admin GET /new/_search?size=0&request_cache=false; response: " + adminNoCache.body, adminNoCache.total,
                is(TOTAL_DOCS));
    }

    /**
     * With an explicit request_cache=true, Elasticsearch also caches size > 0 requests. The poisoned entries then make documents
     * disappear from the hit list, not only from the totals.
     */
    @Test
    public void writeIndexAlias_aliasUser_poisonsCache_requestCacheTrue_adminLosesHits() throws Exception {
        assertDivergence(ALIAS_USER_CONTEXT, ALIAS, INDICES[2]);

        // Note: the request_cache flag is part of the cache key, so the poisoning request and the admin request must use the same value
        SearchResult poisoner = search(ALIAS_USER, "/" + ALIAS + "/_search?size=5&request_cache=true");
        collector.checkThat("alias_user GET /new/_search?size=5&request_cache=true total; response: " + poisoner.body, poisoner.total,
                is(TOTAL_DOCS));
        collector.checkThat("alias_user GET /new/_search?size=5&request_cache=true hits; response: " + poisoner.body, poisoner.hits, is(5));

        SearchResult admin = search(ADMIN, "/" + ALIAS + "/_search?size=5&request_cache=true");
        collector.checkThat("admin GET /new/_search?size=5&request_cache=true total after alias_user's search; response: " + admin.body,
                admin.total, is(TOTAL_DOCS));
        collector.checkThat("admin GET /new/_search?size=5&request_cache=true hits after alias_user's search; response: " + admin.body,
                admin.hits, is(5));
    }

    /**
     * The opposite direction: admin's correct size=0 result is cached first and then served to the alias user, which hides the
     * shard-level restriction of the alias user. Documents the leak direction of the user-agnostic cache key.
     */
    @Test
    public void writeIndexAlias_adminFirst_aliasUserSize0ServedFromCache() throws Exception {
        assertDivergence(ALIAS_USER_CONTEXT, ALIAS, INDICES[2]);

        SearchResult admin = search(ADMIN, "/" + ALIAS + "/_search?size=0");
        collector.checkThat("admin GET /new/_search?size=0 on a cleared cache; response: " + admin.body, admin.total, is(TOTAL_DOCS));

        SearchResult aliasUserSize0 = search(ALIAS_USER, "/" + ALIAS + "/_search?size=0");
        SearchResult aliasUserSize100 = search(ALIAS_USER, "/" + ALIAS + "/_search?size=100");

        // Both are expected to be 15 (the alias grant covers all members). Today, size=0 yields 15 only because admin's cached
        // result is served, while size=100 executes the query and yields 10.
        collector.checkThat("alias_user GET /new/_search?size=0 after admin's size=0 search; response: " + aliasUserSize0.body,
                aliasUserSize0.total, is(TOTAL_DOCS));
        collector.checkThat("alias_user GET /new/_search?size=100; response: " + aliasUserSize100.body, aliasUserSize100.total, is(TOTAL_DOCS));
        collector.checkThat("alias_user GET /new/_search?size=100 hits; response: " + aliasUserSize100.body, aliasUserSize100.hits,
                is((int) TOTAL_DOCS));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Scenario 2: alias without write index, stale snapshot
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void staleSnapshot_aliasUser_poisonsCache_adminSize0GetsWrongTotal() throws Exception {
        forceStaleSnapshot(ALIAS_PLAIN_USER_CONTEXT, PLAIN_ALIAS, PLAIN_INDICES[0]);
        String statsBefore = requestCacheStats("plain-*");

        // This request is evaluated as unrestricted by the valve (cache stays on) but as fully restricted on the shards.
        SearchResult poisoner = search(ALIAS_PLAIN_USER, "/" + PLAIN_ALIAS + "/_search?size=0");
        collector.checkThat("alias_plain_user GET /plain/_search?size=0 (the request which writes the cache entries); request cache before: "
                + statsBefore + "; response: " + poisoner.body, poisoner.total, is(TOTAL_DOCS));

        String statsAfterPoisoner = requestCacheStats("plain-*");

        SearchResult admin = search(ADMIN, "/" + PLAIN_ALIAS + "/_search?size=0");
        String statsAfterAdmin = requestCacheStats("plain-*");
        collector.checkThat("admin GET /plain/_search?size=0 after alias_plain_user's size=0 search; request cache after alias_plain_user: "
                + statsAfterPoisoner + "; after admin: " + statsAfterAdmin + "; response: " + admin.body, admin.total, is(TOTAL_DOCS));

        SearchResult adminPattern = search(ADMIN, "/plain-*/_search?size=0");
        collector.checkThat("admin GET /plain-*/_search?size=0 after alias_plain_user's size=0 search on the alias; response: "
                + adminPattern.body, adminPattern.total, is(TOTAL_DOCS));

        SearchResult adminSize100 = search(ADMIN, "/" + PLAIN_ALIAS + "/_search?size=100");
        collector.checkThat("admin GET /plain/_search?size=100 total; response: " + adminSize100.body, adminSize100.total, is(TOTAL_DOCS));
        collector.checkThat("admin GET /plain/_search?size=100 hits; response: " + adminSize100.body, adminSize100.hits, is((int) TOTAL_DOCS));

        SearchResult adminNoCache = search(ADMIN, "/" + PLAIN_ALIAS + "/_search?size=0&request_cache=false");
        collector.checkThat("admin GET /plain/_search?size=0&request_cache=false; response: " + adminNoCache.body, adminNoCache.total,
                is(TOTAL_DOCS));
    }

    @Test
    public void staleSnapshot_dlsAliasUser_poisonsCache_adminSize0GetsDlsFilteredTotal() throws Exception {
        forceStaleSnapshot(DLS_ALIAS_PLAIN_USER_CONTEXT, PLAIN_ALIAS, PLAIN_INDICES[0]);

        // The user is unrestricted via the alias grant (most permissive role wins), so it must see all documents.
        // On the stale snapshot, the shard side only sees the DLS role and applies its query.
        SearchResult poisoner = search(DLS_ALIAS_PLAIN_USER, "/" + PLAIN_ALIAS + "/_search?size=0");
        collector.checkThat("dls_alias_plain_user GET /plain/_search?size=0 (the request which writes the cache entries); response: "
                + poisoner.body, poisoner.total, is(TOTAL_DOCS));

        SearchResult admin = search(ADMIN, "/" + PLAIN_ALIAS + "/_search?size=0");
        collector.checkThat("admin GET /plain/_search?size=0 after dls_alias_plain_user's size=0 search; response: " + admin.body,
                admin.total, is(TOTAL_DOCS));

        SearchResult adminSize100 = search(ADMIN, "/" + PLAIN_ALIAS + "/_search?size=100");
        collector.checkThat("admin GET /plain/_search?size=100 total; response: " + adminSize100.body, adminSize100.total, is(TOTAL_DOCS));
        collector.checkThat("admin GET /plain/_search?size=100 hits; response: " + adminSize100.body, adminSize100.hits, is((int) TOTAL_DOCS));
    }

    @Test
    public void staleSnapshot_aliasUser_poisonsCache_requestCacheTrue_adminLosesHits() throws Exception {
        forceStaleSnapshot(ALIAS_PLAIN_USER_CONTEXT, PLAIN_ALIAS, PLAIN_INDICES[0]);

        SearchResult poisoner = search(ALIAS_PLAIN_USER, "/" + PLAIN_ALIAS + "/_search?size=5&request_cache=true");
        collector.checkThat("alias_plain_user GET /plain/_search?size=5&request_cache=true total; response: " + poisoner.body, poisoner.total,
                is(TOTAL_DOCS));
        collector.checkThat("alias_plain_user GET /plain/_search?size=5&request_cache=true hits; response: " + poisoner.body, poisoner.hits,
                is(5));

        SearchResult admin = search(ADMIN, "/" + PLAIN_ALIAS + "/_search?size=5&request_cache=true");
        collector.checkThat("admin GET /plain/_search?size=5&request_cache=true total after alias_plain_user's search; response: " + admin.body,
                admin.total, is(TOTAL_DOCS));
        collector.checkThat("admin GET /plain/_search?size=5&request_cache=true hits after alias_plain_user's search; response: " + admin.body,
                admin.hits, is(5));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Controls
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * If the valve detects the restriction, it disables the request cache for the request. Nothing gets cached and admin is
     * unaffected. Passes today and must still pass after a fix.
     */
    @Test
    public void control_dlsUser_detectedRestriction_doesNotPoisonCache() throws Exception {
        String statsBefore = requestCacheStats("plain-*");

        SearchResult dlsUser = search(DLS_PLAIN_USER, "/" + PLAIN_ALIAS + "/_search?size=0");
        collector.checkThat("dls_plain_user GET /plain/_search?size=0; response: " + dlsUser.body, dlsUser.total, is(INFO_DOCS));

        String statsAfterDlsUser = requestCacheStats("plain-*");
        collector.checkThat("request cache stats must not change by a search of a user with a detected DLS restriction; before: " + statsBefore
                + "; after: " + statsAfterDlsUser, statsAfterDlsUser, is(statsBefore));

        SearchResult admin = search(ADMIN, "/" + PLAIN_ALIAS + "/_search?size=0");
        collector.checkThat("admin GET /plain/_search?size=0 after dls_plain_user's size=0 search; response: " + admin.body, admin.total,
                is(TOTAL_DOCS));

        SearchResult dlsUserSize100 = search(DLS_PLAIN_USER, "/" + PLAIN_ALIAS + "/_search?size=100");
        collector.checkThat("dls_plain_user GET /plain/_search?size=100 total; response: " + dlsUserSize100.body, dlsUserSize100.total,
                is(INFO_DOCS));
        collector.checkThat("dls_plain_user GET /plain/_search?size=100 hits; response: " + dlsUserSize100.body, dlsUserSize100.hits,
                is((int) INFO_DOCS));
    }

    /**
     * With an alias without write index and a snapshot which is consistent with the cluster state, valve and shard side agree
     * and admin gets correct totals after the alias user's search. Passes today.
     */
    @Test
    public void control_consistentSnapshot_adminSize0Correct() throws Exception {
        SearchResult aliasUser = search(ALIAS_PLAIN_USER, "/" + PLAIN_ALIAS + "/_search?size=0");
        collector.checkThat("alias_plain_user GET /plain/_search?size=0; response: " + aliasUser.body, aliasUser.total, is(TOTAL_DOCS));

        SearchResult admin = search(ADMIN, "/" + PLAIN_ALIAS + "/_search?size=0");
        collector.checkThat("admin GET /plain/_search?size=0 after alias_plain_user's size=0 search; response: " + admin.body, admin.total,
                is(TOTAL_DOCS));

        SearchResult dlsAliasUser = search(DLS_ALIAS_PLAIN_USER, "/" + PLAIN_ALIAS + "/_search?size=0");
        collector.checkThat("dls_alias_plain_user GET /plain/_search?size=0; response: " + dlsAliasUser.body, dlsAliasUser.total,
                is(TOTAL_DOCS));
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Replaces the stateful rules snapshot of the live RoleBasedDocumentAuthorization with one which knows the member indices
     * but no alias. This is the state between an alias change and the asynchronous rebuild of the snapshot. Searches do not
     * change the cluster metadata, so the asynchronous updater does not undo this during the test.
     */
    private static void forceStaleSnapshot(PrivilegesEvaluationContext context, String alias, String memberIndex) throws Exception {
        liveDocumentAuthorization().updateIndices(STALE_META);
        assertDivergence(context, alias, memberIndex);
    }

    /**
     * Asserts the precondition of the poisoning tests: the valve reports no restrictions for the alias while the shard side
     * reports a restriction for the member index.
     */
    private static void assertDivergence(PrivilegesEvaluationContext context, String alias, String memberIndex) throws Exception {
        RoleBasedDocumentAuthorization documentAuthorization = liveDocumentAuthorization();
        Meta freshMeta = freshMeta();
        boolean valveDecision = documentAuthorization.hasRestrictions(context, ResolvedIndices.of(freshMeta, alias), Meter.NO_OP);
        DlsRestriction shardDecision = documentAuthorization.getRestriction(context, (Meta.Index) freshMeta.getIndexOrLike(memberIndex),
                Meter.NO_OP);

        assertFalse("Precondition: the valve must report no restrictions for " + context.getMappedRoles() + " on alias " + alias, valveDecision);
        assertFalse("Precondition: the shard side must report a restriction for " + context.getMappedRoles() + " on " + memberIndex
                + ". If this fails, valve and shard side no longer diverge and this test needs to be revisited. Shard decision: "
                + shardDecision, shardDecision.isUnrestricted());
    }

    private static void assertConsistent(RoleBasedDocumentAuthorization documentAuthorization, PrivilegesEvaluationContext context,
            String alias, String[] memberIndices) throws Exception {
        Meta freshMeta = freshMeta();

        assertFalse("Valve: no restrictions expected for " + context.getMappedRoles() + " on alias " + alias,
                documentAuthorization.hasRestrictions(context, ResolvedIndices.of(freshMeta, alias), Meter.NO_OP));

        for (String index : memberIndices) {
            DlsRestriction restriction = documentAuthorization.getRestriction(context, (Meta.Index) freshMeta.getIndexOrLike(index), Meter.NO_OP);
            assertTrue("Shard: no restrictions expected for " + context.getMappedRoles() + " on " + index + "; got: " + restriction,
                    restriction.isUnrestricted());
        }
    }

    private static Meta freshMeta() {
        return Meta.from(cluster.getInjectable(ClusterService.class));
    }

    /**
     * Fetches the RoleBasedDocumentAuthorization instance which is used by the DlsFlsValve and the DlsFlsSearchOperationListener
     * of the (single) node.
     */
    @SuppressWarnings("unchecked")
    private static RoleBasedDocumentAuthorization liveDocumentAuthorization() throws Exception {
        SearchGuardModulesRegistry modulesRegistry = cluster.getInjectable(SearchGuardModulesRegistry.class);
        DlsFlsModule module = null;

        for (SearchGuardModule candidate : modulesRegistry.getModules()) {
            if (candidate instanceof DlsFlsModule) {
                module = (DlsFlsModule) candidate;
                break;
            }
        }

        assertTrue("DlsFlsModule not found in " + modulesRegistry.getModules(), module != null);

        Field configField = DlsFlsModule.class.getDeclaredField("config");
        configField.setAccessible(true);
        AtomicReference<DlsFlsProcessedConfig> config = (AtomicReference<DlsFlsProcessedConfig>) configField.get(module);
        RoleBasedDocumentAuthorization documentAuthorization = config.get().getDocumentAuthorization();

        assertTrue("DlsFlsProcessedConfig does not have a RoleBasedDocumentAuthorization yet", documentAuthorization != null);

        return documentAuthorization;
    }

    private static void createIndex(GenericRestClient client, String index, String alias, boolean writeIndex) throws Exception {
        HttpResponse response = client.putJson("/" + index,
                DocNode.of("settings.index.number_of_shards", 1, "settings.index.number_of_replicas", 0, "mappings.properties.level.type",
                        "keyword", "mappings.properties.message.type", "text", "mappings.properties.ts.type", "date",
                        "aliases." + alias + ".is_write_index", writeIndex));
        assertThat(response, isOk());
    }

    private static void addDocuments(List<DocNode> bulk, String[] indices) {
        for (int indexNo = 0; indexNo < indices.length; indexNo++) {
            for (int docNo = 0; docNo < LEVELS.length; docNo++) {
                bulk.add(DocNode.of("index._index", indices[indexNo], "index._id", indices[indexNo] + "-" + (docNo + 1)));
                bulk.add(DocNode.of("message", "log " + (indexNo + 1) + "-" + (docNo + 1), "level", LEVELS[docNo], "ts",
                        "2024-01-0" + (indexNo + 1) + "T0" + docNo + ":00:00Z"));
            }
        }
    }

    private static String requestCacheStats(String indexPattern) throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.get("/" + indexPattern + "/_stats/request_cache");
            assertThat(response, isOk());
            return response.getBodyAsDocNode().getAsNode("_all").getAsNode("total").getAsNode("request_cache").toJsonString();
        }
    }

    private static SearchResult search(TestSgConfig.User user, String path) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            HttpResponse response = client.get(path);
            assertThat("[" + user.getName() + "] GET " + path, response, isOk());

            DocNode body = response.getBodyAsDocNode();
            DocNode total = body.getAsNode("hits").getAsNode("total");

            return new SearchResult(total.getNumber("value").longValue(), total.getAsString("relation"),
                    body.getAsNode("hits").getAsListOfNodes("hits").size(), response.getBody());
        }
    }

    private static PrivilegesEvaluationContext context(String userName, String... mappedRoles) {
        User user = new User.Builder().name(userName).build();
        return new PrivilegesEvaluationContext(user, false, ImmutableSet.ofArray(mappedRoles), null, null, true, null, null);
    }

    static class SearchResult {
        final long total;
        final String relation;
        final int hits;
        final String body;

        SearchResult(long total, String relation, int hits, String body) {
            this.total = total;
            this.relation = relation;
            this.hits = hits;
            this.body = body;
        }
    }
}
