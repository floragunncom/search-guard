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

package com.floragunn.searchguard.enterprise.auditlog.integration;

import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.junit.AsyncAssert;

/**
 * Audit log entries of ES|QL queries.
 *
 * ES|QL is authorized on two levels: the top level request passes SearchGuardFilter, while the requests which read the
 * indices are authorized by SearchGuardRequestHandler on each node which takes part in the query. The latter does not
 * write granted privileges to the audit log, as the entry of the top level request already covers the query; otherwise,
 * one query would produce an entry per participating node. Denied privileges are logged on both levels.
 *
 * The audit log is written to an index by the internal_elasticsearch sink, because the sink has to be part of the plugin:
 * ES|QL requires a real Elasticsearch distribution, which cannot load a sink from the test classpath.
 */
public class EsqlAuditlogIntTest {

    static final String AUDIT_INDEX_PATTERN = "sg7-auditlog-*";

    static final TestSgConfig.User USER = new TestSgConfig.User("user")
            .roles(new TestSgConfig.Role("user_role").clusterPermissions("*").indexPermissions("SGS_READ").on("index_allowed*"));

    /**
     * Passes all checks of the coordinating node, but lacks the privilege for the requests which read the indices. Such a
     * query is denied by SearchGuardRequestHandler.
     */
    static final TestSgConfig.User DATA_NODE_CHECK_USER = new TestSgConfig.User("data_node_check")
            .roles(new TestSgConfig.Role("data_node_check_role").clusterPermissions("*")
                    .indexPermissions("indices:data/read/esql", "indices:data/read/esql/resolve_*", "indices:data/read/esql/search_shards").on("*")
                    .indexPermissions("indices:data/read/esql/data").on("index_allowed*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));

    @ClassRule
    // The cluster has several nodes: the audit log index is created with a replica, which a single node cannot allocate, and
    // it also shows that one query produces one entry and not one per data node.
    public static LocalCluster cluster = new LocalCluster.Builder().clusterConfiguration(ClusterConfiguration.DEFAULT).sslEnabled()
            .users(USER, DATA_NODE_CHECK_USER).authc(AUTHC).enterpriseModulesEnabled()
            .nodeSettings(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, "internal_elasticsearch", //
                    ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true, //
                    ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true, //
                    // Granted privileges are disabled by default; this test is about their number
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, Collections.emptyList(), //
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, Collections.emptyList(), //
                    ConfigConstants.SEARCHGUARD_AUDIT_THREADPOOL_SIZE, 0)
            // ES|QL is only available with a real ES distribution
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void createTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            for (String index : new String[] { "index_allowed", "index_forbidden" }) {
                assertThat(client.putJson("/" + index, DocNode.of("settings.index.number_of_shards", 2, "mappings.properties.product.type",
                        "keyword")), isOk());
                assertThat(client.putJson("/" + index + "/_doc/1?refresh=true", DocNode.of("product", "apple")), isCreated());
            }
        }
    }

    @Test
    public void esqlQuery_logsOneGrantedEntry() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            assertThat(client.postJson("/_query", DocNode.of("query", "FROM index_allowed | STATS c = COUNT(*)")), isOk());
        }

        // The top level request is logged once ...
        AsyncAssert.awaitAssert("The granted privileges of the ES|QL request are logged",
                () -> countAuditEntries("GRANTED_PRIVILEGES", "indices:data/read/esql") == 1, Duration.ofSeconds(10));

        // ... while the requests which read the indices are not logged, even though they were authorized
        assertThat(countAuditEntries("GRANTED_PRIVILEGES", "indices:data/read/esql/data"), equalTo(0L));
        assertThat(countAuditEntries("GRANTED_PRIVILEGES", "indices:data/read/esql/search_shards"), equalTo(1L));
    }

    @Test
    public void esqlQuery_logsDenialOfDataRequest() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DATA_NODE_CHECK_USER)) {
            // Not an aggregation: ES|QL allows partial results by default, so an aggregation would still emit a row
            assertThat(client.postJson("/_query", DocNode.of("query", "FROM index_forbidden | KEEP product")), isForbidden());
        }

        AsyncAssert.awaitAssert("The denial of the ES|QL data request is logged",
                () -> countAuditEntries("MISSING_PRIVILEGES", "indices:data/read/esql/data") >= 1, Duration.ofSeconds(10));
    }

    /**
     * Counts the audit log entries of a category and privilege. The entries are filtered here instead of by a query, as
     * the fields of the audit log index are mapped dynamically.
     */
    private static long countAuditEntries(String category, String privilege) throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            assertThat(client.post("/" + AUDIT_INDEX_PATTERN + "/_refresh"), isOk());

            GenericRestClient.HttpResponse response = client.get("/" + AUDIT_INDEX_PATTERN + "/_search?size=1000");
            assertThat(response, isOk());

            return response.getBodyAsDocNode().findNodesByJsonPath("hits.hits[*]._source").stream()
                    .filter((source) -> category.equals(source.getAsString("audit_category"))
                            && privilege.equals(source.getAsString("audit_request_privilege")))
                    .count();
        }
    }
}
