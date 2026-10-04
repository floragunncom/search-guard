/*
 * Copyright 2026 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.floragunn.searchguard.authz.int_tests;

import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * ES|QL reads shards via indices:data/read/esql/data requests, which are plain transport requests (not TransportActions).
 * These are authorized in SearchGuardRequestHandler. On a single node, such requests arrive via a direct channel; on a
 * multi node cluster, they arrive via the network. This test covers the latter case. The data nodes are separate from the
 * master node to which the REST client connects, so the data requests always cross the network.
 *
 * See EsqlViewIntTest for an overview of how ES|QL is authorized and why forbidden indices yield HTTP 403 instead of
 * HTTP 400.
 *
 * Partial results: ES|QL allows partial results by default (allow_partial_results=true). A denial by the data node check
 * is thus not fatal for the query; it is recorded as a shard failure of type security_exception under
 * _clusters.details. If no rows are produced at all, ES|QL fails the query with the recorded failure (HTTP 403, see
 * queryIndex_dataNodeCheck_withoutPermission). An aggregation like STATS COUNT(*) emits a row even without input, so
 * such a query answers HTTP 200 with is_partial=true, documents_found=0 and the denied shards listed as failures
 * (queryIndex_dataNodeCheck_withoutPermission_aggregation). No document is read in either case. Search Guard
 * intentionally does not force allow_partial_results=false, which would make every denial a hard 403 but would also
 * remove partial results for genuine shard failures.
 */
public class EsqlMultiNodeAuthorizationIntTest {

    static TestSgConfig.User USER_WITH_PERMISSIONS = new TestSgConfig.User("esql_with_permissions")
            .roles(new TestSgConfig.Role("esql_with_permissions").clusterPermissions().indexPermissions("*").on("index_allowed*")
                    .aliasPermissions().on().dataStreamPermissions().on());

    static TestSgConfig.User USER_NO_PERMISSIONS = new TestSgConfig.User("esql_no_permissions")
            .roles(new TestSgConfig.Role("esql_no_permissions").clusterPermissions().indexPermissions().on().aliasPermissions().on()
                    .dataStreamPermissions().on());

    /**
     * This user has all coordinator level ES|QL privileges on all indices, but the privilege for the data node requests
     * (indices:data/read/esql/data) only on index_allowed*. Queries on index_forbidden thus pass all checks on the coordinator
     * and must be denied by the transport level check in SearchGuardRequestHandler.
     */
    static TestSgConfig.User USER_DATA_NODE_PERMISSION_ONLY_FOR_ALLOWED = new TestSgConfig.User("esql_data_node_check")
            .roles(new TestSgConfig.Role("esql_data_node_check").clusterPermissions()
                    .indexPermissions("indices:data/read/esql", "indices:data/read/esql/resolve_*", "indices:data/read/esql/search_shards").on("*")
                    .indexPermissions("indices:data/read/esql/data").on("index_allowed*").aliasPermissions().on().dataStreamPermissions().on());

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().clusterConfiguration(ClusterConfiguration.DEFAULT).sslEnabled()
            .users(USER_WITH_PERMISSIONS, USER_NO_PERMISSIONS, USER_DATA_NODE_PERMISSION_ONLY_FOR_ALLOWED).authzDebug(true).enterpriseModulesEnabled().useExternalProcessCluster()
            .build();

    @BeforeClass
    public static void createTestIndices() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            for (String index : new String[] { "index_allowed", "index_forbidden" }) {
                GenericRestClient.HttpResponse response = adminClient.putJson("/" + index, DocNode.of("settings.index.number_of_shards", 2,
                        "settings.index.number_of_replicas", 0, "mappings.properties.category.type", "keyword"));
                assertThat(response, isOk());
            }

            GenericRestClient.HttpResponse response = adminClient.putJson("/index_allowed/_doc/1?refresh=true", DocNode.of("category", "allowed"));
            assertThat(response, isCreated());
            response = adminClient.putJson("/index_allowed/_doc/2?refresh=true", DocNode.of("category", "allowed"));
            assertThat(response, isCreated());
            response = adminClient.putJson("/index_forbidden/_doc/1?refresh=true", DocNode.of("category", "forbidden"));
            assertThat(response, isCreated());
        }
    }

    @Test
    public void queryIndex_withPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query",
                    DocNode.of("query", "FROM index_allowed | STATS c = COUNT(*)"));

            assertThat(response, isOk());
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(2))))));
        }
    }

    @Test
    public void queryIndex_withoutPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query",
                    DocNode.of("query", "FROM index_forbidden | STATS c = COUNT(*)"));

            assertThat(response, isForbidden());
        }
    }

    @Test
    public void queryIndexPattern_withPartialPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM index_* | STATS c = COUNT(*)"));

            assertThat(response, isOk());
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(2))))));
        }
    }

    @Test
    public void queryIndex_dataNodeCheck_withPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_DATA_NODE_PERMISSION_ONLY_FOR_ALLOWED)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query",
                    DocNode.of("query", "FROM index_allowed | STATS c = COUNT(*)"));

            assertThat(response, isOk());
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(2))))));
        }
    }

    @Test
    public void queryIndex_dataNodeCheck_withoutPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_DATA_NODE_PERMISSION_ONLY_FOR_ALLOWED)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query",
                    DocNode.of("query", "FROM index_forbidden | KEEP category"));

            // All shards are denied by the data nodes and no rows are produced; ES|QL then fails the whole query
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void queryIndex_dataNodeCheck_withoutPermission_aggregation() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_DATA_NODE_PERMISSION_ONLY_FOR_ALLOWED)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query",
                    DocNode.of("query", "FROM index_forbidden | STATS c = COUNT(*)"));

            // ES|QL allows partial results by default. As STATS emits a row even without any input, the query does not fail
            // as a whole. However, all shards are denied by the data nodes, no documents are read and the security
            // exception is reported as shard failure.
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("is_partial", equalTo(true))));
            assertThat(response, json(nodeAt("documents_found", equalTo(0))));
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(0))))));
            assertThat(response, json(nodeAt("_clusters.details['(local)']._shards.failed", equalTo(2))));
            assertThat(response, json(nodeAt("_clusters.details['(local)']._shards.successful", equalTo(0))));
            assertThat(response, json(nodeAt("_clusters.details['(local)'].failures[0].reason.type", equalTo("security_exception"))));
        }
    }

    @Test
    public void queryIndex_noPermissions() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_NO_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query",
                    DocNode.of("query", "FROM index_allowed | STATS c = COUNT(*)"));

            assertThat(response, isForbidden());
        }
    }
}
