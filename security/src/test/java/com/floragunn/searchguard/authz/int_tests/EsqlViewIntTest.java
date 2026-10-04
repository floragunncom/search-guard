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

import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Authorization of ES|QL queries and views.
 *
 * How ES|QL is authorized: The top-level request indices:data/read/esql (EsqlQueryRequest) does not expose the indices it is
 * going to read (the query might reference views), so it is only checked on action level (see
 * ActionRequestIntrospector). The actually referenced indices are checked on the sub-actions issued by the ES|QL
 * coordinator (indices:data/read/esql/resolve_views, indices:data/read/esql/resolve_fields,
 * indices:data/read/esql/search_shards) and, as the final gate, on the data nodes for indices:data/read/esql/data,
 * which is a plain transport request authorized in SearchGuardRequestHandler.
 *
 * 403 vs 400: The ES|QL coordinator resolves indices with ignore_unavailable=true. With the regular
 * ignore_unauthorized_indices (DNFOF) semantics, a forbidden concrete index would be reduced to an empty result and
 * ES|QL would then answer HTTP 400 "Unknown index" (which is what X-Pack security does). Search Guard excludes the
 * ES|QL actions from the "empty result allowed" DNFOF list (see AuthorizationConfig), so a forbidden concrete index
 * or view name is denied with HTTP 403 (queryIndex_withoutPermission, queryView_withoutPermissionForSourceIndex).
 * Partially authorized wildcard expressions are still reduced to the authorized indices and return HTTP 200
 * (queryIndexPattern_withPartialPermission).
 *
 * Partial results: ES|QL allows partial results by default (allow_partial_results=true). If the data node check
 * denies shards, the denial is recorded as a shard failure of type security_exception. A query without any rows then
 * fails as a whole with HTTP 403 (queryIndex_dataNodeCheck_withoutPermission). Aggregations may still emit a row and
 * answer HTTP 200 with is_partial=true and zero documents read; see EsqlMultiNodeAuthorizationIntTest for that case.
 * The data node check only matters for requests which passed the coordinator checks; normally, the coordinator
 * checks already deny with 403.
 */
public class EsqlViewIntTest {

    static TestSgConfig.User USER_NO_PERMISSIONS = new TestSgConfig.User("esql_view_no_permissions")
            .roles(new TestSgConfig.Role("esql_view_no_permissions").clusterPermissions().indexPermissions().on().aliasPermissions().on()
                    .dataStreamPermissions().on());

    static TestSgConfig.User USER_WITH_PERMISSIONS = new TestSgConfig.User("esql_view_with_permissions")
            .roles(new TestSgConfig.Role("esql_view_with_permissions").clusterPermissions().indexPermissions("*").on("index_allowed*")
                    .aliasPermissions().on().dataStreamPermissions().on());

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
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(USER_NO_PERMISSIONS, USER_WITH_PERMISSIONS, USER_DATA_NODE_PERMISSION_ONLY_FOR_ALLOWED)
            .authzDebug(true)
            .enterpriseModulesEnabled()
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void createTestIndices() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/index_allowed",
                    DocNode.of("mappings.properties.category.type", "keyword"));
            assertThat(response, isOk());
            response = adminClient.putJson("/index_allowed/_doc/1?refresh=true", DocNode.of("category", "allowed"));
            assertThat(response, isCreated());

            response = adminClient.putJson("/index_forbidden", DocNode.of("mappings.properties.category.type", "keyword"));
            assertThat(response, isOk());
            response = adminClient.putJson("/index_forbidden/_doc/1?refresh=true", DocNode.of("category", "forbidden"));
            assertThat(response, isCreated());
        }
    }

    @Test
    public void createView_noPermission() throws Exception {
        String viewName = "index_allowed_create_no_permission";

        try (GenericRestClient client = cluster.getRestClient(USER_NO_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.putJson("/_query/view/" + viewName, DocNode.of("query", "FROM index_allowed"));

            assertThat(response, isForbidden());
            try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
                assertThat(adminClient.get("/_query/view/" + viewName), isNotFound());
            }
        }
    }

    @Test
    public void createAndUpdateView_withPermission() throws Exception {
        String viewName = "index_allowed_create_and_update";

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.putJson("/_query/view/" + viewName,
                    DocNode.of("query", "FROM index_allowed"));
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("acknowledged", equalTo(true))));

            response = client.putJson("/_query/view/" + viewName,
                    DocNode.of("query", "FROM index_allowed | WHERE category == \"allowed\""));
            assertThat(response, isOk());

            try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
                GenericRestClient.HttpResponse getResponse = adminClient.get("/_query/view/" + viewName);
                assertThat(getResponse, isOk());
                assertThat(getResponse, json(nodeAt("views[0].name", equalTo(viewName))));
                assertThat(getResponse,
                        json(nodeAt("views[0].query", equalTo("FROM index_allowed | WHERE category == \"allowed\""))));
            }
        }
    }

    @Test
    public void createView_withoutPermissionForViewName() throws Exception {
        String viewName = "index_forbidden_create";

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.putJson("/_query/view/" + viewName, DocNode.of("query", "FROM index_allowed"));

            assertThat(response, isForbidden());
            try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
                assertThat(adminClient.get("/_query/view/" + viewName), isNotFound());
            }
        }
    }

    @Test
    public void getView_withPermission() throws Exception {
        String viewName = "index_allowed_get";
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/_query/view/" + viewName,
                    DocNode.of("query", "FROM index_allowed"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.get("/_query/view/" + viewName);

            assertThat(response, isOk());
            assertThat(response, json(nodeAt("views[0].name", equalTo(viewName))));
            assertThat(response, json(nodeAt("views[0].query", equalTo("FROM index_allowed"))));
        }
    }

    @Test
    public void getView_withoutPermission() throws Exception {
        String viewName = "index_forbidden_get";
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/_query/view/" + viewName, DocNode.of("query", "FROM index_allowed"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.get("/_query/view/" + viewName);

            assertThat(response, isForbidden());
        }
    }

    @Test
    public void listViews_returnsOnlyPermittedViews() throws Exception {
        String allowedViewName = "index_allowed_list";
        String forbiddenViewName = "index_forbidden_list";
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/_query/view/" + allowedViewName,
                    DocNode.of("query", "FROM index_allowed"));
            assertThat(response, isOk());
            response = adminClient.putJson("/_query/view/" + forbiddenViewName, DocNode.of("query", "FROM index_forbidden"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.get("/_query/view");

            // At the moment, the request cannot be resolved, so the request is rejected
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void deleteView_withPermission() throws Exception {
        String viewName = "index_allowed_delete";
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/_query/view/" + viewName,
                    DocNode.of("query", "FROM index_allowed"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.delete("/_query/view/" + viewName);

            assertThat(response, isOk());
            assertThat(response, json(nodeAt("acknowledged", equalTo(true))));
            try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
                assertThat(adminClient.get("/_query/view/" + viewName), isNotFound());
            }
        }
    }

    @Test
    public void deleteView_withoutPermission() throws Exception {
        String viewName = "index_forbidden_delete";
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/_query/view/" + viewName,
                    DocNode.of("query", "FROM index_allowed"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.delete("/_query/view/" + viewName);

            assertThat(response, isForbidden());
            try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
                assertThat(adminClient.get("/_query/view/" + viewName), isOk());
            }
        }
    }

    @Test
    public void queryView_withPermissionForViewAndSourceIndex() throws Exception {
        String viewName = "index_allowed_query";
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/_query/view/" + viewName,
                    DocNode.of("query", "FROM index_allowed | KEEP category"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM " + viewName));

            assertThat(response, isOk());
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of("allowed"))))));
        }
    }

    @Test
    public void queryIndex_withPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM index_allowed | KEEP category"));

            assertThat(response, isOk());
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of("allowed"))))));
        }
    }

    @Test
    public void queryIndex_withoutPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM index_forbidden | KEEP category"));

            assertThat(response, isForbidden());
        }
    }

    @Test
    public void queryIndexPattern_withPartialPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM index_* | KEEP category"));

            // ignore_unauthorized_indices: The query is reduced to the indices the user has access to
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of("allowed"))))));
        }
    }

    @Test
    public void queryIndex_dataNodeCheck_withPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_DATA_NODE_PERMISSION_ONLY_FOR_ALLOWED)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM index_allowed | KEEP category"));

            assertThat(response, isOk());
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of("allowed"))))));
        }
    }

    @Test
    public void queryIndex_dataNodeCheck_withoutPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_DATA_NODE_PERMISSION_ONLY_FOR_ALLOWED)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM index_forbidden | KEEP category"));

            assertThat(response, isForbidden());
        }
    }

    @Test
    public void queryView_dataNodeCheck_withoutPermissionForSourceIndex() throws Exception {
        String viewName = "index_allowed_query_data_node_check";
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/_query/view/" + viewName,
                    DocNode.of("query", "FROM index_forbidden | KEEP category"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_DATA_NODE_PERMISSION_ONLY_FOR_ALLOWED)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM " + viewName));

            assertThat(response, isForbidden());
        }
    }

    @Test
    public void queryIndex_noPermissions() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_NO_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM index_allowed | KEEP category"));

            assertThat(response, isForbidden());
        }
    }

    @Test
    public void queryView_withoutPermissionForViewName() throws Exception {
        String viewName = "index_forbidden_query";
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/_query/view/" + viewName,
                    DocNode.of("query", "FROM index_allowed | KEEP category"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM " + viewName));

            assertThat(response, isForbidden());
        }
    }

    @Test
    public void queryView_withoutPermissionForSourceIndex() throws Exception {
        String viewName = "index_allowed_query_forbidden_source";
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson("/_query/view/" + viewName,
                    DocNode.of("query", "FROM index_forbidden | KEEP category"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM " + viewName));

            assertThat(response, isForbidden());
        }
    }
}
