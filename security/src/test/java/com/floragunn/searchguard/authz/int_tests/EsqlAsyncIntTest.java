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
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Asynchronous ES|QL queries: POST /_query/async, GET /_query/async/&lt;id&gt;, POST /_query/async/&lt;id&gt;/stop and
 * DELETE /_query/async/&lt;id&gt;.
 *
 * How this is authorized:
 *
 * - The submit request uses the same action as a synchronous query, indices:data/read/esql. Index privileges are thus
 *   evaluated exactly as for a synchronous query, views included; see EsqlViewIntTest for the details.
 * - The async id refers to a result which is stored by Elasticsearch in the system index .async-search. Elasticsearch
 *   only protects such results against access by other users if X-Pack security is enabled, which is not the case when
 *   Search Guard is used. Search Guard therefore records the owner of the async id in its own index
 *   .searchguard_resource_owner (see ResourceOwnerService) and checks it for indices:data/read/esql/async/get,
 *   indices:data/read/esql/async/stop and the shared indices:data/read/async_search/delete. This is the same mechanism
 *   which is used for async search and for async SQL (see AsyncSqlIntTests, ResourceOwnerServiceTests).
 * - The two ES|QL specific actions are cluster actions; they are part of SGS_CLUSTER_COMPOSITE_OPS_RO. A user with the
 *   cluster permission indices:searchguard:async_search/_all_owners may access the results of other users.
 *
 * Note on timing: Like AsyncSqlIntTests, these tests do not poll. The query is submitted with
 * wait_for_completion_timeout=0s, which always yields an async id, and the result is then fetched with
 * wait_for_completion_timeout=30s, which lets Elasticsearch wait for the result.
 */
public class EsqlAsyncIntTest {

    static final String RESOURCE_OWNER_INDEX = ".searchguard_resource_owner";
    static final String ASYNC_SEARCH_ID_PREFIX = "async_search_";

    /**
     * A well formed async id (base64 of docId + taskId) which does not refer to an existing result
     */
    static final String UNKNOWN_ASYNC_ID = "FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=";

    static TestSgConfig.User USER_A = new TestSgConfig.User("user_a").roles(new Role("user_a")
            .clusterPermissions("indices:data/read/esql/async/get", "indices:data/read/esql/async/stop",
                    "indices:data/read/async_search/delete")
            .indexPermissions("SGS_READ").on("index_allowed*", "view_allowed*"));

    static TestSgConfig.User USER_B = new TestSgConfig.User("user_b").roles(new Role("user_b")
            .clusterPermissions("indices:data/read/esql/async/get", "indices:data/read/esql/async/stop",
                    "indices:data/read/async_search/delete")
            .indexPermissions("SGS_READ").on("index_allowed*", "view_allowed*"));

    /**
     * May submit async queries, but has no cluster permissions for retrieving the results
     */
    static TestSgConfig.User USER_NO_ASYNC = new TestSgConfig.User("user_no_async")
            .roles(new Role("user_no_async").clusterPermissions().indexPermissions("SGS_READ").on("index_allowed*", "view_allowed*"));

    /**
     * May access the async results of all users
     */
    static TestSgConfig.User USER_ALL_OWNERS = new TestSgConfig.User("user_all_owners").roles(new Role("user_all_owners")
            .clusterPermissions("indices:data/read/esql/async/get", "indices:data/read/esql/async/stop",
                    "indices:data/read/async_search/delete", "indices:searchguard:async_search/_all_owners")
            .indexPermissions("SGS_READ").on("index_allowed*", "view_allowed*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(USER_A, USER_B, USER_NO_ASYNC, USER_ALL_OWNERS).authzDebug(true).enterpriseModulesEnabled()
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void createTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            for (String index : new String[] { "index_allowed", "index_forbidden" }) {
                assertThat(client.putJson("/" + index, DocNode.of("settings.index.number_of_shards", 2,
                        "mappings.properties.product.type", "keyword", "mappings.properties.amount.type", "integer")), isOk());
            }

            assertThat(client.putJson("/index_allowed/_doc/1?refresh=true", DocNode.of("product", "apple", "amount", 1)), isCreated());
            assertThat(client.putJson("/index_allowed/_doc/2?refresh=true", DocNode.of("product", "pear", "amount", 2)), isCreated());
            assertThat(client.putJson("/index_allowed/_doc/3?refresh=true", DocNode.of("product", "plum", "amount", 3)), isCreated());
            assertThat(client.putJson("/index_forbidden/_doc/1?refresh=true", DocNode.of("product", "fig", "amount", 100)), isCreated());
            assertThat(client.putJson("/index_forbidden/_doc/2?refresh=true", DocNode.of("product", "date", "amount", 200)), isCreated());

            assertThat(client.putJson("/_query/view/view_allowed", DocNode.of("query", "FROM index_allowed")), isOk());
            assertThat(client.putJson("/_query/view/view_allowed_forbidden_source", DocNode.of("query", "FROM index_forbidden")), isOk());
        }
    }

    private static GenericRestClient.HttpResponse submitAsync(TestSgConfig.User user, String query) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            return client.postJson("/_query/async", DocNode.of("query", query, "wait_for_completion_timeout", "0s", "keep_on_completion", true));
        }
    }

    /**
     * Submits an async query and lets Elasticsearch wait for its completion. Errors of the query are reported by this
     * request, while a submit with wait_for_completion_timeout=0s always succeeds and reports errors on the subsequent
     * GET request.
     */
    private static GenericRestClient.HttpResponse submitAsyncAndWait(TestSgConfig.User user, String query) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            return client.postJson("/_query/async", DocNode.of("query", query, "wait_for_completion_timeout", "30s", "keep_on_completion", true));
        }
    }

    private static String submitAsyncAndGetId(TestSgConfig.User user, String query) throws Exception {
        GenericRestClient.HttpResponse response = submitAsync(user, query);
        assertThat(response, isOk());
        String asyncId = response.getBodyAsDocNode().getAsString("id");
        assertThat(response.getBody(), asyncId, notNullValue());
        return asyncId;
    }

    private static GenericRestClient.HttpResponse getAsyncResult(TestSgConfig.User user, String asyncId) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            return client.get("/_query/async/" + asyncId + "?wait_for_completion_timeout=30s");
        }
    }

    private static GenericRestClient.HttpResponse stopAsync(TestSgConfig.User user, String asyncId) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            return client.post("/_query/async/" + asyncId + "/stop");
        }
    }

    private static GenericRestClient.HttpResponse deleteAsync(TestSgConfig.User user, String asyncId) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            return client.delete("/_query/async/" + asyncId);
        }
    }

    private static void assertResourceOwner(String asyncId, String expectedUserName) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.get("/" + RESOURCE_OWNER_INDEX + "/_doc/" + ASYNC_SEARCH_ID_PREFIX + asyncId);
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("_source.user_name", equalTo(expectedUserName))));
        }
    }

    @Test
    public void asyncQuery_index_submitGetDelete() throws Exception {
        String asyncId = submitAsyncAndGetId(USER_A, "FROM index_allowed | STATS c = COUNT(*)");
        assertResourceOwner(asyncId, USER_A.getName());

        GenericRestClient.HttpResponse response = getAsyncResult(USER_A, asyncId);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("is_running", equalTo(false))));
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(3))))));

        assertThat(deleteAsync(USER_A, asyncId), isOk());
        assertThat(getAsyncResult(USER_A, asyncId), isNotFound());
    }

    @Test
    public void asyncQuery_view_submitAndGet() throws Exception {
        String asyncId = submitAsyncAndGetId(USER_A, "FROM view_allowed | STATS c = COUNT(*)");
        assertResourceOwner(asyncId, USER_A.getName());

        GenericRestClient.HttpResponse response = getAsyncResult(USER_A, asyncId);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(3))))));
    }

    @Test
    public void asyncQuery_rows() throws Exception {
        String asyncId = submitAsyncAndGetId(USER_A, "FROM index_allowed | KEEP product | SORT product");

        GenericRestClient.HttpResponse response = getAsyncResult(USER_A, asyncId);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of("apple"), List.of("pear"), List.of("plum"))))));
    }

    @Test
    public void asyncQuery_forbiddenIndex_isDenied() throws Exception {
        assertThat(submitAsyncAndWait(USER_A, "FROM index_forbidden | STATS c = COUNT(*)"), isForbidden());
    }

    @Test
    public void asyncQuery_viewWithForbiddenSource_isDenied() throws Exception {
        assertThat(submitAsyncAndWait(USER_A, "FROM view_allowed_forbidden_source | STATS c = COUNT(*)"), isForbidden());
    }

    /**
     * A submit with wait_for_completion_timeout=0s returns an async id before the query has been authorized, as the
     * query is authorized while it is running. The denial is then reported by the GET request.
     */
    @Test
    public void asyncQuery_forbiddenIndex_isDeniedOnGet() throws Exception {
        String asyncId = submitAsyncAndGetId(USER_A, "FROM index_forbidden | STATS c = COUNT(*)");
        assertResourceOwner(asyncId, USER_A.getName());

        assertThat(getAsyncResult(USER_A, asyncId), isForbidden());

        // Other users still have no access to the result of that query
        assertThat(getAsyncResult(USER_B, asyncId), isForbidden());
    }

    /**
     * The async result of one user must not be accessible by another user, even though that user has the same
     * privileges. Elasticsearch itself does not protect the result, as its ownership check is inactive without X-Pack
     * security; the check is done by Search Guard.
     */
    @Test
    public void asyncResult_otherUserCannotAccessIt() throws Exception {
        String asyncId = submitAsyncAndGetId(USER_A, "FROM index_allowed | STATS c = COUNT(*)");
        assertResourceOwner(asyncId, USER_A.getName());

        GenericRestClient.HttpResponse response = getAsyncResult(USER_B, asyncId);
        assertThat(response, isForbidden());
        assertThat(response.getBody(), response.getBody(), containsString("is not owned by user " + USER_B.getName()));

        assertThat(stopAsync(USER_B, asyncId), isForbidden());
        assertThat(deleteAsync(USER_B, asyncId), isForbidden());

        // The owner is still able to access the result
        assertThat(getAsyncResult(USER_A, asyncId), isOk());
        assertResourceOwner(asyncId, USER_A.getName());
    }

    @Test
    public void asyncResult_withoutAsyncPrivileges() throws Exception {
        // Submitting works: the privileges of the submit request are those of a regular ES|QL query
        String asyncId = submitAsyncAndGetId(USER_NO_ASYNC, "FROM index_allowed | STATS c = COUNT(*)");
        assertResourceOwner(asyncId, USER_NO_ASYNC.getName());

        assertThat(getAsyncResult(USER_NO_ASYNC, asyncId), isForbidden());
        assertThat(stopAsync(USER_NO_ASYNC, asyncId), isForbidden());
    }

    @Test
    public void asyncResult_allOwnersBypass() throws Exception {
        String asyncId = submitAsyncAndGetId(USER_A, "FROM index_allowed | STATS c = COUNT(*)");

        GenericRestClient.HttpResponse response = getAsyncResult(USER_ALL_OWNERS, asyncId);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(3))))));

        assertThat(deleteAsync(USER_ALL_OWNERS, asyncId), isOk());
    }

    @Test
    public void asyncQuery_stop() throws Exception {
        String asyncId = submitAsyncAndGetId(USER_A, "FROM index_allowed | STATS c = COUNT(*)");

        GenericRestClient.HttpResponse response = stopAsync(USER_A, asyncId);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("is_running", equalTo(false))));
    }

    @Test
    public void asyncResult_unknownId() throws Exception {
        // The owner information cannot be found, thus Search Guard answers with 404 - like Elasticsearch does for
        // async ids which do not exist
        assertThat(getAsyncResult(USER_A, UNKNOWN_ASYNC_ID), isNotFound());
    }

    @Test
    public void syncQuery_doesNotCreateResourceOwnerEntry() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_A)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", "FROM index_allowed | STATS c = COUNT(*)"));
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(3))))));
            assertThat(response.getBody(), response.getBodyAsDocNode().hasNonNull("id"), equalTo(false));
        }
    }
}
