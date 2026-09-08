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

public class EsqlViewIntTest {

    static TestSgConfig.User USER_NO_PERMISSIONS = new TestSgConfig.User("esql_view_no_permissions")
            .roles(new TestSgConfig.Role("esql_view_no_permissions").clusterPermissions().indexPermissions().on().aliasPermissions().on()
                    .dataStreamPermissions().on());

    static TestSgConfig.User USER_WITH_PERMISSIONS = new TestSgConfig.User("esql_view_with_permissions")
            .roles(new TestSgConfig.Role("esql_view_with_permissions").clusterPermissions().indexPermissions("*").on("index_allowed*")
                    .aliasPermissions().on().dataStreamPermissions().on());

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(USER_NO_PERMISSIONS, USER_WITH_PERMISSIONS)
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

            // At the moment, the request cannot be resolved, so the request is rejected
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
