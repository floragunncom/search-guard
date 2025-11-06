/*
 * Copyright 2025 floragunn GmbH
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
 *
 */

package com.floragunn.searchguard.authz.int_tests;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;
import java.time.Year;
import java.time.ZoneId;

import static com.floragunn.searchguard.test.IndexApiMatchers.sqlLimitedTo;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class AsyncSqIntTests {

    public static final String ASYNC_SEARCH_ID_PREFIX = "async_search_";
    public static final String MORE_THAN_10_YEARS = "3661d";

    static TestIndex test_index = TestIndex.name("test_index") //
            .documentCount(100) //
            .attr("index_or_ds", "test_index") //
            .seed(2) //
            .build();

    static User USER_1 = new User("user_1")
            .roles(new TestSgConfig.Role("user_1_role").clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor",
                            "indices:data/read/close_point_in_time", "indices:data/read/sql/async/get", "indices:data/read/async_search/delete",
                            "cluster:monitor/xpack/sql/async/status")
                    .indexPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search")
                    .on("test_index")

            );

    static User USER_2 = new User("user_2")
            .roles(new TestSgConfig.Role("user_2_role").clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor",
                            "indices:data/read/close_point_in_time", "indices:data/read/sql/async/get", "indices:data/read/async_search/delete",
                            "cluster:monitor/xpack/sql/async/status")
                    .indexPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search")
                    .on("test_index")

            );
    
    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder() //
            .singleNode() //
            .sslEnabled() //
            .users(USER_1, USER_2) //
            .indices(test_index) //
            .authzDebug(true) //
            .useExternalProcessCluster() //
            .enterpriseModulesEnabled() //
            .build();

    @Test
    public void queryAsyncExecution_success() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(USER_1); GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            DocNode body = DocNode.of("query", "SELECT * FROM test_index", "wait_for_completion_timeout", "0s");

            // execute async search
            GenericRestClient.HttpResponse response = restClient.postSql(body);

            assertThat(response, isOk());
            String asyncSearchId = response.getBodyAsDocNode().getAsString("id");
            assertThat(asyncSearchId, notNullValue());
            String indexId = ASYNC_SEARCH_ID_PREFIX + asyncSearchId;
            response = adminClient.get("/.searchguard_resource_owner/_doc/" + indexId);
            assertThat(response, isOk());
            Number expires = response.getBodyAsDocNode().getAsNode("_source").getNumber("expires");
            // default expiration is set
            assertThat(expires, notNullValue());
            assertThat(expires.longValue(), Matchers.greaterThan(0L)); // the test uses default value

            // fetch async search results
            response = restClient.get("/_sql/async/" + asyncSearchId + "?wait_for_completion_timeout=30s&format=json");
            assertThat(response, sqlLimitedTo(test_index));
        }
    }

    @Test
    public void queryAsyncExecution_customTimeout() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(USER_1); GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            DocNode body = DocNode.of("query", "SELECT * FROM test_index", "wait_for_completion_timeout", "0s", "keep_alive", MORE_THAN_10_YEARS);

            // execute async search
            GenericRestClient.HttpResponse response = restClient.postSql(body);

            assertThat(response, isOk());
            String asyncSearchId = response.getBodyAsDocNode().getAsString("id");
            assertThat(asyncSearchId, notNullValue());

            String indexId = ASYNC_SEARCH_ID_PREFIX + asyncSearchId;
            response = adminClient.get("/.searchguard_resource_owner/_doc/" + indexId);
            assertThat(response, isOk());
            Number expires = response.getBodyAsDocNode().getAsNode("_source").getNumber("expires");
            // default expiration is set
            assertThat(expires, notNullValue());
            long expirationMillis = expires.longValue();

            // Extract year from timestamp and verify it's 10 or more years in the future
            int expirationYear = Instant.ofEpochMilli(expirationMillis).atZone(ZoneId.systemDefault()).getYear();
            int tenYearsFromNow = Year.now().getValue() + 10;
            assertThat(expirationYear, Matchers.greaterThanOrEqualTo(tenYearsFromNow));

            // fetch async search results
            response = restClient.get("/_sql/async/" + asyncSearchId + "?wait_for_completion_timeout=30s&format=json");
            assertThat(response, sqlLimitedTo(test_index));
        }
    }

    @Test
    public void queryAsyncExecution_failure() throws Exception {
        String asyncSearchId = null;
        try (GenericRestClient restClient = cluster.getRestClient(USER_2)) {
            DocNode body = DocNode.of("query", "SELECT * FROM test_index", "wait_for_completion_timeout", "0s");

            // execute async search
            GenericRestClient.HttpResponse response = restClient.postSql(body);

            assertThat(response, isOk());
            asyncSearchId = response.getBodyAsDocNode().getAsString("id");
            assertThat(asyncSearchId, notNullValue());
            assertResourceExistsAndBelongsToUser(asyncSearchId, USER_2.getName());
        }

        try (GenericRestClient restClient = cluster.getRestClient(USER_1)) {
            // fetch async search results
            GenericRestClient.HttpResponse response = restClient.get("/_sql/async/" + asyncSearchId + "?wait_for_completion_timeout=30s&format=json");
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void queryAsyncStatus_success() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(USER_1)) {
            DocNode body = DocNode.of("query", "SELECT * FROM test_index", "wait_for_completion_timeout", "0s");

            // execute async search
            GenericRestClient.HttpResponse response = restClient.postSql(body);

            assertThat(response, isOk());
            String asyncSearchId = response.getBodyAsDocNode().getAsString("id");
            assertThat(asyncSearchId, notNullValue());

            // search status
            response = restClient.get("/_sql/async/status/" + asyncSearchId);
            assertThat(response, isOk());
        }
    }

    @Test
    public void queryAsyncStatus_failure() throws Exception {
        String asyncSearchId = null;
        try (GenericRestClient restClient = cluster.getRestClient(USER_2)) {
            DocNode body = DocNode.of("query", "SELECT * FROM test_index", "wait_for_completion_timeout", "0s");

            // execute async search
            GenericRestClient.HttpResponse response = restClient.postSql(body);

            assertThat(response, isOk());
            asyncSearchId = response.getBodyAsDocNode().getAsString("id");
            assertThat(asyncSearchId, notNullValue());
            assertResourceExistsAndBelongsToUser(asyncSearchId, USER_2.getName());
        }
        try (GenericRestClient restClient = cluster.getRestClient(USER_1)) {

            // search status
            GenericRestClient.HttpResponse response = restClient.get("/_sql/async/status/" + asyncSearchId);
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void queryAsyncResultDeletion_success() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(USER_1)) {
            DocNode body = DocNode.of("query", "SELECT * FROM test_index", "wait_for_completion_timeout", "0s");

            // execute async search
            GenericRestClient.HttpResponse response = restClient.postSql(body);

            assertThat(response, isOk());
            String asyncSearchId = response.getBodyAsDocNode().getAsString("id");
            assertThat(asyncSearchId, notNullValue());

            // delete async search results
            response = restClient.delete("/_sql/async/delete/" + asyncSearchId);
            assertThat(response, isOk());
        }
    }

    @Test
    public void queryAsyncResultDeletion_failure() throws Exception {
        String asyncSearchId = null;
        try (GenericRestClient restClient = cluster.getRestClient(USER_2)) {
            DocNode body = DocNode.of("query", "SELECT * FROM test_index", "wait_for_completion_timeout", "0s");

            // execute async search
            GenericRestClient.HttpResponse response = restClient.postSql(body);

            assertThat(response, isOk());
            asyncSearchId = response.getBodyAsDocNode().getAsString("id");
            assertThat(asyncSearchId, notNullValue());
            assertResourceExistsAndBelongsToUser(asyncSearchId, USER_2.getName());
        }
        try (GenericRestClient restClient = cluster.getRestClient(USER_1)) {
            assertThat(asyncSearchId, notNullValue());

            // delete async search results
            GenericRestClient.HttpResponse response = restClient.delete("/_sql/async/delete/" + asyncSearchId);

            assertThat(response, isForbidden());
        }
    }

    private static void assertResourceExistsAndBelongsToUser(String asyncSearchId, String expectedUserName) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response;
            String indexId = ASYNC_SEARCH_ID_PREFIX + asyncSearchId;
            response = adminClient.get("/.searchguard_resource_owner/_doc/" + indexId);
            assertThat(response, isOk());
            String username = response.getBodyAsDocNode().getAsNode("_source").getAsString("user_name");

            assertThat(username, equalTo(expectedUserName));
        }
    }

}
