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
 *
 */
package com.floragunn.searchguard.authz.int_tests;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.floragunn.searchguard.test.IndexApiMatchers.containsExactly;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedTo;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedToNone;
import static com.floragunn.searchguard.test.IndexApiMatchers.searchGuardIndices;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class AllLocalFailureStoreAccessIntTests {

    private static final Logger log = LogManager.getLogger(AllLocalFailureStoreAccessIntTests.class);

    public static final String MATCHER_FAILURE_STORE_ONLY = "matcher-failure-store-only";
    public static final String MATCHER_DATA_ONLY = "matcher-data-only";
    public static final String MATCHER_ALL_WITH_EXPAND_WILDCARDS = "matcher-all-with-expand-wildcards";

    static TestDataStream ds_alpha = TestDataStream.name("ds_alpha")
            .failureStoreEnabled(true)
            .documentCount(3)
            .rolloverAfter(2)
            .failureDocumentCount(3)
            .build();

    static User ADMIN_CERT_USER = new User("admin_cert_user")
            .description("admin_cert_user - admin cert user")
            .adminCertUser()
            .indexMatcher(MATCHER_FAILURE_STORE_ONLY, containsExactly(ds_alpha.failureStore().orElseThrow()).at("hits.hits[*]._index"))
            .indexMatcher(MATCHER_DATA_ONLY, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"))
            .indexMatcher(MATCHER_ALL_WITH_EXPAND_WILDCARDS, containsExactly(ds_alpha, searchGuardIndices()).at("hits.hits[*]._index"));

    static User USER_ALL_ACCESS_NO_CERTS = new User("user_all_access_no_certs")
            .description("user_all_access_no_certs - user with all access no certs")
            .roles(Role.ALL_ACCESS)
            .indexMatcher(MATCHER_FAILURE_STORE_ONLY, containsExactly(ds_alpha.failureStore().orElseThrow()).at("hits.hits[*]._index"))
            .indexMatcher(MATCHER_DATA_ONLY, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"))
            // todo COMPONENT SELECTORS: this is unexpected. Should be the same as in case of ADMIN_CERT_USER but without searchGuardIndices
            // (failure store indices should be included)
            .indexMatcher(MATCHER_ALL_WITH_EXPAND_WILDCARDS, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"));


    static User USER_FS_ACCESS_DS_LEVEL = new User("user_fs_access_ds_level")
            .description("user_fs_access_ds_level - user with access to all local failure stores on data stream level")
            .roles(
                    new Role("all_local_failure_store_role_ds_level")
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*", "special:failure_store")
                            .on("*")
            )
            .indexMatcher(MATCHER_FAILURE_STORE_ONLY, containsExactly(ds_alpha.failureStore().orElseThrow()).at("hits.hits[*]._index"))
            .indexMatcher(MATCHER_DATA_ONLY, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"))
            // todo COMPONENT SELECTORS: this is unexpected. Should be the same as in case of ADMIN_CERT_USER but without searchGuardIndices
            // (failure store indices should be included)
            .indexMatcher(MATCHER_ALL_WITH_EXPAND_WILDCARDS, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"));

    static User USER_LACKING_FS_ACCESS_DS_LEVEL = new User("user_lacking_fs_access_ds_level")
            .description("user_lacking_fs_access_ds_level - user with lacking access to all local failure stores on data stream level")
            .roles(
                    new Role("all_local_no_failure_store_role_ds_level")
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*")
                            .on("*")
            )
            .indexMatcher(MATCHER_FAILURE_STORE_ONLY, limitedToNone().at("hits.hits[*]._index"))
            .indexMatcher(MATCHER_DATA_ONLY, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"))
            .indexMatcher(MATCHER_ALL_WITH_EXPAND_WILDCARDS, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"));

    static User USER_LACKING_FS_ACCESS_INDEX_LEVEL = new User("user_lacking_fs_access_index_level")
            .description("user_lacking_fs_access_index_level - user with lacking access to all local failure stores on index level")
            .roles(
                    new Role("all_local_no_failure_store_role_index_level")
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*")
                            .on("*")
            )
            .indexMatcher(MATCHER_FAILURE_STORE_ONLY, limitedToNone().at("hits.hits[*]._index"))
            .indexMatcher(MATCHER_DATA_ONLY, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"))
            .indexMatcher(MATCHER_ALL_WITH_EXPAND_WILDCARDS, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"));

    static User USER_FS_ACCESS_INDEX_LEVEL = new User("user_fs_access_index_level")
            .description("user_fs_access_index_level - user with access to all local failure stores on index level")
            .roles(
                    new Role("all_local_failure_store_role_index_level")
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*", "special:failure_store")
                            .on("*")
            )
            .indexMatcher(MATCHER_FAILURE_STORE_ONLY, containsExactly(ds_alpha.failureStore().orElseThrow()).at("hits.hits[*]._index"))
            .indexMatcher(MATCHER_DATA_ONLY, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"))
            // todo COMPONENT SELECTORS - This user should have access to the failure store indices when wildcard expansion is enabled.
            .indexMatcher(MATCHER_ALL_WITH_EXPAND_WILDCARDS, containsExactly(ds_alpha.dataOnly()).at("hits.hits[*]._index"));

    static List<User> USERS = ImmutableList.of(
            ADMIN_CERT_USER, USER_ALL_ACCESS_NO_CERTS, USER_FS_ACCESS_DS_LEVEL,
            USER_LACKING_FS_ACCESS_DS_LEVEL, USER_FS_ACCESS_INDEX_LEVEL,
            USER_LACKING_FS_ACCESS_INDEX_LEVEL);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(USERS)
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))
            .dataStreams(ds_alpha)
            .authzDebug(true)
            .enterpriseModulesEnabled()
            .useExternalProcessCluster()
            .build();

    @Parameters(name = "{1}")
    public static Collection<Object[]> params() {
        List<Object[]> result = new ArrayList<>();

        for (User user : USERS) {
            result.add(new Object[] { user, user.getDescription() });
        }

        return result;
    }

    final User user;

    public AllLocalFailureStoreAccessIntTests(User user, String description) {
        this.user = user;
    }

    @Test
    public void allStar_standardExpandWildcards() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/*/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_DATA_ONLY));
        }
    }

    @Test
    public void allStarFailures_standardExpandWildcards() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/*::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_FAILURE_STORE_ONLY));
        }
    }

    @Test
    public void allLocal_standardExpandWildcards() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/_all/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_DATA_ONLY));
        }
    }

    @Test
    public void allLocalFailures_standardExpandWildcards() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/_all::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_FAILURE_STORE_ONLY));
        }
    }

    @Test
    public void allStar_expandWildcardsAll() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/*/_search?pretty&size=100&expand_wildcards=all");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_ALL_WITH_EXPAND_WILDCARDS));
        }
    }

    @Test
    public void allStarFailures_expandWildcardsAll() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/*::failures/_search?pretty&size=100&expand_wildcards=all");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_FAILURE_STORE_ONLY));
        }
    }

    @Test
    public void allLocal_expandWildcardsAll() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/_all/_search?pretty&size=100&expand_wildcards=all");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_ALL_WITH_EXPAND_WILDCARDS));
        }
    }

    @Test
    public void omitIndexExpression_standardExpandWildcards() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_DATA_ONLY));
        }
    }

    @Test
    public void omitIndexExpression_expandWildcardsAll() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/_search?pretty&size=100&expand_wildcards=all");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_ALL_WITH_EXPAND_WILDCARDS));
        }
    }

    @Test
    public void allLocalFailures_expandWildcardsAll() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {

            GenericRestClient.HttpResponse response = client.get("/_all::failures/_search?pretty&size=100&expand_wildcards=all");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, user.indexMatcher(MATCHER_FAILURE_STORE_ONLY));
        }
    }
}
