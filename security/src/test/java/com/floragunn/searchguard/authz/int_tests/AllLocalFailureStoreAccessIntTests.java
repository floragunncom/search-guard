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

import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class AllLocalFailureStoreAccessIntTests {

    private static final Logger log = LogManager.getLogger(AllLocalFailureStoreAccessIntTests.class);

    static TestDataStream ds_alpha = TestDataStream.name("ds_alpha")
            .failureStoreEnabled(true)
            .documentCount(3)
            .rolloverAfter(2)
            .failureDocumentCount(3)
            .build();

    static User USER_ALL_ACCESS_NO_CERTS = new User("user_all_access_no_certs")
            .description("User with all access no certs")
            .roles(Role.ALL_ACCESS);

    static User USER_FS_ACCESS = new User("user_fs_access")
            .description("User with access to all local failure stores")
            .roles(
                    new Role("all_local_failure_store_role")
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*", "special:failure_store")
                            .on("*")
            );

    static User USER_LACKING_FS_ACCESS = new User("user_lacking_fs_access")
            .description("User with access to all local failure stores")
            .roles(
                    new Role("all_local_no_failure_store_role")
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*")
                            .on("*")
            );

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(USER_FS_ACCESS, USER_ALL_ACCESS_NO_CERTS, USER_LACKING_FS_ACCESS)
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))
            .dataStreams(ds_alpha)
            .authzDebug(true)
            .enterpriseModulesEnabled()
            .useExternalProcessCluster()
            .build();


    @Test
    public void allStar_standardExpandWildcards_certUser() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            GenericRestClient.HttpResponse response = client.get("/*/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
        }
    }

    @Test
    public void allStar_standardExpandWildcards_allAccessNoCertsUser() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_ALL_ACCESS_NO_CERTS)) {

            GenericRestClient.HttpResponse response = client.get("/*/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
        }
    }

    @Test
    public void allStar_standardExpandWildcards_userWithFsAccess() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_FS_ACCESS)) {

            GenericRestClient.HttpResponse response = client.get("/*/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
        }
    }

    @Test
    public void allStar_standardExpandWildcards_userLackingFsAccess() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_LACKING_FS_ACCESS)) {

            GenericRestClient.HttpResponse response = client.get("/*/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
        }
    }

    @Test
    public void allStarFailures_standardExpandWildcards_certUser() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            GenericRestClient.HttpResponse response = client.get("/*::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha"))))));
        }
    }

    @Test
    public void allStarFailures_standardExpandWildcards_allAccessNoCertsUser() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_ALL_ACCESS_NO_CERTS)) {

            GenericRestClient.HttpResponse response = client.get("/*::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha"))))));
        }
    }

    @Test
    public void allStarFailures_standardExpandWildcards_userWithFsAccess() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_FS_ACCESS)) {

            GenericRestClient.HttpResponse response = client.get("/*::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha"))))));
        }
    }

    @Test
    public void allStarFailures_standardExpandWildcards_userLackingFsAccess() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_LACKING_FS_ACCESS)) {

            GenericRestClient.HttpResponse response = client.get("/*::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha"))))));
        }
    }

    @Test
    public void allLocal_standardExpandWildcards_certUser() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            GenericRestClient.HttpResponse response = client.get("/_all/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
        }
    }

    @Test
    public void allLocal_standardExpandWildcards_allAccessNoCertsUser() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_ALL_ACCESS_NO_CERTS)) {

            GenericRestClient.HttpResponse response = client.get("/_all/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
        }
    }

    @Test
    public void allLocal_standardExpandWildcards_userWithFsAccess() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_FS_ACCESS)) {

            GenericRestClient.HttpResponse response = client.get("/_all/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
        }
    }

    @Test
    public void allLocal_standardExpandWildcards_userLackingFsAccess() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_LACKING_FS_ACCESS)) {

            GenericRestClient.HttpResponse response = client.get("/_all/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
        }
    }

    @Test
    public void allLocalFailures_standardExpandWildcards_certUser() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            GenericRestClient.HttpResponse response = client.get("/_all::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha"))))));
        }
    }

    @Test
    public void allLocalFailures_standardExpandWildcards_allAccessNoCertsUser() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_ALL_ACCESS_NO_CERTS)) {

            GenericRestClient.HttpResponse response = client.get("/_all::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha"))))));
        }
    }

    @Test
    public void allLocalFailures_standardExpandWildcards_userWithFsAccess() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_FS_ACCESS)) {

            GenericRestClient.HttpResponse response = client.get("/_all::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha")))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha"))))));
        }
    }

    @Test
    public void allLocalFailures_standardExpandWildcards_userLackingFsAccess() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_LACKING_FS_ACCESS)) {

            GenericRestClient.HttpResponse response = client.get("/_all::failures/_search?pretty&size=100");

            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha"))))));
            assertThat(response, not(json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha"))))));
        }
    }

//    @Test
//    public void failureStoreSearch_shouldReturnDataFromFailureStoreComponents() throws Exception {
//        try (GenericRestClient client = cluster.getRestClient(USER_FS_ACCESS)) {
//            // Issue a global search request
//            GenericRestClient.HttpResponse response = client.get("/ds_alpha::failures/_search?pretty&size=100&expand_wildcards=all");
//            log.info("Global search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
//            assertThat(response, isOk());
//
//            int expectedTotalDocuments = ds_alpha.getFailureDocumentCount();
//
//            // Verify total hits include both data and failure store documents
//            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value", expectedTotalDocuments));
//
//            // Verify we have hits from the data store backing indices (.ds-ds_alpha-*)
////            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".ds-ds_alpha")))));
//
//            // Verify we have hits from the failure store backing indices (.fs-ds_alpha-*)
//            assertThat(response, json(distinctNodesAt("hits.hits[*]._index", hasItem(startsWith(".fs-ds_alpha")))));
//        }
//    }
}
