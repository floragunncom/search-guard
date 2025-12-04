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

package com.floragunn.signals;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetSocketAddress;
import java.time.Duration;

import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;

@RunWith(Parameterized.class)
public class RemoteClusterWatchTest {

    private static final TestCertificates CERTIFICATES_CONTEXT = TestCertificates.builder().ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard").addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard").build();

    static TestSgConfig.User LIMITED_USER_COORD_A = new TestSgConfig.User("limited_user_A").roles(//
            new TestSgConfig.Role("limited_user_a_role")
                    .clusterPermissions("*")
                    .indexPermissions("indices:*").on("outputcoord")
                    .indexPermissions("indices:data/read/search").on("sourcecoord")
                    .tenantPermission("*", "SGS_KIBANA_ALL_WRITE", "cluster:admin:searchguard:tenant:signals:*")
    );

    static TestSgConfig.User LIMITED_USER_REMOTE_A = new TestSgConfig.User("limited_user_A").roles(//
            new TestSgConfig.Role("limited_user_a_role")
                    .clusterPermissions("*")
                    .indexPermissions("indices:*").on("outputremote")
                    .indexPermissions("indices:data/read/search").on("sourceremote")
                    .tenantPermission("*", "SGS_KIBANA_ALL_WRITE", "cluster:admin:searchguard:tenant:signals:*")
    );

    @ClassRule
    public static LocalCluster.Embedded remoteCluster = new LocalCluster.Builder().singleNode().sslEnabled(CERTIFICATES_CONTEXT)
            .nodeSettings("signals.enterprise.enabled", false)
            .users(LIMITED_USER_REMOTE_A)//
            .authzDebug(true)
            .enableModule(SignalsModule.class).waitForComponents("signals").enterpriseModulesEnabled()
            .embedded().build();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled(CERTIFICATES_CONTEXT)
            .nodeSettings("signals.enterprise.enabled", false)
            .users(LIMITED_USER_COORD_A)//
            .authzDebug(true)
            .enableModule(SignalsModule.class).waitForComponents("signals").enterpriseModulesEnabled()
            .embedded().build();

    @BeforeClass
    public static void setupTestData() throws Exception {

        Client client = cluster.getInternalNodeClient();
        client.index(new IndexRequest("sourcecoord").source(XContentType.JSON, "cluster", "coord", "key1", "val1", "key2", "val2")).actionGet();

        client.index(new IndexRequest("sourcecoord").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "cluster", "coord", "a", "aa", "b", "bb"))
                .actionGet();

        client = remoteCluster.getInternalNodeClient();
        client.index(new IndexRequest("sourceremote").source(XContentType.JSON, "cluster", "remote", "key1", "val1", "key2", "val2")).actionGet();

        client.index(new IndexRequest("sourceremote").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "cluster", "remote", "a", "aa", "b", "bb"))
                .actionGet();
        client.index(new IndexRequest("another-sourceremote").source(XContentType.JSON, "cluster", "remote", "key1", "val1", "key2", "val2")).actionGet();

        client.index(new IndexRequest("another-sourceremote").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "cluster", "remote", "a", "aa", "b", "bb"))
                .actionGet();
    }

    @Before
    public void setUp() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            InetSocketAddress remoteNodeAddress = remoteCluster.getNodeByName("single").getTransportAddress();
            GenericRestClient.HttpResponse response = adminClient.putJson("_cluster/settings", """
                    {
                      "persistent": {
                        "cluster": {
                          "remote": {
                            "my_remote": {
                              "seeds": ["%s:%d"],
                              "skip_unavailable": %b
                            }
                          }
                        }
                      }
                    }
                    """.formatted(remoteNodeAddress.getHostString(), remoteNodeAddress.getPort(), remoteClusterSkipUnavailable));
            assertThat(response, isOk());
        }
    }

    private final boolean remoteClusterSkipUnavailable;

    public RemoteClusterWatchTest(boolean remoteClusterSkipUnavailable) {
        this.remoteClusterSkipUnavailable = remoteClusterSkipUnavailable;
    }

    @Parameterized.Parameters(name = "remoteClusterSkipUnavailable={0}")
    public static Object[] parameters() {
        return new Object[] { true, false };
    }

    @Test
    public void searchLocal_indexToLocal() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {

            //index action - allowed local index
            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("sourcecoord").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("outputcoord").refreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).name("output").build();

            GenericRestClient.HttpResponse response = restClient.postJson("/_signals/watch/_main/_execute", "{\"watch\": " + watch.toJson() + "}");

            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.status.detail", "All actions have been executed"));

            assertNumberOfHitsInIndex(cluster, "outputcoord", 1);
            assertTestsearchInputHitsClusterFieldValueInIndex(cluster, "outputcoord", "coord");
            removeDocsFromIndex(cluster, "outputcoord");

            //index action - not allowed local index
            watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("sourcecoord").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("notallowed").refreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).name("output").build();

            response = restClient.postJson("/_signals/watch/_main/_execute", "{\"watch\": " + watch.toJson() + "}");

            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.status.detail", "All actions failed"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), docNodeSizeEqualTo("$.actions", 1));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.actions[0].name", "output"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.actions[0].error.detail.missing_permissions", "notallowed: indices:data/write/index"));
            assertIndexDoesNotExist(cluster, "notallowed");
        }
    }

    @Test
    public void searchRemote_indexToLocal() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {

            //search input - allowed remote index, index action - allowed local index
            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("my_remote:sourceremote").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("outputcoord").refreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).name("output").build();

            GenericRestClient.HttpResponse response = restClient.postJson("/_signals/watch/_main/_execute", "{\"watch\": " + watch.toJson() + "}");

            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.status.detail", "All actions have been executed"));

            assertNumberOfHitsInIndex(cluster, "outputcoord", 1);
            assertTestsearchInputHitsClusterFieldValueInIndex(cluster, "outputcoord", "remote");
            removeDocsFromIndex(cluster, "outputcoord");

            //search input - allowed remote index, index action - not allowed local index
            watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("my_remote:sourceremote").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("notallowed").refreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).name("output").build();

            response = restClient.postJson("/_signals/watch/_main/_execute", "{\"watch\": " + watch.toJson() + "}");

            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.status.detail", "All actions failed"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), docNodeSizeEqualTo("$.actions", 1));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.actions[0].name", "output"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.actions[0].error.detail.missing_permissions", "notallowed: indices:data/write/index"));
            assertIndexDoesNotExist(cluster, "notallowed");

            //search - not allowed remote index, index action - allowed local index
            watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("my_remote:another-sourceremote").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("outputcoord").refreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).name("output").build();

            response = restClient.postJson("/_signals/watch/_main/_execute", "{\"watch\": " + watch.toJson() + "}");

            if (remoteClusterSkipUnavailable) {
                //index action executed, but testsearch returned 0 hits, remote search failed because of missing permissions
                assertThat(response, isOk());
                assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.status.detail", "All actions have been executed"));
                assertNumberOfHitsInIndex(cluster, "outputcoord", 1);
                try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
                    GenericRestClient.HttpResponse searchResponse = adminClient.get("outputcoord" + "/_search");
                    assertThat(searchResponse, isOk());
                    assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containsValue("$.hits.hits[0]_source.testsearch.hits.total.value", 0));
                    assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().findNodesByJsonPath("$.hits.hits[0]_source.testsearch._clusters.details.my_remote.failures"), hasSize(1));
                    assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containsValue("$.hits.hits[0]_source.testsearch._clusters.details.my_remote.failures[0].reason.missing_permissions", "another-sourceremote: indices:data/read/search"));
                }
                removeDocsFromIndex(cluster, "outputcoord");
            } else {
                //index action not executed, testsearch failed
                assertThat(response.getBody(), response.getStatusCode(), equalTo(422));
                assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.status.detail", "Error while executing SearchInput testsearch: Insufficient permissions"));
                assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.detail.missing_permissions", "another-sourceremote: indices:data/read/search"));
                assertNumberOfHitsInIndex(cluster, "outputcoord", 0);
            }
        }
    }

    @Test
    public void searchLocal_indexToRemote() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            //todo it fails, we cannot simply add doc to `my_remote:outputremote`
            //index action - allowed remote index
            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("sourcecoord").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("my_remote:outputremote").refreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).name("output").build();

            GenericRestClient.HttpResponse response = restClient.postJson("/_signals/watch/_main/_execute", "{\"watch\": " + watch.toJson() + "}");

            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.status.detail", "All actions failed"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), docNodeSizeEqualTo("$.actions", 1));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.actions[0].name", "output"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.actions[0].error.detail.reason", "Invalid index name [my_remote:outputremote], must not contain ':'"));

            assertIndexDoesNotExist(remoteCluster, "outputremote");

            //todo it fails, we cannot simply add doc to `my_remote:notallowed`
            //index action - not allowed remote index
            watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("sourcecoord").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("my_remote:notallowed").refreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).name("output").build();

            response = restClient.postJson("/_signals/watch/_main/_execute", "{\"watch\": " + watch.toJson() + "}");

            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.status.detail", "All actions failed"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), docNodeSizeEqualTo("$.actions", 1));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.actions[0].name", "output"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.actions[0].error.detail.reason", "Invalid index name [my_remote:notallowed], must not contain ':'"));

            assertIndexDoesNotExist(remoteCluster, "notallowed");

        }
    }

    private void assertNumberOfHitsInIndex(LocalCluster.Embedded cluster, String index, int numberOfHits) throws Exception {
        await("number of docs in index " + index + " == " + numberOfHits)
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
                        GenericRestClient.HttpResponse searchResponse = adminClient.get(index + "/_search");
                        assertThat(searchResponse, isOk());
                        assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containsValue("$.hits.total.value", numberOfHits));
                    }
                });
    }

    private void assertTestsearchInputHitsClusterFieldValueInIndex(LocalCluster.Embedded cluster, String index, String expectedClusterFieldValue) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse searchResponse = adminClient.get(index + "/_search");
            assertThat(searchResponse, isOk());
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().findByJsonPath("$.hits.hits[*]._source.testsearch.hits.hits[*]._source.cluster"), everyItem(equalTo(expectedClusterFieldValue)));
        }
    }

    private void removeDocsFromIndex(LocalCluster.Embedded cluster, String index) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse deleteResponse = adminClient.postJson(index + "/_delete_by_query", """
                    {
                      "query": { "match_all": {} }
                    }
                    """);
            assertThat(deleteResponse, isOk());

            GenericRestClient.HttpResponse refreshResponse = adminClient.post(index + "/_refresh");
            assertThat(refreshResponse, isOk());

            GenericRestClient.HttpResponse flushResponse = adminClient.post(index + "/_flush");
            assertThat(flushResponse, isOk());

            assertNumberOfHitsInIndex(cluster, index, 0);

        }
    }

    private void assertIndexDoesNotExist(LocalCluster.Embedded cluster, String index) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse headResponse = adminClient.head(index);
            assertThat(headResponse.getBody(), headResponse, isNotFound());
        }
    }
}
