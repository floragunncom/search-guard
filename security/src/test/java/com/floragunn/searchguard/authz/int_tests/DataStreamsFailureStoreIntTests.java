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
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.valueSatisfiesMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamsFailureStoreIntTests {

    private static final Logger log = LogManager.getLogger(DataStreamsFailureStoreIntTests.class);
    static TestDataStream ds_two_documents = TestDataStream.name("ds_two_documents").documentCount(1).rolloverAfter(10).build();
    static TestDataStream ds_ar1 = TestDataStream.name("ds_ar1").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_ar2 = TestDataStream.name("ds_ar2").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_aw1 = TestDataStream.name("ds_aw1").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_aw2 = TestDataStream.name("ds_aw2").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_br1 = TestDataStream.name("ds_br1").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_br2 = TestDataStream.name("ds_br2").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_bw1 = TestDataStream.name("ds_bw1").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_bw2 = TestDataStream.name("ds_bw2").documentCount(22).rolloverAfter(10).build();
    static TestIndex index_cr1 = TestIndex.name("index_cr1").documentCount(10).build();
    static TestIndex index_cw1 = TestIndex.name("index_cw1").documentCount(10).build();
    static TestDataStream ds_hidden = TestDataStream.name("ds_hidden").documentCount(10).rolloverAfter(3).seed(8).attr("prefix", "h").build();

    static TestAlias alias_ab1r = new TestAlias("alias_ab1r", ds_ar1, ds_ar2, ds_aw1, ds_aw2, ds_br1, ds_bw1);
    static TestAlias alias_ab1w = new TestAlias("alias_ab1w", ds_aw1, ds_aw2, ds_bw1).writeIndex(ds_aw1);
    static TestAlias alias_ab1w_nowriteindex = new TestAlias("alias_ab1w_nowriteindex", ds_aw1, ds_aw2, ds_bw1);

    static TestAlias alias_c1 = new TestAlias("alias_c1", index_cr1);

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("ds_a*")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_a*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_aw*"));

    static TestSgConfig.User LIMITED_USER_B = new TestSgConfig.User("limited_user_B")//
            .description("ds_b*")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*"));

    static TestSgConfig.User LIMITED_USER_B_READ_ONLY_A = new TestSgConfig.User("limited_user_B_read_only_A")//
            .description("ds_b*; read only on ds_a*")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_a*", "ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*"));

    static TestSgConfig.User LIMITED_USER_A_FAILURE_STORE_INDEX = new TestSgConfig.User("limited_to_a_failure_store_index")
            .description("reads only a failure store")//
            .roles(//
                    new TestSgConfig.Role("limited_to_a_failure_store_role")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").on(".fs-ds_a*"));

    static TestSgConfig.User LIMITED_USER_A_DATA_COMPONENT_SELECTOR = new TestSgConfig.User("limited_user_A_data_component_selector")//
            .description("ds_a component selector in SG role*")//
            .roles(//
                    new TestSgConfig.Role("limited_user_A_data_component_selector_role")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("ds_ar1::data"));

    static TestSgConfig.User LIMITED_USER_A_FAILURE_COMPONENT_SELECTOR = new TestSgConfig.User("limited_user_A_failure_component_selector")//
            .description("ds_a failures component selector in SG role*")//
            .roles(//
                    new TestSgConfig.Role("limited_user_A_failure_component_selector_role")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_ar1::failures"));

    static TestSgConfig.User TWO_DOCUMENT_USER = new TestSgConfig.User("two_document_user")//
            .description("Access to data stream ds_two_documents and its faulure store")//
            .roles(//
                    new TestSgConfig.Role("two_document_role")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_two_documents")
                            .indexPermissions("SGS_READ").on(".fs-ds_two_documents*")
            );

    static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")//
            .description("Admin user")//
            .roles(TestSgConfig.Role.ALL_ACCESS);


    static TestSgConfig.User ALL_INDEX_READ_USER = new TestSgConfig.User("all_index_read")//
            .description("All index read")//
            .roles(new TestSgConfig.Role("all_index_read")
                    .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")
                    .indexPermissions("SGS_READ").on("*"));

    static TestSgConfig.User DATA_STREAM_A_AS_INDEX = new TestSgConfig.User("data_stream_a_as_index_user")//
            .description("data_stream_a_as_index_")//
            .roles(new TestSgConfig.Role("data_stream_a_as_index_role")
                    .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")
                    .indexPermissions("SGS_READ").on("ds_aw1*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_B_READ_ONLY_A,
                    LIMITED_USER_A_FAILURE_STORE_INDEX, LIMITED_USER_A_DATA_COMPONENT_SELECTOR, LIMITED_USER_A_FAILURE_COMPONENT_SELECTOR,
                    TWO_DOCUMENT_USER, ADMIN_USER, ALL_INDEX_READ_USER, DATA_STREAM_A_AS_INDEX)//
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .indexTemplates(new TestIndexTemplate("ds_hidden", "ds_hidden*").priority(10).dataStream("hidden", true)
                    .composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .indices(index_cr1, index_cw1)//
            .aliases(alias_ab1w, alias_ab1r, alias_ab1w_nowriteindex, alias_c1)//
            .dataStreams(ds_ar1, ds_ar2, ds_aw1, ds_aw2, ds_br1, ds_br2, ds_bw1, ds_bw2, ds_hidden, ds_two_documents)//
            .authzDebug(true)//
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void setupData() throws Exception {
        try (GenericRestClient aClient = cluster.getRestClient(LIMITED_USER_A);
                GenericRestClient bClient = cluster.getRestClient(LIMITED_USER_B);
                GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            insertValidDoc(aClient, ds_aw1.getName(), 201);
            insertInvalidDoc(aClient, ds_aw1.getName(), 201);
            insertValidDoc(bClient, ds_bw1.getName(), 201);
            insertInvalidDoc(bClient, ds_bw1.getName(), 201);
            insertInvalidDoc(adminClient, ds_two_documents.getName(), 201);
        }
    }

    @Test
    public void shouldNotReturnDocumentsFromFailureStoreInRegularSearch() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(TWO_DOCUMENT_USER)) {
            HttpResponse response = client.get("/" + ds_two_documents.getName() + "/_search?pretty");
            log.info("Generic ds search status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));

            response = client.get("/.fs-ds_two_documents*/_search?pretty");
            log.info("search ds failure store status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));
        }
    }

    @Test
    public void component_selector_and_admin_user() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.get("/" + ds_two_documents.getName() + "::data/_search?pretty");
            log.info("Generic ds search status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));

            response = client.get("/" + ds_two_documents.getName() + "::failures/_search?pretty");
            log.info("search ds failure store status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));
        }
    }

    @Test
    public void component_selector_and_all_index_read_user() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ALL_INDEX_READ_USER)) {
            HttpResponse response = client.get("/" + ds_two_documents.getName() + "::data/_search?pretty");
            log.info("Generic ds search status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));

            response = client.get("/" + ds_two_documents.getName() + "::failures/_search?pretty");
            log.info("search ds failure store status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));
        }
    }

    @Test
    public void read_data_stream_as_index() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DATA_STREAM_A_AS_INDEX)) {
            HttpResponse response = client.get("/" + ds_aw1.getName() + "::data/_search?pretty");
            log.info("DS search with ::data status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",23));

            response = client.get("/" + ds_aw1.getName() + "/_search?pretty");
            log.info("Generic ds search status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isForbidden());

            response = client.get("/" + ds_aw1.getName() + "::failures/_search?pretty");
            log.info("search ds failure store status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));
        }
    }

    @Test
    public void testFailureStore() throws Exception {
        try (GenericRestClient aClient = cluster.getRestClient(LIMITED_USER_A);
                GenericRestClient bClient = cluster.getRestClient(LIMITED_USER_B);
                GenericRestClient bReadAClient = cluster.getRestClient(LIMITED_USER_B_READ_ONLY_A)) {

            HttpResponse response = aClient.get("/" + ds_aw1.getName() + "::failures/_search?size=1000");
            assertThat(response, isForbidden());
//            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));

            response = bReadAClient.get("/" + ds_aw1.getName() + "::failures/_search?size=1000");
            assertThat(response, isForbidden());
//            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));

            response = bReadAClient.get("/" + ds_bw1.getName() + "::failures/_search?size=1000");
            assertThat(response, isForbidden());
//            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));

            response = bClient.get("/" + ds_bw1.getName() + "::failures/_search?size=1000");
            assertThat(response, isForbidden());
//            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));

            response = bClient.get("/" + ds_aw1.getName() + "::failures/_search");
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void testDataComponentSelector() throws Exception {
        try (GenericRestClient aClient = cluster.getRestClient(LIMITED_USER_A);
                GenericRestClient bClient = cluster.getRestClient(LIMITED_USER_B);
                GenericRestClient bReadAClient = cluster.getRestClient(LIMITED_USER_B_READ_ONLY_A)) {

            HttpResponse response = aClient.get("/" + ds_aw1.getName() + "::data/_search?size=1000");
            assertThat(response, isForbidden());
//            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",23));

            response = bReadAClient.get("/" + ds_aw1.getName() + "::data/_search?size=1000");
            assertThat(response, isForbidden());
//            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",23));

            response = bReadAClient.get("/" + ds_bw1.getName() + "::data/_search?size=1000");
            assertThat(response, isForbidden());
//            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",23));

            response = bClient.get("/" + ds_bw1.getName() + "::data/_search?size=1000");
            assertThat(response, isForbidden());
//            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",23));

            response = bClient.get("/" + ds_aw1.getName() + "::data/_search");
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void testDataComponentSelectorRoleWithSelector() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(LIMITED_USER_A_DATA_COMPONENT_SELECTOR)) {

            HttpResponse response = client.get("/" + ds_ar1.getName() + "::data/_search?size=1000");
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void testFailuresComponentSelectorRoleWithSelector() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(LIMITED_USER_A_FAILURE_COMPONENT_SELECTOR)) {

            HttpResponse response = client.get("/" + ds_ar1.getName() + "::failures/_search?size=1000");
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void failureStoreAccessViaIndexPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(LIMITED_USER_A_FAILURE_STORE_INDEX)) {

            HttpResponse response = client.get("/.fs-ds_aw1*/_search?pretty&size=1000");
            assertThat(response, isOk());
            log.info("Response body status code {} and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",1));

            response = client.get("/.fs-ds_bw1*/_search?pretty&size=1000");
            log.info("Response body status code {} and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.hits.total.value",0));// access is lacking
        }
    }

    private static void insertValidDoc(GenericRestClient client, String dataStream, int expectedResponseCode) throws Exception {
        HttpResponse response = client.postJson("/"+ dataStream + "/_doc/?refresh=true", DocNode.of("a", 1, "@timestamp", Instant.now().toString()));
        assertThat(response.getBody(), response.getStatusCode(), equalTo(expectedResponseCode));
        assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("$", "failure_store")));
        assertThat(response.getBody(), response.getBodyAsDocNode(), valueSatisfiesMatcher("$._index", String.class, startsWith(".ds")));
    }

    private static void insertInvalidDoc(GenericRestClient client, String dataStream, int expectedResponseCode) throws Exception {
        HttpResponse response = client.postJson("/"+ dataStream + "/_doc/?refresh=true", DocNode.of("a", 1, "@timestamp", "asd"));
        assertThat(response.getBody(), response.getStatusCode(), equalTo(expectedResponseCode));
        assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.failure_store", "used"));
        assertThat(response.getBody(), response.getBodyAsDocNode(), valueSatisfiesMatcher("$._index", String.class, startsWith(".fs")));
    }

}
