/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.authz;

import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestData.TestDocument;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class IgnoreUnauthorizedIntTest {
    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A").roles(//
            new Role("limited_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("a*"));

    static TestSgConfig.User LIMITED_USER_B = new TestSgConfig.User("limited_user_B").roles(//
            new Role("limited_user_b_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("b*"));

    static TestSgConfig.User LIMITED_USER_C = new TestSgConfig.User("limited_user_C").roles(//
            new Role("limited_user_c_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("c*"));

    static TestSgConfig.User LIMITED_USER_D = new TestSgConfig.User("limited_user_D").roles(//
            new Role("limited_user_d_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")
                    .indexPermissions("SGS_CRUD", "indices:admin/refresh", "indices:data/write/delete/byquery").on("d*"));

    static TestSgConfig.User LIMITED_USER_A_B1 = new TestSgConfig.User("limited_user_A_B1").roles(//
            new Role("limited_user_a_b1_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("a*")
                    .indexPermissions("SGS_CRUD").on("b1"));

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user").roles(//
            new Role("unlimited_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("*"));

    static TestSgConfig.User LIMITED_USER_A_WITHOUT_ANALYZE = new TestSgConfig.User("limited_user_A_without_analyze").roles(//
            new Role("limited_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("indices:data/read*").on("a*"));

    static TestIndex index_a1 = TestIndex.name("a1").documentCount(100).seed(1).attr("prefix", "a").build();
    static TestIndex index_a2 = TestIndex.name("a2").documentCount(110).seed(2).attr("prefix", "a").build();
    static TestIndex index_a3 = TestIndex.name("a3").documentCount(120).seed(3).attr("prefix", "a").build();
    static TestIndex index_b1 = TestIndex.name("b1").documentCount(51).seed(4).attr("prefix", "b").build();
    static TestIndex index_b2 = TestIndex.name("b2").documentCount(52).seed(5).attr("prefix", "b").build();
    static TestIndex index_b3 = TestIndex.name("b3").documentCount(53).seed(6).attr("prefix", "b").build();
    static TestIndex index_c1 = TestIndex.name("c1").documentCount(5).seed(7).attr("prefix", "c").build();

    static TestAlias xalias_ab1 = new TestAlias("xalias_ab1", index_a1, index_a2, index_a3, index_b1);

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_C, LIMITED_USER_D, LIMITED_USER_A_B1, UNLIMITED_USER, LIMITED_USER_A_WITHOUT_ANALYZE)//
            .indices(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1)//
            .aliases(xalias_ab1)//
            .embedded().build();

    @Test
    public void search_noPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3", "b1", "b2", "b3", "c1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_B)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("b1", "b2", "b3"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A_B1)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3", "b1"))));
        }
    }

    @Test
    public void search_staticIndicies() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("a1,a2,b1/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "b1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("a1,a2,b1/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_C)) {
            HttpResponse httpResponse = restClient.get("a1,a2,b1/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void search_staticIndicies_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("a1,a2,b1/_search?size=1000&ignore_unavailable=true");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "b1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("a1,a2,b1/_search?size=1000&ignore_unavailable=true");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_C)) {
            HttpResponse httpResponse = restClient.get("a1,a2,b1/_search?size=1000&ignore_unavailable=true");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", empty())));
        }
    }

    @Test
    public void search_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("a*,b*/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3", "b1", "b2", "b3"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("a*,b*/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_C)) {
            HttpResponse httpResponse = restClient.get("a*,b*/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", empty())));
        }
    }

    @Test
    public void search_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("_all/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3", "b1", "b2", "b3", "c1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("_all/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_C)) {
            HttpResponse httpResponse = restClient.get("_all/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("c1"))));
        }
    }

    @Test
    public void search_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("*/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3", "b1", "b2", "b3", "c1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("*/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_C)) {
            HttpResponse httpResponse = restClient.get("*/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("c1"))));
        }
    }

    @Test
    public void search_staticNonExisting() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("ax/_search?size=1000");

            Assert.assertThat(httpResponse, isNotFound());
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_C)) {
            HttpResponse httpResponse = restClient.get("ax/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void search_alias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("xalias_ab1/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3", "b1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("xalias_ab1/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A_B1)) {
            HttpResponse httpResponse = restClient.get("xalias_ab1/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3", "b1"))));
        }
    }

    @Test
    public void msearch_staticIndices_ignoreUnavailable() throws Exception {
        String msearchBody = "{\"index\":\"a1\", \"ignore_unavailable\": true}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n" //
                + "{\"index\":\"a2\", \"ignore_unavailable\": true}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n" //
                + "{\"index\":\"b1\", \"ignore_unavailable\": true}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].hits.hits[*]._index", containsInAnyOrder("a1", "a2", "b1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].hits.hits[*]._index", containsInAnyOrder("a1", "a2"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_B)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].hits.hits[*]._index", containsInAnyOrder("b1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_C)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].hits.hits[*]._index", empty())));
        }
    }

    @Test
    public void msearch_staticIndices_noIgnoreUnavailable() throws Exception {
        String msearchBody = "{\"index\":\"a1\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n" //
                + "{\"index\":\"a2\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].hits.hits[*]._index", containsInAnyOrder("a1", "a2"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_B)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);

            System.out.println(httpResponse.getBody());

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].error.type", containsInAnyOrder("security_exception"))));
        }

    }

    @Test
    public void mget() throws Exception {
        TestDocument testDocumentA1 = index_a1.getTestData().anyDocument();
        TestDocument testDocumentB2 = index_b2.getTestData().anyDocument();

        DocNode mget = DocNode.of("docs",
                Arrays.asList(DocNode.of("_index", "a1", "_id", testDocumentA1.getId()), DocNode.of("_index", "b1", "_id", testDocumentB2.getId())));

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson("/_mget", mget);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("docs[*]._index", containsInAnyOrder("a1", "b1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.postJson("/_mget", mget);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("docs[?(@._index == 'a1')].found", containsInAnyOrder(true))));
            Assert.assertThat(httpResponse, json(distinctNodesAt("docs[?(@._index == 'b1')].error.type", containsInAnyOrder("security_exception"))));
        }

    }

    @Test
    public void deleteByQuery() throws Exception {
        // TODO: Moved over from IntegrationTests.testDeleteByQueryDnfof; however the purpose of this is a bit unclear, as no behaviour specific to DNFOF is tested here

        try (Client client = cluster.getInternalNodeClient()) {
            for (int i = 0; i < 3; i++) {
                client.index(new IndexRequest("d1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                        .actionGet();
            }
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_D)) {
            HttpResponse httpResponse = restClient.postJson("/d*/_delete_by_query?refresh=true&wait_for_completion=true",
                    "{\"query\" : {\"match_all\" : {}}}");

            System.out.println(httpResponse.getBody());
            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("deleted", is(3))));
        }

    }

    @Test
    public void search_termsAggregation_index() throws Exception {

        String aggregationBody = "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":40}}}}";

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.postJson("/_search", aggregationBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("aggregations.indices.buckets[*].key", containsInAnyOrder("a1", "a2", "a3"))));
        }

    }

    @Test
    public void analyze_specificIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.postJson("/a1/_analyze", DocNode.of("text", "foo"));

            Assert.assertThat(httpResponse, isOk());
        }
    }

    @Test
    public void analyze_specificIndex_forbidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.postJson("/b1/_analyze", DocNode.of("text", "foo"));

            Assert.assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void analyze_noIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.postJson("/_analyze", DocNode.of("text", "foo"));

            Assert.assertThat(httpResponse, isOk());
        }
    }
    
    @Test
    public void analyze_noIndex_forbidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A_WITHOUT_ANALYZE)) {
            HttpResponse httpResponse = restClient.postJson("/_analyze", DocNode.of("text", "foo"));

            Assert.assertThat(httpResponse, isForbidden());
        }
    }
}
