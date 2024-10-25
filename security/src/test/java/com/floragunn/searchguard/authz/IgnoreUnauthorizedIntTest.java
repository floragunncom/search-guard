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
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.test.helper.PitHolder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import com.floragunn.codova.documents.DocNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * TODO remove me altogether, take care of PIT
 *
 */
public class IgnoreUnauthorizedIntTest {
    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();


    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A").roles(//
            new Role("limited_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD", "SGS_MANAGE_ALIASES")
                    .on("a*").aliasPermissions("SGS_MANAGE_ALIASES").on("z_alias_a*"));

    static TestSgConfig.User LIMITED_USER_B = new TestSgConfig.User("limited_user_B").roles(//
            new Role("limited_user_b_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD", "SGS_MANAGE_ALIASES")
                    .on("b*"));

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

    static NamedWriteableRegistry nameRegistry;

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_C, LIMITED_USER_D, LIMITED_USER_A_B1, UNLIMITED_USER, LIMITED_USER_A_WITHOUT_ANALYZE)//
            .indices(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1)//
            .aliases(xalias_ab1)//
            .embedded().build();

    @BeforeClass
    public static void beforeClass() {
        nameRegistry = cluster.getInjectable(NamedWriteableRegistry.class);
    }

    @Test
    public void createAlias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A).trackResources()) {
            HttpResponse httpResponse = restClient.put("/a1,a2/_alias/z_alias_xxx");

            Assert.assertThat(httpResponse, isForbidden());

            httpResponse = restClient.put("/a1,a2/_alias/z_alias_a12");

            Assert.assertThat(httpResponse, isOk());

            //httpResponse = restClient.put("/a3,z_alias_a12/_alias/z_alias_aa12");

            //Assert.assertThat(httpResponse, isOk());

            httpResponse = restClient.put("/a1,a2,b1/_alias/z_alias_a12b1");

            Assert.assertThat(httpResponse, isForbidden());

            httpResponse = restClient.get("/_alias/z_alias_a12");

            Assert.assertThat(httpResponse, isOk());

            try (GenericRestClient restClient2 = cluster.getRestClient(LIMITED_USER_B)) {
                httpResponse = restClient2.delete("/b1/_alias/z_alias_a12");

                Assert.assertThat(httpResponse, isForbidden());

                httpResponse = restClient2.get("/_alias/z_alias_a12");

                Assert.assertThat(httpResponse, isNotFound());
            }
        }

    }

    @Test
    public void createAlias2() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A).trackResources()) {
            HttpResponse httpResponse = restClient.put("/a99/_alias/z_alias_xxx");
            // httpResponse = restClient.put("/a98/_alias/z_alias_a98");

            //Assert.assertThat(httpResponse, isOk());

        }

    }

    @Test
    public void search_termsAggregation_index_withPit() throws Exception {
        String aggregationBody = "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":40}}}}";

        try (
            GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A);
            PitHolder pitHolder = PitHolder.of(restClient).post("/_all/_pit?keep_alive=1m")) {
            DocNode searchWithAggregationAndPitId = DocNode.of("pit.id", pitHolder.getPitId()) //
                .with(DocNode.parse(Format.JSON).from(aggregationBody));
            HttpResponse httpResponse = restClient.postJson("/_search", searchWithAggregationAndPitId);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(
                httpResponse,
                json(distinctNodesAt("aggregations.indices.buckets[*].key", containsInAnyOrder("a1", "a2", "a3"))));
            // test contract with ES - indices name are expected, not the '_all' marker
            Assert.assertThat(pitHolder.extractIndicesFromPit(nameRegistry), arrayContainingInAnyOrder("a1", "a2", "a3"));
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
