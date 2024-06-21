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
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static org.hamcrest.Matchers.containsInAnyOrder;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * This class tests ignore_unauthorized_index: false together with the legacy non-hidden searchguard index. This will make more queries fail with 403, because they match on the searchguard  index.
 */
public class IgnoreUnauthorizedDisabledWithLegacyIndexIntTest {
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

    static TestIndex index_a1 = TestIndex.name("a1").documentCount(100).seed(1).attr("prefix", "a").build();
    static TestIndex index_a2 = TestIndex.name("a2").documentCount(110).seed(2).attr("prefix", "a").build();
    static TestIndex index_a3 = TestIndex.name("a3").documentCount(120).seed(3).attr("prefix", "a").build();
    static TestIndex index_b1 = TestIndex.name("b1").documentCount(51).seed(4).attr("prefix", "b").build();
    static TestIndex index_b2 = TestIndex.name("b2").documentCount(52).seed(5).attr("prefix", "b").build();
    static TestIndex index_b3 = TestIndex.name("b3").documentCount(53).seed(6).attr("prefix", "b").build();
    static TestIndex index_c1 = TestIndex.name("c1").documentCount(5).seed(7).attr("prefix", "c").build();

    static TestAlias xalias_ab1 = new TestAlias("xalias_ab1", index_a1, index_a2, index_a3, index_b1);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().embedded().singleNode().sslEnabled().ignoreUnauthorizedIndices(false)//
            .configIndexName("searchguard")//
            .users(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_C, LIMITED_USER_D, LIMITED_USER_A_B1, UNLIMITED_USER)//
            .indices(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1)//
            .aliases(xalias_ab1)//
            .build();

    @Test
    public void search_noPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void search_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("_all/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("_all/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_C)) {
            HttpResponse httpResponse = restClient.get("_all/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void search_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("*/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get("*,-searchguard/_search?size=1000");

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("a1", "a2", "a3", "b1", "b2", "b3", "c1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.get("*/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_C)) {
            HttpResponse httpResponse = restClient.get("*/_search?size=1000");

            Assert.assertThat(httpResponse, isForbidden());
        }
    }
}
