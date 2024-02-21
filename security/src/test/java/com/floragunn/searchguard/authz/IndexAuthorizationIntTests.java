/*
 * Copyright 2024 floragunn GmbH
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
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
public class IndexAuthorizationIntTests {
    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("index_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")//
                            .indexPermissions("SGS_CRUD", "SGS_MANAGE_ALIASES").on("index_a*")//
                            .aliasPermissions("SGS_MANAGE_ALIASES").on("z_alias_a*"))//
            .restMatcher(matcher("index_a1", "index_a2", "index_a3"));

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

    static List<TestSgConfig.User> USERS = ImmutableList.of(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_C, LIMITED_USER_D, LIMITED_USER_A_B1,
            UNLIMITED_USER, LIMITED_USER_A_WITHOUT_ANALYZE);

    static TestIndex index_a1 = TestIndex.name("index_a1").documentCount(100).seed(1).attr("prefix", "a").build();
    static TestIndex index_a2 = TestIndex.name("index_a2").documentCount(110).seed(2).attr("prefix", "a").build();
    static TestIndex index_a3 = TestIndex.name("index_a3").documentCount(120).seed(3).attr("prefix", "a").build();
    static TestIndex index_b1 = TestIndex.name("index_b1").documentCount(51).seed(4).attr("prefix", "b").build();
    static TestIndex index_b2 = TestIndex.name("index_b2").documentCount(52).seed(5).attr("prefix", "b").build();
    static TestIndex index_b3 = TestIndex.name("index_b3").documentCount(53).seed(6).attr("prefix", "b").build();
    static TestIndex index_c1 = TestIndex.name("index_c1").documentCount(5).seed(7).attr("prefix", "c").build();

    static TestAlias xalias_ab1 = new TestAlias("xalias_ab1", index_a1, index_a2, index_a3, index_b1);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(USERS)//
            .indices(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1)//
            .aliases(xalias_ab1)//
            .build();

    final TestSgConfig.User user;

    @Test
    public void search_noPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");

            assertThat(httpResponse,
                    user.restMatcher("hits.hits[*]._index", "index_a1", "index_a2", "index_a3", "index_b1", "index_b2", "index_b3", "index_c1"));
        }

    }

    @Parameters(name = "{1}")
    public static Collection<Object[]> params() {
        List<Object[]> result = new ArrayList<>();

        for (TestSgConfig.User user : USERS) {
            result.add(new Object[] { user, user.getDescription() });
        }

        return result;
    }

    public IndexAuthorizationIntTests(TestSgConfig.User user, String description) throws Exception {
        this.user = user;
    }

    private static BiFunction<String, List<String>, Matcher<HttpResponse>> matcher(String... allowedIndices) {
        ImmutableSet<String> allowedIndicesSet = ImmutableSet.ofArray(allowedIndices);
        return (jsonPath, args) -> {
            return matcher(allowedIndicesSet, ImmutableSet.of(args), jsonPath);
        };

    }

    private static Matcher<HttpResponse> matcher(ImmutableSet<String> allowedIndices, ImmutableSet<String> requestedIndices, String jsonPath) {
        DiagnosingMatcher<HttpResponse> statusMatcher = isOk();
        DiagnosingMatcher<HttpResponse> contentMatcher = json(
                distinctNodesAt(jsonPath, containsInAnyOrder(requestedIndices.intersection(allowedIndices))));

        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Got indices " + requestedIndices.intersection(allowedIndices));
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {

                if (!statusMatcher.matches(item)) {
                    statusMatcher.describeMismatch(item, mismatchDescription);
                    return false;
                }

                if (contentMatcher.matches(item)) {
                    return true;
                } else {
                    contentMatcher.describeMismatch(item, mismatchDescription);
                    return false;
                }
            }

        };

    }
}
