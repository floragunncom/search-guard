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

import static com.floragunn.searchguard.test.IndexApiMatchers.containsExactly;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedTo;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimited;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestData.TestDocument;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

@RunWith(Parameterized.class)
public class IndexAuthorizationIntTests {
    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    static TestIndex index_a1 = TestIndex.name("index_a1").documentCount(100).seed(1).attr("prefix", "a").build();
    static TestIndex index_a2 = TestIndex.name("index_a2").documentCount(110).seed(2).attr("prefix", "a").build();
    static TestIndex index_a3 = TestIndex.name("index_a3").documentCount(120).seed(3).attr("prefix", "a").build();
    static TestIndex index_b1 = TestIndex.name("index_b1").documentCount(51).seed(4).attr("prefix", "b").build();
    static TestIndex index_b2 = TestIndex.name("index_b2").documentCount(52).seed(5).attr("prefix", "b").build();
    static TestIndex index_b3 = TestIndex.name("index_b3").documentCount(53).seed(6).attr("prefix", "b").build();
    static TestIndex index_c1 = TestIndex.name("index_c1").documentCount(5).seed(7).attr("prefix", "c").build();
    static TestAlias alias_ab1 = new TestAlias("alias_ab1", index_a1, index_a2, index_a3, index_b1);
    static TestAlias alias_c1 = new TestAlias("alias_c1", index_c1);

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("index_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_a*")//
                            .aliasPermissions("SGS_MANAGE_ALIASES").on("z_alias_a*"))//
            .indexMatcher("read", limitedTo(index_a1, index_a2, index_a3));

    static TestSgConfig.User LIMITED_USER_B = new TestSgConfig.User("limited_user_B")//
            .description("index_b*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_b*"))//
            .indexMatcher("read", limitedTo(index_b1, index_b2, index_b3));

    static TestSgConfig.User LIMITED_USER_B1 = new TestSgConfig.User("limited_user_B1")//
            .description("index_b1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_b1"))//
            .indexMatcher("read", limitedTo(index_b1));

    static TestSgConfig.User LIMITED_USER_C = new TestSgConfig.User("limited_user_C")//
            .description("index_c*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_c*"))//
            .indexMatcher("read", limitedTo(index_c1));

    static TestSgConfig.User LIMITED_USER_ALIAS_AB1 = new TestSgConfig.User("limited_user_alias_AB1")//
            .description("alias_ab1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("alias_ab1*"))//
            .indexMatcher("read", limitedTo(index_a1, index_a2, index_a3, index_b1));

    static TestSgConfig.User LIMITED_USER_ALIAS_C1 = new TestSgConfig.User("limited_user_alias_C1")//
            .description("alias_c1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("alias_c1"))//
            .indexMatcher("read", limitedTo(index_c1));

    static TestSgConfig.User LIMITED_USER_NONE = new TestSgConfig.User("limited_user_none")//
            .description("no privileges for existing indices")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_CRUD", "SGS_INDICES_MONITOR").on("index_does_not_exist_*"))//
            .indexMatcher("read", limitedTo());

    static TestSgConfig.User INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS = new TestSgConfig.User("invalid_user_index_permissions_for_alias")//
            .description("invalid: index permissions for alias")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("alias_ab1"))//
            .indexMatcher("read", limitedTo());

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user")//
            .description("unlimited")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("*").on("*")//
                            .aliasPermissions("*").on("*")

            )//
            .indexMatcher("read", unlimited());

    /*
    static TestSgConfig.User LIMITED_USER_D = new TestSgConfig.User("limited_user_D").roles(//
            new Role("limited_user_d_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")
                    .indexPermissions("SGS_CRUD", "indices:admin/refresh", "indices:data/write/delete/byquery").on("d*"));
    
    static TestSgConfig.User LIMITED_USER_A_B1 = new TestSgConfig.User("limited_user_A_B1").roles(//
            new Role("limited_user_a_b1_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("a*")
                    .indexPermissions("SGS_CRUD").on("b1"));
    */

    /*
    static TestSgConfig.User LIMITED_USER_A_WITHOUT_ANALYZE = new TestSgConfig.User("limited_user_A_without_analyze").roles(//
            new Role("limited_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("indices:data/read*").on("a*"));
    */
    static List<TestSgConfig.User> USERS = ImmutableList.of(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_B1, LIMITED_USER_C, LIMITED_USER_ALIAS_C1,
            LIMITED_USER_NONE, INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS, UNLIMITED_USER);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(USERS)//
            .indices(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1)//
            .aliases(alias_ab1, alias_c1)//
            .authzDebug(true)//
            .build();

    final TestSgConfig.User user;

    @Test
    public void search_noPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_noPattern_noWildcards() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000&expand_wildcards=none");
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(200));
        }
    }

    @Test
    public void search_noPattern_allowNoIndicesFalse() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000&allow_no_indices=false");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(404));
        }
    }

    @Test
    public void search_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_all_noWildcards() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000&expand_wildcards=none");
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(200));

            System.out.println(httpResponse.getBody());

        }
    }

    @Test
    public void search_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_wildcard_noWildcards() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*/_search?size=1000&expand_wildcards=none");
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(200));

            System.out.println(httpResponse.getBody());

        }
    }

    @Test
    public void search_staticIndicies_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a1,index_a2,index_b1/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_b1).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_staticIndicies_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a1,index_a2,index_b1/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_b1).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a*,index_b*/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_minus() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a*,index_b*,-index_b2,-index_b3/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_a3, index_b1).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_alias_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_a3, index_b1).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_alias_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1).at("hits.hits[*]._index")
                    .butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_aliasPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1*/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_a3, index_b1).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_aliasAndIndex_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1,index_b2/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_aliasAndIndex_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1,index_b2/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2).at("hits.hits[*]._index")
                    .butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_nonExisting_static() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist/_search?size=1000");

            if (user.getName().equals("unlimited_user")) {
                assertThat(httpResponse, isNotFound());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void search_nonExisting_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist*/_search?size=1000");

            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(200));
        }
    }

    @Test
    public void search_termsAggregation_index() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/_search",
                    "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":1000}}}}");

            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1)
                    .at("aggregations.indices.buckets[*].key").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void msearch_staticIndices() throws Exception {
        String msearchBody = "{\"index\":\"index_b1\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n" //
                + "{\"index\":\"index_b2\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);
            assertThat(httpResponse,
                    containsExactly(index_b1, index_b2).at("responses[*].hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void mget() throws Exception {
        TestDocument testDocumentA1 = index_a1.getTestData().anyDocument();
        TestDocument testDocumentB1 = index_b1.getTestData().anyDocument();
        TestDocument testDocumentB2 = index_b2.getTestData().anyDocument();

        DocNode mget = DocNode.of("docs", DocNode.array(//
                DocNode.of("_index", "index_a1", "_id", testDocumentA1.getId()), //
                DocNode.of("_index", "index_b1", "_id", testDocumentB1.getId()), //
                DocNode.of("_index", "index_b2", "_id", testDocumentB2.getId())));

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/_mget", mget);
            assertThat(httpResponse, containsExactly(index_a1, index_b1, index_b2).at("docs[?(@.found == true)]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void mget_alias() throws Exception {
        TestDocument testDocumentC1a = index_c1.getTestData().anyDocument();
        TestDocument testDocumentC1b = index_c1.getTestData().anyDocument();

        DocNode mget = DocNode.of("docs", DocNode.array(//
                DocNode.of("_index", "alias_c1", "_id", testDocumentC1a.getId()), //
                DocNode.of("_index", "alias_c1", "_id", testDocumentC1b.getId())));

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/_mget", mget);
            assertThat(httpResponse, containsExactly(index_c1).at("docs[?(@.found == true)]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void get() throws Exception {
        TestDocument testDocumentB1 = index_b1.getTestData().anyDocument();

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/index_b1/_doc/" + testDocumentB1.getId());
            assertThat(httpResponse, containsExactly(index_b1).at("_index").but(user.indexMatcher("read")).whenEmpty(403));
        }
    }

    @Test
    public void get_alias() throws Exception {
        TestDocument testDocumentC1 = index_c1.getTestData().anyDocument();

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/alias_c1/_doc/" + testDocumentC1.getId());
            assertThat(httpResponse, containsExactly(index_c1).at("_index").but(user.indexMatcher("read")).whenEmpty(403));
        }
    }

    @Test
    public void cat_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_cat/indices?format=json");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1).at("$[*].index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void cat_pattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_cat/indices/index_a*?format=json");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3).at("$[*].index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void index_stats_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_stats");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1).at("indices.keys()")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void index_stats_pattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_b*/_stats");
            assertThat(httpResponse,
                    containsExactly(index_b1, index_b2, index_b3).at("indices.keys()").but(user.indexMatcher("read")).whenEmpty(200));
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

}
