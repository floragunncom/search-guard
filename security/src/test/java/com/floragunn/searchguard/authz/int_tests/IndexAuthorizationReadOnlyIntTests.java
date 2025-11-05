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

package com.floragunn.searchguard.authz.int_tests;

import static com.floragunn.searchguard.test.IndexApiMatchers.containsExactly;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedTo;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedToNone;
import static com.floragunn.searchguard.test.IndexApiMatchers.searchGuardIndices;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimited;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimitedIncludingSearchGuardIndices;
import static com.floragunn.searchguard.test.RestMatchers.isBadRequest;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.tasks.Task;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.IndexApiMatchers.IndexMatcher;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestData.TestDocument;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

@RunWith(Parameterized.class)
public class IndexAuthorizationReadOnlyIntTests {
  
    static TestIndex index_a1 = TestIndex.name("index_a1").documentCount(100).seed(1).attr("prefix", "a").build();
    static TestIndex index_a2 = TestIndex.name("index_a2").documentCount(110).seed(2).attr("prefix", "a").build();
    static TestIndex index_a3 = TestIndex.name("index_a3").documentCount(120).seed(3).attr("prefix", "a").build();
    static TestIndex index_ax = TestIndex.name("index_ax").build(); // Not existing index
    static TestIndex index_b1 = TestIndex.name("index_b1").documentCount(51).seed(4).attr("prefix", "b").build();
    static TestIndex index_b2 = TestIndex.name("index_b2").documentCount(52).seed(5).attr("prefix", "b").build();
    static TestIndex index_b3 = TestIndex.name("index_b3").documentCount(53).seed(6).attr("prefix", "b").build();
    static TestIndex index_c1 = TestIndex.name("index_c1").documentCount(5).seed(7).attr("prefix", "c").build();
    static TestIndex index_hidden = TestIndex.name("index_hidden").hidden().documentCount(1).seed(8).attr("prefix", "h").build();
    static TestIndex index_hidden_dot = TestIndex.name(".index_hidden_dot").hidden().documentCount(1).seed(8).attr("prefix", "h").build();
    static TestIndex index_system = TestIndex.name(".index_system").hidden().documentCount(1).seed(8).attr("prefix", "system").build();

    static TestAlias alias_ab1 = new TestAlias("alias_ab1", index_a1, index_a2, index_a3, index_b1);
    static TestAlias alias_c1 = new TestAlias("alias_c1", index_c1);

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("index_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_a*"))//
            .indexMatcher("read", limitedTo(index_a1, index_a2, index_a3, index_ax))//
            .indexMatcher("read_top_level", limitedTo(index_a1, index_a2, index_a3))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B = new TestSgConfig.User("limited_user_B")//
            .description("index_b*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_b*"))//
            .indexMatcher("read", limitedTo(index_b1, index_b2, index_b3))//
            .indexMatcher("read_top_level", limitedTo(index_b1, index_b2, index_b3))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B1 = new TestSgConfig.User("limited_user_B1")//
            .description("index_b1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_b1"))//
            .indexMatcher("read", limitedTo(index_b1))//
            .indexMatcher("read_top_level", limitedTo(index_b1))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_C = new TestSgConfig.User("limited_user_C")//
            .description("index_c*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_c*"))//
            .indexMatcher("read", limitedTo(index_c1))//
            .indexMatcher("read_top_level", limitedTo(index_c1))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_ALIAS_AB1 = new TestSgConfig.User("limited_user_alias_AB1")//
            .description("alias_ab1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/aliases/get").on("alias_ab1*"))//
            .indexMatcher("read", limitedTo(index_a1, index_a2, index_a3, index_b1, alias_ab1))//
            .indexMatcher("read_top_level", limitedTo(alias_ab1))//
            .indexMatcher("get_alias", limitedTo(index_a1, index_a2, index_a3, index_b1, alias_ab1));

    static TestSgConfig.User LIMITED_USER_ALIAS_C1 = new TestSgConfig.User("limited_user_alias_C1")//
            .description("alias_c1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("alias_c1"))//
            .indexMatcher("read", limitedTo(index_c1, alias_c1))//
            .indexMatcher("read_top_level", limitedTo(alias_c1))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_A_HIDDEN = new TestSgConfig.User("limited_user_A_hidden")//
            .description("index_a*, index_hidden*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_a*", "index_hidden*", ".index_hidden*"))//
            .indexMatcher("read", limitedTo(index_a1, index_a2, index_a3, index_ax, index_hidden, index_hidden_dot))//
            .indexMatcher("read_top_level", limitedTo(index_a1, index_a2, index_a3, index_ax, index_hidden, index_hidden_dot))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_A_SYSTEM = new TestSgConfig.User("limited_user_A_system")//
            .description("index_a*, .index_system*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_a*", ".index_system*"))//
            .indexMatcher("read", limitedTo(index_a1, index_a2, index_a3, index_ax, index_system))//
            .indexMatcher("read_top_level", limitedTo(index_a1, index_a2, index_a3, index_ax, index_system))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_NONE = new TestSgConfig.User("limited_user_none")//
            .description("no privileges for existing indices")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_CRUD", "SGS_INDICES_MONITOR").on("index_does_not_exist_*"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("read_top_level", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS = new TestSgConfig.User("invalid_user_index_permissions_for_alias")//
            .description("invalid: index permissions for alias")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("alias_ab1"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("read_top_level", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user")//
            .description("unlimited")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("*").on("*")//
                            .aliasPermissions("*").on("*")

            )//
            .indexMatcher("read", unlimited())//
            .indexMatcher("read_top_level", unlimited())//
            .indexMatcher("get_alias", unlimited());

    /**
     * The SUPER_UNLIMITED_USER authenticates with an admin cert, which will cause all access control code to be skipped.
     * This serves as a base for comparison with the default behavior.
     */
    static TestSgConfig.User SUPER_UNLIMITED_USER = new TestSgConfig.User("super_unlimited_user")//
            .description("super unlimited (admin cert)")//
            .adminCertUser()//
            .indexMatcher("read", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("read_top_level", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("get_alias", unlimitedIncludingSearchGuardIndices());

    static List<TestSgConfig.User> USERS = ImmutableList.of(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_B1, LIMITED_USER_C, LIMITED_USER_ALIAS_AB1,
            LIMITED_USER_ALIAS_C1, LIMITED_USER_A_HIDDEN, LIMITED_USER_A_SYSTEM, LIMITED_USER_NONE, INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS,
            UNLIMITED_USER, SUPER_UNLIMITED_USER);

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled()
        .users(USERS)//
            .indices(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1, index_hidden, index_hidden_dot, index_system)//
            .aliases(alias_ab1, alias_c1)//
            .authzDebug(true)//
            .embedded().plugin(TestSystemIndexPlugin.class)//
            .logRequests()//
            .build();

    static List<GenericRestClient.RequestInfo> executedRequests = new ArrayList<>(1000);

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
        }
    }

    @Test
    public void search_all_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000&expand_wildcards=all");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1, index_hidden,
                    index_hidden_dot, searchGuardIndices()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_all_includeHidden_origin() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000&expand_wildcards=all",
                    new BasicHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "origin-with-allowed-system-indices"));
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1, index_hidden,
                    index_hidden_dot, index_system, searchGuardIndices()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
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
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(404));
        }
    }

    @Test
    public void search_wildcard_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*/_search?size=1000&expand_wildcards=all");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1, index_hidden,
                    index_hidden_dot, searchGuardIndices()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
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
    public void search_staticIndicies_nonExisting() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_ax/_search?size=1000");

            if (containsExactly(index_ax).but(user.indexMatcher("read")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isNotFound());
            }
        }
    }

    @Test
    public void search_staticIndicies_negation() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            // On static indices, negation does not have an effect
            HttpResponse httpResponse = restClient.get("index_a1,index_a2,index_b1,-index_b1/_search?size=1000");

            if (httpResponse.getStatusCode() == 404) {
                // The pecularities of index resolution, chapter 634:
                // A 404 error is also acceptable if we get ES complaining about -index_b1. This will be the case for users with full permissions
                assertThat(httpResponse, json(nodeAt("error.type", equalTo("index_not_found_exception"))));
                assertThat(httpResponse, json(nodeAt("error.reason", containsString("no such index [-index_b1]"))));
            } else {
                assertThat(httpResponse,
                        containsExactly(index_a1, index_a2, index_b1).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")).whenEmpty(403));
            }
        }
    }

    @Test
    public void search_staticIndicies_hidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_hidden/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_hidden).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
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
    public void search_indexPattern_nonExistingIndex_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a*,index_b*,xxx_non_existing/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_noWildcards() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a*,index_b*/_search?size=1000&expand_wildcards=none&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPatternAndStatic_noWildcards() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a*,index_b1/_search?size=1000&expand_wildcards=none&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(index_b1).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPatternAndStatic_negation() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            // If there is a wildcard, negation will also affect indices specified without a wildcard
            HttpResponse httpResponse = restClient.get("index_a*,index_b1,index_b2,-index_b2/_search?size=1000");
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
    public void search_alias_pattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1*/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_a3, index_b1).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_alias_pattern_negation() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_*,-alias_ab1/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
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

            if (user == UNLIMITED_USER || user == SUPER_UNLIMITED_USER) {
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
    public void search_protectedIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/.searchguard/_search");

            if (user == SUPER_UNLIMITED_USER) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }
    
    @Test
    public void search_pit() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {            
            HttpResponse httpResponse = restClient.post("/index_*/_pit?keep_alive=1m");
            assertThat(httpResponse, isOk());
                        
            String pitId = httpResponse.getBodyAsDocNode().getAsString("id");                            
            httpResponse = restClient.postJson("/_search?size=1000", DocNode.of("pit.id", pitId));
            assertThat(httpResponse, isOk());
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));            
        }
    }
    
    @Test
    public void search_pit_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {            
            HttpResponse httpResponse = restClient.post("/_all/_pit?keep_alive=1m");
            assertThat(httpResponse, isOk());
                        
            String pitId = httpResponse.getBodyAsDocNode().getAsString("id");                            
            httpResponse = restClient.postJson("/_search?size=1000", DocNode.of("pit.id", pitId));
            assertThat(httpResponse, isOk());
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));            
        }
    }

    @Test
    public void search_pit_static() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {            
            HttpResponse httpResponse = restClient.post("/index_a1/_pit?keep_alive=1m");
            
            if (containsExactly(index_a1).but(user.indexMatcher("read")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
                return;
            } else {
                assertThat(httpResponse, isOk());                
            }
            
                        
            String pitId = httpResponse.getBodyAsDocNode().getAsString("id");                            
            httpResponse = restClient.postJson("/_search?size=1000", DocNode.of("pit.id", pitId));
            assertThat(httpResponse, containsExactly(index_a1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(403));            
        }
    }
    
    
    @Test
    public void search_pit_wrongIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {            
            HttpResponse httpResponse = restClient.post("/index_a*/_pit?keep_alive=1m");
            assertThat(httpResponse, isOk());
                        
            String pitId = httpResponse.getBodyAsDocNode().getAsString("id");                            
            httpResponse = restClient.postJson("/index_b*/_search?size=1000", DocNode.of("pit.id", pitId));
            assertThat(httpResponse, isBadRequest("error.reason", "*[indices] cannot be used with point in time*"));
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
    public void cat_all_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_cat/indices?format=json&expand_wildcards=all");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1, index_hidden,
                    index_hidden_dot, index_system, searchGuardIndices()).at("$[*].index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void cat_all_includeHidden_origin() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_cat/indices?format=json&expand_wildcards=all",
                    new BasicHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "origin-with-allowed-system-indices"));
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1, index_hidden,
                    index_hidden_dot, index_system, searchGuardIndices()).at("$[*].index").but(user.indexMatcher("read")).whenEmpty(200));
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

    @Test
    public void getAlias_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_alias");
            assertThat(httpResponse,
                    containsExactly(alias_ab1, alias_c1).at("$.*.aliases.keys()").but(user.indexMatcher("get_alias")).whenEmpty(200));
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1, index_hidden,
                    index_hidden_dot, searchGuardIndices()).at("$.keys()").but(user.indexMatcher("get_alias")).whenEmpty(200));
        }
    }

    @Test
    public void getAlias_staticAlias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_alias/alias_c1");
            // RestGetAliasesAction does some further post processing on the results, thus we get 404 errors in case a non wildcard alias was removed
            assertThat(httpResponse, containsExactly(alias_c1).at("$.*.aliases.keys()").but(user.indexMatcher("get_alias")).whenEmpty(404));
            assertThat(httpResponse, containsExactly(index_c1).at("$.keys()").but(user.indexMatcher("get_alias")).whenEmpty(404));
        }
    }

    @Test
    public void getAlias_aliasPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_alias/alias_ab*");
            assertThat(httpResponse, containsExactly(alias_ab1).at("$.*.aliases.keys()").but(user.indexMatcher("get_alias")).whenEmpty(200));
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_a3, index_b1).at("$.keys()").but(user.indexMatcher("get_alias")).whenEmpty(200));
        }
    }

    @Test
    public void getAlias_aliasPattern_noWildcards() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_alias/alias_ab*?expand_wildcards=none");
            assertThat(httpResponse, isOk());
            assertThat(httpResponse.getBodyAsDocNode(), equalTo(DocNode.EMPTY));
        }
    }

    @Test
    public void getAlias_mixed() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_alias/alias_ab1,alias_c*");
            // RestGetAliasesAction does some further post processing on the results, thus we get 404 errors in case a non wildcard alias was removed

            assertThat(httpResponse,
                    containsExactly(alias_ab1, alias_c1).at("$.*.aliases.keys()").but(user.indexMatcher("get_alias")).whenEmpty(404));
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_c1).at("$.keys()")
                    .but(user.indexMatcher("get_alias")).whenEmpty(404));
        }
    }

    @Test
    public void analyze_noIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("_analyze", "{\"text\": \"sample text\"}");

            if (user.indexMatcher("read").isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
            }
        }
    }

    @Test
    public void analyze_staticIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("index_a1/_analyze", "{\"text\": \"sample text\"}");
            IndexMatcher matcher = containsExactly(index_a1).but(user.indexMatcher("read"));

            if (matcher.isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
            }
        }
    }

    @Test
    public void resolve_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/*");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1, alias_ab1, alias_c1)
                    .at("$.*[*].name").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void resolve_wildcard_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/*?expand_wildcards=all");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1, alias_ab1, alias_c1,
                    index_hidden, index_hidden_dot, searchGuardIndices()).at("$.*[*].name").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void resolve_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/index_a*,index_b*");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3).at("$.*[*].name")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3, index_c1).at("indices")
                    .but(user.indexMatcher("read")).whenEmpty(200));
            String product = httpResponse.getHeaderValue("X-elastic-product");
            assertThat(httpResponse.getBody(), product, containsString("search"));
        }
    }

    @Test
    public void field_caps_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_b*/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(index_b1, index_b2, index_b3).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_staticIndices_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a1,index_a2,index_b1/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_b1).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_staticIndices_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a1,index_a2,index_b1/_field_caps?fields=*&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_b1).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_staticIndices_hidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_hidden/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(index_hidden).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_alias_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1/_field_caps?fields=*&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_a3, index_b1).at("indices")
                            .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_alias_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1).at("indices")
                    .butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_aliasPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/alias_ab*/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1)
                    .at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_nonExisting_static() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_ax/_field_caps?fields=*");

            if (containsExactly(index_ax).but(user.indexMatcher("read")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isNotFound());
            }
        }
    }

    @Test
    public void field_caps_nonExisting_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist*/_field_caps?fields=*");

            assertThat(httpResponse, containsExactly().at("indices").whenEmpty(200));
        }
    }

    @Test
    public void field_caps_aliasAndIndex_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1,index_b2/_field_caps?fields=*&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2).at("indices")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_aliasAndIndex_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1,index_b2/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(index_a1, index_a2, index_a3, index_b1, index_b2).at("indices")
                    .butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_staticIndices_negation() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            // On static indices, negation does not have an effect
            HttpResponse httpResponse = restClient.get("index_a1,index_a2,index_b1,-index_b1/_field_caps?fields=*");

            if (httpResponse.getStatusCode() == 404) {
                // The pecularities of index resolution, chapter 634:
                // A 404 error is also acceptable if we get ES complaining about -index_b1. This will be the case for users with full permissions
                assertThat(httpResponse, json(nodeAt("error.type", equalTo("index_not_found_exception"))));
                assertThat(httpResponse, json(nodeAt("error.reason", containsString("no such index [-index_b1]"))));
            } else {
                assertThat(httpResponse,
                        containsExactly(index_a1, index_a2, index_b1).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")).whenEmpty(403));
            }
        }
    }

    @Test
    public void field_caps_indexPattern_minus() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_a*,index_b*,-index_b2,-index_b3/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(index_a1, index_a2, index_a3, index_b1).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
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

    public IndexAuthorizationReadOnlyIntTests(TestSgConfig.User user, String description) throws Exception {
        this.user = user;
    }

    public static class TestSystemIndexPlugin extends Plugin implements SystemIndexPlugin {

        @Override
        public String getFeatureName() {
            return "TestSystemIndexPlugin";
        }

        @Override
        public String getFeatureDescription() {
            return "Plugin that defines system indices";
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            String mappingJson = index_system.getFieldsMappings() //
                    .with(DocNode.of("prefix.type", "text", "prefix.fields.keyword.type", "keyword")) //
                    .toJsonString();
            String mappings = """
                    {
                     	"_doc": {
                     		"_meta": {
                     			"my_test_version": "8.0.0",
                     			"managed_index_mappings_version": 0
                     		},
                     		"properties": "!!mappings-placeholder!!"
                     	}
                     }
                    """.replace("\"!!mappings-placeholder!!\"", mappingJson);
            return ImmutableList.of(//
                    SystemIndexDescriptor.builder()//
                            .setIndexPattern(".index_system*").setDescription("Test system indices")
                            /*
                                The SG plugin for ES used an index of type "EXTERNAL_UNMANAGED" which seems to be correct. However,
                                due to changes introduced in the following commit
                                https://github.com/elastic/elasticsearch/commit/9cf33d74263be44358d9bd059a91ffb0455295f7
                                the test cases "search_all_includeHidden" and "search_all_includeHidden_origin" get the same result,
                                making them useless. Therefore, the index type was changed from "EXTERNAL_UNMANAGED" to "EXTERNAL_MANAGED".
                                Hence, the later test also returns data from the system index in contrast to the other test.
                             */
                            .setType(SystemIndexDescriptor.Type.EXTERNAL_MANAGED)
                            .setVersionMetaKey("my_test_version")
                            .setSettings(Settings.builder().build())
                            .setMappings(mappings)//
                            .setPrimaryIndex(index_system.getName())
                            .setOrigin("origin-with-allowed-system-indices")
                            .setAllowedElasticProductOrigins(ImmutableList.of("origin-with-allowed-system-indices")).setNetNew().build());
        }
    }

}
