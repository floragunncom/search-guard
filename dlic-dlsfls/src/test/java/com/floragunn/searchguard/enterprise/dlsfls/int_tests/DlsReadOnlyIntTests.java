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

package com.floragunn.searchguard.enterprise.dlsfls.int_tests;

import static com.floragunn.searchguard.test.IndexApiMatchers.containsExactly;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedTo;
import static com.floragunn.searchguard.test.IndexApiMatchers.searchGuardIndices;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimited;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimitedIncludingSearchGuardIndices;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isInternalServerError;
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestIndexLike;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestData.TestDocument;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

@RunWith(Parameterized.class)
public class DlsReadOnlyIntTests {
    static TestIndex index_1 = TestIndex.name("index_1").documentCount(100).seed(1).attr("prefix", "a").setting("index.number_of_shards", 5).build();
    static TestIndex index_2 = TestIndex.name("index_2").documentCount(110).seed(2).attr("prefix", "a").setting("index.number_of_shards", 5).build();
    static TestIndex index_3 = TestIndex.name("index_3").documentCount(51).seed(4).attr("prefix", "b").setting("index.number_of_shards", 5).build();
    static TestIndex index_hidden = TestIndex.name("index_hidden").documentCount(52).hidden().seed(8).attr("prefix", "h").build();
    static TestIndex user_dept_terms_lookup = TestIndex.name("user_dept_terms_lookup").documentCount(0).customDocument("limited_user_index_1_dept_D_terms_lookup", ImmutableMap.of("dept", "dept_d")).hidden().build();
    static TestDataStream ds_a1 = TestDataStream.name("ds_a1").documentCount(100).rolloverAfter(10).seed(1).build();

    static TestAlias alias_1 = new TestAlias("alias_1", index_1);
    static TestAlias alias_12 = new TestAlias("alias_12", index_1, index_2);

    static TestSgConfig.User LIMITED_USER_INDEX_1_DEPT_A = new TestSgConfig.User("limited_user_index_1_dept_A")//
            .description("dept_a in index_1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on("index_1"))//
            .indexMatcher("read", limitedTo(index_1.filteredBy(node -> node.getAsString("dept").startsWith("dept_a"))));

    static TestSgConfig.User LIMITED_USER_INDEX_1_DEPT_D = new TestSgConfig.User("limited_user_index_1_dept_D")//
            .description("dept_d in index_1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_d")).on("index_1"))//
            .indexMatcher("read", limitedTo(index_1.filteredBy(node -> node.getAsString("dept").startsWith("dept_d"))));

    static TestSgConfig.User LIMITED_USER_INDEX_1_HIDDEN_DEPT_A = new TestSgConfig.User("limited_user_index_1_hidden_dept_A")//
            .description("dept_a in index_1 and index_hidden")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on("index_1", "index_hidden"))//
            .indexMatcher("read", limitedTo(index_1.filteredBy(node -> node.getAsString("dept").startsWith("dept_a")),
                    index_hidden.filteredBy(node -> node.getAsString("dept").startsWith("dept_a"))));

    static TestSgConfig.User LIMITED_USER_INDEX_1_DEPT_A_INDEX_2_DEPT_D = new TestSgConfig.User("limited_user_index_1_dept_A_index_2_dept_D")//
            .description("dept_a in index_1; dept_d in index_2")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on("index_1")//
                            .indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_d")).on("index_2"))//
            .indexMatcher("read", limitedTo(index_1.filteredBy(node -> node.getAsString("dept").startsWith("dept_a")),
                    index_2.filteredBy(node -> node.getAsString("dept").startsWith("dept_d"))));

    static TestSgConfig.User LIMITED_USER_ALIAS_1_DEPT_A = new TestSgConfig.User("limited_user_alias_1_dept_A")//
            .description("dept_a in alias_1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on("alias_1"))//
            .indexMatcher("read", limitedTo(alias_1.filteredBy(node -> node.getAsString("dept").startsWith("dept_a")),
                    index_1.filteredBy(node -> node.getAsString("dept").startsWith("dept_a"))));

    static TestSgConfig.User LIMITED_USER_ALIAS_12_DEPT_D = new TestSgConfig.User("limited_user_alias_12_dept_D")//
            .description("dept_d in alias_12")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_d")).on("alias_12"))//
            .indexMatcher("read", limitedTo(index_1.filteredBy(node -> node.getAsString("dept").startsWith("dept_d")),
                    index_2.filteredBy(node -> node.getAsString("dept").startsWith("dept_d"))));

    static TestSgConfig.User LIMITED_USER_ALIAS_12_DEPT_D_INDEX_1_DEPT_A = new TestSgConfig.User("limited_user_alias_12_dept_D_index_1_dept_a")//
            .description("dept_d in alias_12; additionally dept_a in index_1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_d")).on("alias_12"), //
                    new Role("r2")//
                            .indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on("index_1"))//
            .indexMatcher("read",
                    limitedTo(
                            index_1.filteredBy(
                                    node -> node.getAsString("dept").startsWith("dept_d") || node.getAsString("dept").startsWith("dept_a")),
                            index_2.filteredBy(node -> node.getAsString("dept").startsWith("dept_d"))));

    static TestSgConfig.User LIMITED_USER_INDEX_1_DS_A1_DEPT_D_TERMS_LOOKUP = new TestSgConfig.User("limited_user_index_1_dept_D_terms_lookup")//
            .description("dept_d in index_1 with terms lookup")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ")
                            .dls(DocNode.of("terms",
                                    DocNode.of("dept", DocNode.of("index", "user_dept_terms_lookup", "id", "${user.name}", "path", "dept"))))
                            .on("index_1")
                            .dataStreamPermissions("SGS_READ")
                            .dls(DocNode.of("terms",
                                    DocNode.of("dept", DocNode.of("index", "user_dept_terms_lookup", "id", "${user.name}", "path", "dept"))))
                            .on("ds_a1"))//
            .indexMatcher("read", limitedTo(
                    index_1.filteredBy(node -> node.getAsString("dept").startsWith("dept_d")),
                    ds_a1.filteredBy(node -> node.getAsString("dept").startsWith("dept_d"))
            ));

    /*
    static TestSgConfig.User LIMITED_USER_B1 = new TestSgConfig.User("limited_user_B1")//
            .description("index_b1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").on("index_b1"))//
            .indexMatcher("read", limitedTo(index_b1))//
            .indexMatcher("read_top_level", limitedTo(index_b1))//
            .indexMatcher("get_alias", limitedToNone());
    
    static TestSgConfig.User LIMITED_USER_C = new TestSgConfig.User("limited_user_C")//
            .description("index_c*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").on("index_c*"))//
            .indexMatcher("read", limitedTo(index_c1))//
            .indexMatcher("read_top_level", limitedTo(index_c1))//
            .indexMatcher("get_alias", limitedToNone());
    
    static TestSgConfig.User LIMITED_USER_ALIAS_AB1 = new TestSgConfig.User("limited_user_alias_AB1")//
            .description("alias_ab1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "indices:admin/aliases/get").on("alias_ab1*"))//
            .indexMatcher("read", limitedTo(index_a1, index_a2, index_a3, index_b1, alias_ab1))//
            .indexMatcher("read_top_level", limitedTo(alias_ab1))//
            .indexMatcher("get_alias", limitedTo(index_a1, index_a2, index_a3, index_b1, alias_ab1));
    
    static TestSgConfig.User LIMITED_USER_ALIAS_C1 = new TestSgConfig.User("limited_user_alias_C1")//
            .description("alias_c1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ").on("alias_c1"))//
            .indexMatcher("read", limitedTo(index_c1, alias_c1))//
            .indexMatcher("read_top_level", limitedTo(alias_c1))//
            .indexMatcher("get_alias", limitedToNone());
    
    static TestSgConfig.User LIMITED_USER_A_HIDDEN = new TestSgConfig.User("limited_user_A_hidden")//
            .description("index_a*, index_hidden*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").on("index_a*", "index_hidden*", ".index_hidden*"))//
            .indexMatcher("read", limitedTo(index_a1, index_a2, index_a3, index_ax, index_hidden, index_hidden_dot))//
            .indexMatcher("read_top_level", limitedTo(index_a1, index_a2, index_a3, index_ax, index_hidden, index_hidden_dot))//
            .indexMatcher("get_alias", limitedToNone());
    
    static TestSgConfig.User LIMITED_USER_A_SYSTEM = new TestSgConfig.User("limited_user_A_system")//
            .description("index_a*, .index_system*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").on("index_a*", ".index_system*"))//
            .indexMatcher("read", limitedTo(index_a1, index_a2, index_a3, index_ax, index_system))//
            .indexMatcher("read_top_level", limitedTo(index_a1, index_a2, index_a3, index_ax, index_system))//
            .indexMatcher("get_alias", limitedToNone());
    
    static TestSgConfig.User LIMITED_USER_NONE = new TestSgConfig.User("limited_user_none")//
            .description("no privileges for existing indices")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_CRUD").on("index_does_not_exist_*"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("read_top_level", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());
    
    static TestSgConfig.User INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS = new TestSgConfig.User("invalid_user_index_permissions_for_alias")//
            .description("invalid: index permissions for alias")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").on("alias_ab1"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("read_top_level", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());
            */

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user")//
            .description("unlimited")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("*").on("*")//
                            .aliasPermissions("*").on("*")
                            .dataStreamPermissions("*").on("*")

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

    static List<TestSgConfig.User> USERS = ImmutableList.of(LIMITED_USER_INDEX_1_DEPT_A, LIMITED_USER_INDEX_1_DEPT_D,
            LIMITED_USER_INDEX_1_HIDDEN_DEPT_A, LIMITED_USER_INDEX_1_DEPT_A_INDEX_2_DEPT_D, LIMITED_USER_ALIAS_1_DEPT_A, LIMITED_USER_ALIAS_12_DEPT_D,
            LIMITED_USER_ALIAS_12_DEPT_D_INDEX_1_DEPT_A, LIMITED_USER_INDEX_1_DS_A1_DEPT_D_TERMS_LOOKUP, UNLIMITED_USER, SUPER_UNLIMITED_USER);

    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().metrics("detailed");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled().users(USERS)//
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .indices(index_1, index_2, index_3, index_hidden, user_dept_terms_lookup)//
            .aliases(alias_1, alias_12)//
            .dataStreams(ds_a1)
            .authzDebug(true)//
            .logRequests()//
            .dlsFls(DLSFLS)//
            .useExternalProcessCluster()//
            .build();

    final TestSgConfig.User user;

    @Test
    public void search_noPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3, ds_a1).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3, ds_a1).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_all_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000&expand_wildcards=all");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3, index_hidden, user_dept_terms_lookup, searchGuardIndices(), ds_a1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3, ds_a1).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_wildcard_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*/_search?size=1000&expand_wildcards=all");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3, index_hidden, user_dept_terms_lookup, searchGuardIndices(), ds_a1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_staticIndicies_hidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_hidden/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_hidden).at("hits.hits[*]").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("index_*/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3).at("hits.hits[*]").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_alias_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_12/_search?size=1000&ignore_unavailable=true");
            if (containsExactly(index_1, index_1).at("hits.hits[*]").but(user.indexMatcher("read")).isEmpty() || user == LIMITED_USER_ALIAS_1_DEPT_A) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, containsExactly(index_1, index_2).at("hits.hits[*]").but(user.indexMatcher("read")).whenEmpty(200));
            }
        }
    }

    @Test
    public void search_alias_pattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_*/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_1, index_2).at("hits.hits[*]").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_scroll_all() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.get("/_all/_search?scroll=1m&size=15");
            assertThat(response, isOk());
            List<String> hits = new ArrayList<>(response.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits").map(node -> node.getAsString("_index")));

            String scrollId = response.getBodyAsDocNode().getAsString("_scroll_id");

            for (;;) {
                GenericRestClient.HttpResponse scrollResponse = client.postJson("/_search/scroll", DocNode.of("scroll", "1m", "scroll_id", scrollId));
                assertThat(scrollResponse, isOk());
                List<String> moreHits = scrollResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits").map(node -> node.getAsString("_index"));

                if (moreHits.size() == 0) {
                    break;
                }

                hits.addAll(moreHits);
            }

            assertThat(hits, containsExactly(index_1, index_2, index_3, ds_a1).but(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_aggregation_terms_all() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.postJson("/_all/_search",
                    DocNode.of("query.match_all", DocNode.EMPTY, "aggs.test_agg.terms.field", "dept.keyword"));

            assertThat(response, containsExactly(index_1, index_2, index_3, ds_a1).aggregateTerm("dept").at("aggregations.test_agg.buckets")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_aggregation_terms_static_index() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.postJson("/index_1/_search",
                    DocNode.of("query.match_all", DocNode.EMPTY, "aggs.test_agg.terms.field", "dept.keyword"));

            assertThat(response,
                    containsExactly(index_1).aggregateTerm("dept").at("aggregations.test_agg.buckets").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_aggregation_terms_static_alias() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.postJson("/alias_12/_search?ignore_unavailable=true",
                    DocNode.of("query.match_all", DocNode.EMPTY, "aggs.test_agg.terms.field", "dept.keyword"));

            if (containsExactly(index_1, index_1).at("hits.hits[*]").but(user.indexMatcher("read")).isEmpty() || user == LIMITED_USER_ALIAS_1_DEPT_A) {
                assertThat(response, isOk());
            } else {
                assertThat(response, containsExactly(index_1, index_2).aggregateTerm("dept").at("aggregations.test_agg.buckets")
                        .but(user.indexMatcher("read")).whenEmpty(200));
            }
        }
    }

    @Test
    public void search_dataStream() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void msearch_staticIndices() throws Exception {
        String msearchBody = "{\"index\":\"index_1\"}\n" //
                + "{\"size\":200, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n" //
                + "{\"index\":\"index_2\"}\n" //
                + "{\"size\":200, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);
            assertThat(httpResponse, containsExactly(index_1, index_2).at("responses[*].hits.hits[*]").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void get() throws Exception {
        TestDocument testDocument1a1 = index_1.getTestData().anyDocumentForDepartment("dept_a_1");

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/index_1/_doc/" + testDocument1a1.getId());
            if (containsExactly(index_1).isCoveredBy(user.indexMatcher("read"))) {
                if (containsExactly(index_1).but(user.indexMatcher("read")).containsDocument(testDocument1a1.getId())) {
                    assertThat(httpResponse, isOk());
                } else {
                    assertThat(httpResponse, isNotFound());
                }

            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void get2() throws Exception {
        TestDocument testDocument1d = index_1.getTestData().anyDocumentForDepartment("dept_d");

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/index_1/_doc/" + testDocument1d.getId());
            if (containsExactly(index_1).isCoveredBy(user.indexMatcher("read"))) {
                if (containsExactly(index_1).but(user.indexMatcher("read")).containsDocument(testDocument1d.getId())) {
                    assertThat(httpResponse, isOk());
                } else {
                    assertThat(httpResponse, isNotFound());
                }

            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void get_alias() throws Exception {
        TestDocument testDocument1a1 = index_1.getTestData().anyDocumentForDepartment("dept_a_1");

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/alias_1/_doc/" + testDocument1a1.getId());
            if (containsExactly(index_1).isCoveredBy(user.indexMatcher("read")) && user != LIMITED_USER_ALIAS_12_DEPT_D) {
                if (containsExactly(index_1).but(user.indexMatcher("read")).containsDocument(testDocument1a1.getId())) {
                    assertThat(httpResponse, isOk());
                } else {
                    assertThat(httpResponse, isNotFound());
                }

            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void get_alias2() throws Exception {
        TestDocument testDocument1d = index_1.getTestData().anyDocumentForDepartment("dept_d");

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/alias_1/_doc/" + testDocument1d.getId());
            if (containsExactly(index_1).isCoveredBy(user.indexMatcher("read")) && user != LIMITED_USER_ALIAS_12_DEPT_D) {
                if (containsExactly(index_1).but(user.indexMatcher("read")).containsDocument(testDocument1d.getId())) {
                    assertThat(httpResponse, isOk());
                } else {
                    assertThat(httpResponse, isNotFound());
                }

            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void mget() throws Exception {
        TestDocument testDocument1a1 = index_1.getTestData().anyDocumentForDepartment("dept_a_1");
        TestDocument testDocument1b1 = index_1.getTestData().anyDocumentForDepartment("dept_b_1");
        TestDocument testDocument2a1 = index_2.getTestData().anyDocumentForDepartment("dept_a_1");
        TestDocument testDocument2b1 = index_2.getTestData().anyDocumentForDepartment("dept_b_1");

        DocNode mget = DocNode.of("docs", DocNode.array(//
                DocNode.of("_index", "index_1", "_id", testDocument1a1.getId()), //
                DocNode.of("_index", "index_1", "_id", testDocument1b1.getId()), //
                DocNode.of("_index", "index_2", "_id", testDocument2a1.getId()), //
                DocNode.of("_index", "index_2", "_id", testDocument2b1.getId())));

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse response = restClient.postJson("/_mget", mget);

            if (user == LIMITED_USER_INDEX_1_DS_A1_DEPT_D_TERMS_LOOKUP) {
                //mget request sent by user with TLQ is handled differently. It's replaced with search request, response looks different from standard mget response.
                assertThat(response, isForbidden());
                assertEquals(response.getBody(), "Insufficient permissions", response.getBodyAsDocNode().get("error", "reason"));
            } else {
                assertThat(response, isOk());
                DocNode body = response.getBodyAsDocNode();

                checkMgetDocument(response, body, index_1, testDocument1a1);
                checkMgetDocument(response, body, index_1, testDocument1b1);
                checkMgetDocument(response, body, index_2, testDocument2a1);
                checkMgetDocument(response, body, index_2, testDocument2b1);
            }
        }
    }

    @Test
    public void mget_alias() throws Exception {
        TestDocument testDocument1a1 = index_1.getTestData().anyDocumentForDepartment("dept_a_1");
        TestDocument testDocument1b1 = index_1.getTestData().anyDocumentForDepartment("dept_b_1");

        DocNode mget = DocNode.of("docs", DocNode.array(//
                DocNode.of("_index", "alias_1", "_id", testDocument1a1.getId()), //
                DocNode.of("_index", "alias_1", "_id", testDocument1b1.getId())));

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse response = restClient.postJson("/_mget", mget);
            assertThat(response, isOk());
            DocNode body = response.getBodyAsDocNode();

            if (user == LIMITED_USER_INDEX_1_DS_A1_DEPT_D_TERMS_LOOKUP) {
                //mget request sent by user with TLQ is handled differently. It's replaced with search request, response looks different from standard mget response.
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("docs", 0));
            } else {
                checkMgetDocument(response, body, index_1, testDocument1a1);
                checkMgetDocument(response, body, index_1, testDocument1b1);
            }
        }
    }

    private void checkMgetDocument(HttpResponse response, DocNode body, TestIndexLike testIndex, TestDocument testDocument) throws Exception {
        DocNode foundDocument = body.findSingleNodeByJsonPath("docs[?(@._id == \"" + testDocument.getId() + "\")]").toListOfNodes().get(0);
        assertNotNull(response.getBody(), foundDocument);

        if (containsExactly(testIndex).isCoveredBy(user.indexMatcher("read"))) {
            Boolean found = foundDocument.getBoolean("found");
            
            if (found == null) {
                fail("No found attribute " + foundDocument.toString());
            }
            
            if (containsExactly(testIndex).but(user.indexMatcher("read")).containsDocument(testDocument.getId())) {
                assertTrue(response.getBody(), found);
            } else {
                assertFalse(response.getBody(), found);
            }
        } else {
            assertEquals(response.getBody(), "Insufficient permissions", foundDocument.get("error", "reason"));
        }
    }

    @Test
    public void termvectors() throws Exception {
        TestDocument testDocument1a1 = index_1.getTestData().anyDocumentForDepartment("dept_a_1");

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/index_1/_termvectors/" + testDocument1a1.getId());

            if (user == LIMITED_USER_INDEX_1_DS_A1_DEPT_D_TERMS_LOOKUP) {
                // termverctors is not supported for terms lookup DLS
                assertThat(httpResponse, isInternalServerError());
                assertTrue(httpResponse.getBody(),
                        httpResponse.getBodyAsDocNode().getAsNode("error").getAsString("reason").startsWith("Unsupported request type for filter level DLS"));
            } else {
                if (containsExactly(index_1).isCoveredBy(user.indexMatcher("read"))) {
                    assertThat(httpResponse, isOk());

                    if (containsExactly(index_1).but(user.indexMatcher("read")).containsDocument(testDocument1a1.getId())) {
                        assertEquals(httpResponse.getBody(), true, httpResponse.getBodyAsDocNode().get("found"));
                    } else {
                        assertEquals(httpResponse.getBody(), false, httpResponse.getBodyAsDocNode().get("found"));
                    }
                } else {
                    assertThat(httpResponse, isForbidden());
                }
            }
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

    public DlsReadOnlyIntTests(TestSgConfig.User user, String description) throws Exception {
        this.user = user;
    }

}
