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
import static com.floragunn.searchguard.test.RestMatchers.isOk;
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
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * TODO negation auf static: aa,-a*
 *
 */
@RunWith(Parameterized.class)
public class DlsReadOnlyIntTests {
    static TestIndex index_1 = TestIndex.name("index_1").documentCount(100).seed(1).attr("prefix", "a").setting("index.number_of_shards", 5).build();
    static TestIndex index_2 = TestIndex.name("index_2").documentCount(110).seed(2).attr("prefix", "a").setting("index.number_of_shards", 5).build();
    static TestIndex index_3 = TestIndex.name("index_3").documentCount(51).seed(4).attr("prefix", "b").setting("index.number_of_shards", 5).build();
    static TestIndex index_hidden = TestIndex.name("index_hidden").documentCount(52).hidden().seed(8).attr("prefix", "h").build();

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
            LIMITED_USER_INDEX_1_HIDDEN_DEPT_A, LIMITED_USER_INDEX_1_DEPT_A_INDEX_2_DEPT_D, LIMITED_USER_ALIAS_12_DEPT_D,
            LIMITED_USER_ALIAS_12_DEPT_D_INDEX_1_DEPT_A, UNLIMITED_USER, SUPER_UNLIMITED_USER);

    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().useImpl("flx").metrics("detailed");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().users(USERS)//
            .indices(index_1, index_2, index_3, index_hidden)//
            .aliases(alias_12)//
            .authzDebug(true)//
            .logRequests()//
            .dlsFls(DLSFLS)//
            // .useExternalProcessCluster()//
            .build();

    final TestSgConfig.User user;

    @Test
    public void search_noPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3).at("hits.hits[*]").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3).at("hits.hits[*]").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_all_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000&expand_wildcards=all");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3, index_hidden, searchGuardIndices()).at("hits.hits[*]")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*/_search?size=1000");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3).at("hits.hits[*]").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_wildcard_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*/_search?size=1000&expand_wildcards=all");
            assertThat(httpResponse, containsExactly(index_1, index_2, index_3, index_hidden, searchGuardIndices()).at("hits.hits[*]")
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
            assertThat(httpResponse, containsExactly(index_1, index_2).at("hits.hits[*]").but(user.indexMatcher("read")).whenEmpty(200));
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
            List<DocNode> hits = new ArrayList<>(response.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"));

            String scrollId = response.getBodyAsDocNode().getAsString("_scroll_id");

            for (;;) {
                GenericRestClient.HttpResponse scrollResponse = client.postJson("/_search/scroll", DocNode.of("scroll", "1m", "scroll_id", scrollId));
                assertThat(scrollResponse, isOk());
                List<DocNode> moreHits = scrollResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits");

                if (moreHits.size() == 0) {
                    break;
                }

                hits.addAll(moreHits);
            }

            assertThat(hits, containsExactly(index_1, index_2, index_3).but(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_aggregation_terms_all() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.postJson("/_all/_search",
                    DocNode.of("query.match_all", DocNode.EMPTY, "aggs.test_agg.terms.field", "dept.keyword"));

            assertThat(response, containsExactly(index_1, index_2, index_3).aggregateTerm("dept").at("aggregations.test_agg.buckets")
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

            assertThat(response, containsExactly(index_1, index_2).aggregateTerm("dept").at("aggregations.test_agg.buckets")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void msearch_staticIndices() throws Exception {
        String msearchBody = "{\"index\":\"index_1\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n" //
                + "{\"index\":\"index_2\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);
            assertThat(httpResponse, containsExactly(index_1, index_2).at("responses[*].hits.hits[*]").but(user.indexMatcher("read")).whenEmpty(200));
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
            HttpResponse httpResponse = restClient.postJson("/_mget", mget);
            System.out.println(httpResponse.getBody());
            // TODO
          //  assertThat(httpResponse, containsExactly(index_a1, index_b1, index_b2).at("docs[?(@.found == true)]._index")
           //         .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    /*
    
    
    
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
                    index_hidden_dot, searchGuardIndices()).at("$[*].index").but(user.indexMatcher("read")).whenEmpty(200));
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
            HttpResponse httpResponse = restClient.get("index_b* /_stats");
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
    }*/

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
