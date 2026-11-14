/*
 * Copyright 2026 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.dlsfls;

import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * The value of a constant_keyword field lives in the mapping, not in the Lucene index. Elasticsearch applies the
 * field filter of the plugin (i.e., the FLS rules of the current user) whenever it resolves such a field in a query.
 * DLS queries are authorization rules: they must see the real value even if FLS hides the field from the user.
 */
public class DlsFlsConstantKeywordIntTest {

    static final String INDEX_PATTERN = "ck_*";
    static final String INDEX_PROD = "ck_prod";
    static final String INDEX_DEV = "ck_dev";
    static final String LOOKUP_INDEX = "lookup_dept";
    static final int DOC_COUNT_PER_INDEX = 10;

    static final TestSgConfig.User DLS_USER = new TestSgConfig.User("dls_user")
            .roles(new Role("dls").indexPermissions("SGS_READ").dls(DocNode.of("term.env.value", "prod")).on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User DLS_TERM_FLS_USER = new TestSgConfig.User("dls_term_fls_user").roles(new Role("dls_term_fls")
            .indexPermissions("SGS_READ").dls(DocNode.of("term.env.value", "prod")).fls("~env").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.User DLS_MUST_NOT_FLS_USER = new TestSgConfig.User("dls_must_not_fls_user")
            .roles(new Role("dls_must_not_fls").indexPermissions("SGS_READ").dls(DocNode.of("bool.must_not.term.env.value", "dev")).fls("~env")
                    .on(INDEX_PATTERN).clusterPermissions("*"));

    /**
     * A DLS query with a terms lookup query makes Search Guard apply DLS on filter level, i.e., as part of the query of the search request.
     */
    static final TestSgConfig.User DLS_FILTER_LEVEL_FLS_USER = new TestSgConfig.User("dls_filter_level_fls_user")
            .roles(new Role("dls_filter_level_fls").indexPermissions("SGS_READ")
                    .dls(DocNode.of("bool",
                            DocNode.of("must", DocNode.of("terms", DocNode.of("dept", DocNode.of("index", LOOKUP_INDEX, "id", "${user.name}", "path", "dept"))),
                                    "must_not", DocNode.of("term", DocNode.of("env", "dev")))))
                    .fls("~env").on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC)
            .users(DLS_USER, DLS_TERM_FLS_USER, DLS_MUST_NOT_FLS_USER, DLS_FILTER_LEVEL_FLS_USER).resources("dlsfls")
            // constant_keyword is provided by an x-pack module, which only the real distribution contains
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            createIndex(client, INDEX_PROD, "prod");
            createIndex(client, INDEX_DEV, "dev");
            HttpResponse response = client.putJson("/" + LOOKUP_INDEX + "/_doc/" + DLS_FILTER_LEVEL_FLS_USER.getName() + "?refresh=true",
                    DocNode.of("dept", "dept_0"));
            assertThat(response.getBody(), response.getStatusCode(), equalTo(201));
        }
    }

    private static void createIndex(GenericRestClient client, String index, String env) throws Exception {
        // several shards per data node, so that the shards of an index are searched on both data nodes
        HttpResponse response = client.putJson("/" + index,
                DocNode.of("settings.index.number_of_shards", 5, "mappings.properties",
                        DocNode.of("env", DocNode.of("type", "constant_keyword", "value", env), "dept", DocNode.of("type", "keyword"))));
        assertThat(response, isOk());

        for (int i = 0; i < DOC_COUNT_PER_INDEX; i++) {
            response = client.putJson("/" + index + "/_doc/" + i + "?refresh=true", DocNode.of("dept", "dept_" + (i % 2)));
            assertThat(response.getBody(), response.getStatusCode(), equalTo(201));
        }
    }

    @Test
    public void search_dlsOnConstantKeyword() throws Exception {
        assertSeesOnlyProd(DLS_USER);
    }

    @Test
    public void search_dlsOnConstantKeywordHiddenByFls_term() throws Exception {
        assertSeesOnlyProd(DLS_TERM_FLS_USER);
    }

    @Test
    public void search_dlsOnConstantKeywordHiddenByFls_mustNot() throws Exception {
        assertSeesOnlyProd(DLS_MUST_NOT_FLS_USER);
    }

    @Test
    public void get_dlsOnConstantKeywordHiddenByFls() throws Exception {
        for (TestSgConfig.User user : List.of(DLS_TERM_FLS_USER, DLS_MUST_NOT_FLS_USER)) {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                assertThat(user.getName(), client.get("/" + INDEX_PROD + "/_doc/1"), isOk());
                assertThat(user.getName(), client.get("/" + INDEX_DEV + "/_doc/1"), isNotFound());
            }
        }
    }

    @Test
    public void search_flsHidesConstantKeywordValueFromUser() throws Exception {
        // only the DLS query may see the value; for the query, the fields and the aggregations of the user it stays hidden
        try (GenericRestClient client = cluster.getRestClient(DLS_TERM_FLS_USER)) {
            HttpResponse response = client.postJson("/" + INDEX_PATTERN + "/_search?size=100",
                    DocNode.of("query.term.env.value", "prod", "track_total_hits", true));
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").get("value"), equalTo(0));

            response = client.postJson("/" + INDEX_PATTERN + "/_search?size=100",
                    DocNode.of("fields", List.of("env"), "aggs.envs.terms.field", "env"));
            assertThat(response, isOk());
            DocNode body = response.getBodyAsDocNode();
            List<DocNode> hits = body.getAsNode("hits").getAsListOfNodes("hits");
            assertThat(response.getBody(), hits, hasSize(DOC_COUNT_PER_INDEX));
            assertThat(response.getBody(), hits.stream().filter(hit -> hit.hasNonNull("fields")).collect(Collectors.toList()), empty());
            assertThat(response.getBody(), body.getAsNode("aggregations").getAsNode("envs").getAsListOfNodes("buckets"), empty());
        }
    }

    @Test
    public void search_filterLevelDlsOnConstantKeywordHiddenByFls() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DLS_FILTER_LEVEL_FLS_USER)) {
            HttpResponse response = client.get("/" + INDEX_PATTERN + "/_search?size=100");
            assertThat(response, isOk());
            assertThat(response.getBody(), hitIndices(response), hasSize(DOC_COUNT_PER_INDEX / 2));
            assertThat(response.getBody(), hitIndices(response), everyItem(equalTo(INDEX_PROD)));

            // the later pages of a scroll search are executed without the thread context of the initial request
            response = client.get("/" + INDEX_PATTERN + "/_search?size=2&scroll=1m");
            int hitCount = 0;

            while (true) {
                assertThat(response, isOk());
                List<String> indices = hitIndices(response);

                if (indices.isEmpty()) {
                    break;
                }

                assertThat(response.getBody(), indices, everyItem(equalTo(INDEX_PROD)));
                hitCount += indices.size();
                response = client.postJson("/_search/scroll",
                        DocNode.of("scroll", "1m", "scroll_id", response.getBodyAsDocNode().getAsString("_scroll_id")));
            }

            assertThat(hitCount, equalTo(DOC_COUNT_PER_INDEX / 2));
        }
    }

    @Test
    public void search_dlsDoesNotLeakRequestBreakerMemory() throws Exception {
        long before = requestBreakerEstimatedSize();

        try (GenericRestClient client = cluster.getRestClient(DLS_TERM_FLS_USER)) {
            for (int i = 0; i < 20; i++) {
                assertThat(client.get("/" + INDEX_PATTERN + "/_search?size=100"), isOk());
            }
        }

        assertThat(requestBreakerEstimatedSize(), lessThanOrEqualTo(before));
    }

    private static long requestBreakerEstimatedSize() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.get("/_nodes/stats/breaker");
            assertThat(response, isOk());
            return response.getBodyAsDocNode().getAsNode("nodes").values().stream()
                    .mapToLong(node -> ((Number) DocNode.wrap(node).getAsNode("breakers").getAsNode("request").get("estimated_size_in_bytes")).longValue())
                    .sum();
        }
    }

    private static List<String> hitIndices(HttpResponse response) throws Exception {
        return response.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits").stream().map(hit -> hit.getAsString("_index"))
                .collect(Collectors.toList());
    }

    private static void assertSeesOnlyProd(TestSgConfig.User user) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            HttpResponse response = client.get("/" + INDEX_PATTERN + "/_search?size=100");
            assertThat(response, isOk());
            assertThat(response.getBody(), hitIndices(response), hasSize(DOC_COUNT_PER_INDEX));
            assertThat(response.getBody(), hitIndices(response), everyItem(equalTo(INDEX_PROD)));

            // size=0 with a query takes the path where the DLS query is added to the search query
            response = client.postJson("/" + INDEX_PATTERN + "/_search?size=0",
                    DocNode.of("query.term.dept.value", "dept_0", "track_total_hits", true));
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").get("value"),
                    equalTo(DOC_COUNT_PER_INDEX / 2));
        }
    }
}
