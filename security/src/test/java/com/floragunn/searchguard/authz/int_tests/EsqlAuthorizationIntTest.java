/*
 * Copyright 2026 floragunn GmbH
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
 */

package com.floragunn.searchguard.authz.int_tests;

import static com.floragunn.searchguard.test.RestMatchers.isBadRequest;
import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * ES|QL authorization with several indices, an alias and several views. See EsqlViewIntTest for an overview of how ES|QL
 * is authorized and for the reasoning behind the HTTP status codes.
 *
 * Reminder on the ignore_unauthorized_indices (DNFOF) semantics, which ES|QL inherits because its coordinator resolves
 * indices with ignore_unavailable=true:
 *
 * - A single forbidden concrete index or view name is denied with 403.
 * - An explicit list of indices which contains a forbidden index is denied with 403: The search_shards step of the
 *   ES|QL coordinator uses strict index options (ignore_unavailable=false), so the list is not reduced. This is the same
 *   behaviour as for a _search without ignore_unavailable.
 * - A wildcard expression which matches forbidden indices next to permitted ones is reduced to the permitted indices and
 *   succeeds with 200.
 * - A wildcard expression where nothing is permitted is denied with 403 (not an empty result).
 * - Privileges on an alias also permit addressing its member indices directly (regular Search Guard alias semantics).
 *
 * Test data:
 *
 * - sales_eu: 3 documents, sales_us: 2 documents, hr_data: 2 documents, alias sales -> sales_eu, sales_us
 * - views: view_sales_all (FROM sales_eu,sales_us), view_sales_eu_big (FROM sales_eu | WHERE amount > 100),
 *   view_hr (FROM hr_data), view_mixed (FROM sales_eu,hr_data), view_ds (FROM ds_sales)
 * - products: lookup index (index.mode: lookup) for LOOKUP JOIN
 * - product_info + enrich policy product_policy for ENRICH
 * - ds_sales: data stream
 *
 * LOOKUP JOIN reads the lookup index via indices:data/read/esql/lookup_from_index, a transport-only request which is
 * authorized in SearchGuardRequestHandler; additionally, the lookup index is resolved via resolve_fields on the coordinator.
 * ENRICH resolves the policy via cluster:monitor/xpack/enrich/esql/resolve_policy (transport-only, authorized as cluster
 * privilege in SearchGuardRequestHandler); the lookup in the .enrich-* index is then done by Elasticsearch as internal user.
 */
public class EsqlAuthorizationIntTest {

    static final TestSgConfig.User SALES_USER = new TestSgConfig.User("sales")
            .roles(new Role("sales").clusterPermissions().indexPermissions("SGS_READ").on("sales_*", "view_sales*"));

    static final TestSgConfig.User HR_USER = new TestSgConfig.User("hr")
            .roles(new Role("hr").clusterPermissions().indexPermissions("SGS_READ").on("hr_*", "view_hr*"));

    static final TestSgConfig.User ALL_READ_USER = new TestSgConfig.User("all_read")
            .roles(new Role("all_read").clusterPermissions().indexPermissions("SGS_READ").on("*"));

    static final TestSgConfig.User LOOKUP_USER = new TestSgConfig.User("lookup")
            .roles(new Role("lookup").clusterPermissions().indexPermissions("SGS_READ").on("sales_*", "products"));

    static final TestSgConfig.User ENRICH_USER = new TestSgConfig.User("enrich").roles(new Role("enrich")
            .clusterPermissions("cluster:monitor/xpack/enrich/esql/resolve_policy").indexPermissions("SGS_READ").on("sales_*"));

    /**
     * Passes all coordinator level checks for a LOOKUP JOIN, but lacks the privilege for indices:data/read/esql/lookup_from_index
     * on the lookup index. The query must be denied by the transport level check in SearchGuardRequestHandler.
     */
    static final TestSgConfig.User LOOKUP_DATA_NODE_CHECK_USER = new TestSgConfig.User("lookup_data_node_check")
            .roles(new Role("lookup_data_node_check").clusterPermissions()
                    .indexPermissions("indices:data/read/esql", "indices:data/read/esql/resolve_*", "indices:data/read/esql/search_shards").on("*")
                    .indexPermissions("indices:data/read/esql/data").on("sales_*"));

    static final TestSgConfig.User DS_USER = new TestSgConfig.User("ds").roles(new Role("ds").clusterPermissions()
            .dataStreamPermissions("SGS_READ").on("ds_*").indexPermissions("SGS_READ").on("view_ds*"));

    static final TestDataStream DS_SALES = TestDataStream.name("ds_sales").documentCount(10).seed(5).build();

    /**
     * Only has privileges on the alias sales, but not on its member indices or on any view
     */
    static final TestSgConfig.User SALES_ALIAS_USER = new TestSgConfig.User("sales_alias")
            .roles(new Role("sales_alias").clusterPermissions().aliasPermissions("SGS_READ").on("sales"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(SALES_USER, HR_USER, ALL_READ_USER, SALES_ALIAS_USER, LOOKUP_USER, ENRICH_USER, LOOKUP_DATA_NODE_CHECK_USER, DS_USER)
            .indexTemplates(TestIndexTemplate.DATA_STREAM_MINIMAL).dataStreams(DS_SALES).authzDebug(true).enterpriseModulesEnabled()
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void createTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            for (String index : new String[] { "sales_eu", "sales_us" }) {
                GenericRestClient.HttpResponse response = client.putJson("/" + index, DocNode.of("settings.index.number_of_shards", 2,
                        "mappings.properties.product.type", "keyword", "mappings.properties.region.type", "keyword",
                        "mappings.properties.amount.type", "integer"));
                assertThat(response, isOk());
            }

            GenericRestClient.HttpResponse response = client.putJson("/hr_data", DocNode.of("settings.index.number_of_shards", 2,
                    "mappings.properties.employee.type", "keyword", "mappings.properties.amount.type", "integer"));
            assertThat(response, isOk());

            indexDoc(client, "sales_eu", "1", DocNode.of("region", "eu", "product", "apple", "amount", 50));
            indexDoc(client, "sales_eu", "2", DocNode.of("region", "eu", "product", "pear", "amount", 150));
            indexDoc(client, "sales_eu", "3", DocNode.of("region", "eu", "product", "plum", "amount", 250));
            indexDoc(client, "sales_us", "1", DocNode.of("region", "us", "product", "apple", "amount", 75));
            indexDoc(client, "sales_us", "2", DocNode.of("region", "us", "product", "pear", "amount", 175));
            indexDoc(client, "hr_data", "1", DocNode.of("employee", "alice", "amount", 5000));
            indexDoc(client, "hr_data", "2", DocNode.of("employee", "bob", "amount", 6000));

            response = client.postJson("/_aliases",
                    DocNode.of("actions", List.of(DocNode.of("add", DocNode.of("indices", List.of("sales_eu", "sales_us"), "alias", "sales")))));
            assertThat(response, isOk());

            createView(client, "view_sales_all", "FROM sales_eu,sales_us");
            createView(client, "view_sales_eu_big", "FROM sales_eu | WHERE amount > 100");
            createView(client, "view_hr", "FROM hr_data");
            createView(client, "view_mixed", "FROM sales_eu,hr_data");
            createView(client, "view_ds", "FROM ds_sales");

            // Lookup index for LOOKUP JOIN
            response = client.putJson("/products", DocNode.of("settings.index.mode", "lookup", "mappings.properties.product.type", "keyword",
                    "mappings.properties.price.type", "integer"));
            assertThat(response, isOk());
            indexDoc(client, "products", "1", DocNode.of("product", "apple", "price", 1));
            indexDoc(client, "products", "2", DocNode.of("product", "pear", "price", 2));
            indexDoc(client, "products", "3", DocNode.of("product", "plum", "price", 3));

            // Source index and policy for ENRICH
            response = client.putJson("/product_info", DocNode.of("mappings.properties.product.type", "keyword",
                    "mappings.properties.category.type", "keyword"));
            assertThat(response, isOk());
            indexDoc(client, "product_info", "1", DocNode.of("product", "apple", "category", "pome"));
            indexDoc(client, "product_info", "2", DocNode.of("product", "pear", "category", "pome"));
            indexDoc(client, "product_info", "3", DocNode.of("product", "plum", "category", "drupe"));
            response = client.putJson("/_enrich/policy/product_policy",
                    DocNode.of("match", DocNode.of("indices", "product_info", "match_field", "product", "enrich_fields", List.of("category"))));
            assertThat(response, isOk());
            response = client.put("/_enrich/policy/product_policy/_execute");
            assertThat(response, isOk());
        }
    }

    private static void indexDoc(GenericRestClient client, String index, String id, DocNode doc) throws Exception {
        assertThat(client.putJson("/" + index + "/_doc/" + id + "?refresh=true", doc), isCreated());
    }

    private static void createView(GenericRestClient client, String name, String query) throws Exception {
        assertThat(client.putJson("/_query/view/" + name, DocNode.of("query", query)), isOk());
    }

    private static GenericRestClient.HttpResponse query(TestSgConfig.User user, String query) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            return client.postJson("/_query", DocNode.of("query", query));
        }
    }

    private static GenericRestClient.HttpResponse count(TestSgConfig.User user, String from) throws Exception {
        return query(user, from + " | STATS c = COUNT(*)");
    }

    // --- all_read user: everything is permitted

    @Test
    public void allRead_indices_alias_views() throws Exception {
        assertThat(count(ALL_READ_USER, "FROM sales_eu"), json(nodeAt("values", equalTo(List.of(List.of(3))))));
        assertThat(count(ALL_READ_USER, "FROM sales_eu,sales_us"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
        assertThat(count(ALL_READ_USER, "FROM sales"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
        assertThat(count(ALL_READ_USER, "FROM sales_*,hr_*"), json(nodeAt("values", equalTo(List.of(List.of(7))))));
        assertThat(count(ALL_READ_USER, "FROM *"), isOk());
        assertThat(count(ALL_READ_USER, "FROM view_sales_all"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
        assertThat(count(ALL_READ_USER, "FROM view_sales_eu_big"), json(nodeAt("values", equalTo(List.of(List.of(2))))));
        assertThat(count(ALL_READ_USER, "FROM view_hr"), json(nodeAt("values", equalTo(List.of(List.of(2))))));
        assertThat(count(ALL_READ_USER, "FROM view_mixed"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
    }

    @Test
    public void allRead_rows() throws Exception {
        GenericRestClient.HttpResponse response = query(ALL_READ_USER, "FROM view_sales_all | WHERE product == \"pear\" | KEEP region, amount | SORT region");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of("eu", 150), List.of("us", 175))))));
    }

    // --- sales user

    @Test
    public void sales_permittedIndicesAndViews() throws Exception {
        assertThat(count(SALES_USER, "FROM sales_eu"), json(nodeAt("values", equalTo(List.of(List.of(3))))));
        assertThat(count(SALES_USER, "FROM sales_eu,sales_us"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
        assertThat(count(SALES_USER, "FROM sales_*"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
        assertThat(count(SALES_USER, "FROM view_sales_all"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
        assertThat(count(SALES_USER, "FROM view_sales_eu_big"), json(nodeAt("values", equalTo(List.of(List.of(2))))));
    }

    @Test
    public void sales_aliasWithPrivilegesOnAllMembers() throws Exception {
        // The user has no privileges on the alias itself, but on all of its members
        assertThat(count(SALES_USER, "FROM sales"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
    }

    @Test
    public void sales_forbiddenIndex() throws Exception {
        assertThat(count(SALES_USER, "FROM hr_data"), isForbidden());
    }

    @Test
    public void sales_forbiddenViewName() throws Exception {
        assertThat(count(SALES_USER, "FROM view_hr"), isForbidden());
    }

    @Test
    public void sales_explicitListWithForbiddenIndex() throws Exception {
        // The explicit list is not reduced (see class comment)
        assertThat(count(SALES_USER, "FROM sales_eu,hr_data"), isForbidden());
    }

    @Test
    public void sales_wildcardIsReducedToPermittedIndices() throws Exception {
        // DNFOF: hr_data is removed from the expression, the query runs on sales_eu and sales_us only
        assertThat(count(SALES_USER, "FROM sales_*,hr_*"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
        assertThat(count(SALES_USER, "FROM *"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
        assertThat(count(SALES_USER, "FROM hr_*"), isForbidden());
    }

    @Test
    public void sales_viewWithPartiallyPermittedSources() throws Exception {
        // view_mixed is FROM sales_eu,hr_data; the view name is not covered by the sales role
        assertThat(count(SALES_USER, "FROM view_mixed"), isForbidden());
    }

    @Test
    public void sales_columnsOfForbiddenIndexAreNotResolved() throws Exception {
        // employee only exists in hr_data, which is removed from the wildcard expression; thus, it is an unknown column
        GenericRestClient.HttpResponse response = query(SALES_USER, "FROM sales_*,hr_* | KEEP employee");
        assertThat(response, isBadRequest());
        assertThat(response.getBody(), containsString("employee"));
    }

    // --- hr user

    @Test
    public void hr_permittedAndForbidden() throws Exception {
        assertThat(count(HR_USER, "FROM hr_data"), json(nodeAt("values", equalTo(List.of(List.of(2))))));
        assertThat(count(HR_USER, "FROM view_hr"), json(nodeAt("values", equalTo(List.of(List.of(2))))));
        assertThat(count(HR_USER, "FROM sales_eu"), isForbidden());
        assertThat(count(HR_USER, "FROM sales"), isForbidden());
        assertThat(count(HR_USER, "FROM view_sales_all"), isForbidden());
        assertThat(count(HR_USER, "FROM view_sales_eu_big"), isForbidden());
    }

    @Test
    public void hr_wildcardReducedToPermittedIndices() throws Exception {
        GenericRestClient.HttpResponse response = query(HR_USER, "FROM * | STATS c = COUNT(*) BY employee | SORT employee");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(1, "alice"), List.of(1, "bob"))))));
    }

    // --- alias only user

    @Test
    public void aliasUser_alias() throws Exception {
        assertThat(count(SALES_ALIAS_USER, "FROM sales"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
    }

    @Test
    public void aliasUser_memberIndicesArePermittedViaAlias() throws Exception {
        assertThat(count(SALES_ALIAS_USER, "FROM sales_eu"), json(nodeAt("values", equalTo(List.of(List.of(3))))));
        assertThat(count(SALES_ALIAS_USER, "FROM sales_us"), json(nodeAt("values", equalTo(List.of(List.of(2))))));
    }

    @Test
    public void aliasUser_otherIndicesAndViewsAreForbidden() throws Exception {
        assertThat(count(SALES_ALIAS_USER, "FROM hr_data"), isForbidden());
        assertThat(count(SALES_ALIAS_USER, "FROM view_sales_all"), isForbidden());
        assertThat(count(SALES_ALIAS_USER, "FROM view_hr"), isForbidden());
    }

    // --- LOOKUP JOIN

    @Test
    public void lookupJoin_withPermission() throws Exception {
        GenericRestClient.HttpResponse response = query(LOOKUP_USER,
                "FROM sales_eu | LOOKUP JOIN products ON product | STATS total = SUM(amount * price)");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(50 + 300 + 750))))));
    }

    @Test
    public void lookupJoin_withoutPermissionOnLookupIndex() throws Exception {
        assertThat(query(SALES_USER, "FROM sales_eu | LOOKUP JOIN products ON product | KEEP product, price"), isForbidden());
    }

    @Test
    public void lookupJoin_withoutPermissionOnSourceIndex() throws Exception {
        assertThat(query(LOOKUP_USER, "FROM hr_data | LOOKUP JOIN products ON product | KEEP product, price"), isForbidden());
    }

    @Test
    public void lookupJoin_dataNodeCheck() throws Exception {
        assertThat(query(LOOKUP_DATA_NODE_CHECK_USER, "FROM sales_eu | KEEP product"), isOk());
        assertThat(query(LOOKUP_DATA_NODE_CHECK_USER, "FROM sales_eu | LOOKUP JOIN products ON product | KEEP product, price"), isForbidden());
    }

    // --- ENRICH

    @Test
    public void enrich_withPermission() throws Exception {
        GenericRestClient.HttpResponse response = query(ENRICH_USER,
                "FROM sales_eu | ENRICH product_policy ON product | STATS c = COUNT(*) BY category | SORT category");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(1, "drupe"), List.of(2, "pome"))))));
    }

    @Test
    public void enrich_withoutClusterPermission() throws Exception {
        assertThat(query(SALES_USER, "FROM sales_eu | ENRICH product_policy ON product | KEEP product, category"), isForbidden());
    }

    @Test
    public void enrich_withoutPermissionOnSourceIndex() throws Exception {
        assertThat(query(ENRICH_USER, "FROM hr_data | ENRICH product_policy ON product | KEEP product, category"), isForbidden());
    }

    // --- data streams

    @Test
    public void dataStream_withPermission() throws Exception {
        int expectedCount = DS_SALES.getTestData().getRetainedDocuments().size();
        assertThat(count(DS_USER, "FROM ds_sales"), json(nodeAt("values", equalTo(List.of(List.of(expectedCount))))));
        assertThat(count(DS_USER, "FROM ds_*"), json(nodeAt("values", equalTo(List.of(List.of(expectedCount))))));
        assertThat(count(DS_USER, "FROM view_ds"), json(nodeAt("values", equalTo(List.of(List.of(expectedCount))))));
        assertThat(count(ALL_READ_USER, "FROM ds_sales"), json(nodeAt("values", equalTo(List.of(List.of(expectedCount))))));
    }

    @Test
    public void dataStream_withoutPermission() throws Exception {
        assertThat(count(SALES_USER, "FROM ds_sales"), isForbidden());
        assertThat(count(SALES_USER, "FROM view_ds"), isForbidden());
        assertThat(count(DS_USER, "FROM sales_eu"), isForbidden());
    }

    @Test
    public void aliasUser_wildcardReducedToAlias() throws Exception {
        GenericRestClient.HttpResponse response = query(SALES_ALIAS_USER, "FROM * METADATA _index | STATS c = COUNT(*) BY _index | SORT _index");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][1]", containsInAnyOrder("sales_eu", "sales_us"))));
    }
}
