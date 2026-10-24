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

package com.floragunn.searchguard.transport;

import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

import java.util.List;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Makes sure that the authorization of ES|QL stays complete.
 *
 * ES|QL is authorized in two ways: the requests which are TransportActions pass SearchGuardFilter, while the requests
 * which are plain transport requests are authorized by SearchGuardRequestHandler, which needs to know them by name (see
 * TRANSPORT_LEVEL_AUTHORIZED_ACTIONS). If Elasticsearch introduces a further ES|QL request which reads indices and we do
 * not add it there, it would not be authorized at all.
 *
 * This test executes ES|QL queries which use all features we support and then checks the transport actions which were
 * actually used against the classification below. A new ES|QL action makes this test fail, which forces a decision on how
 * it has to be authorized.
 *
 * The transport statistics only count requests which were received over the network, thus the cluster has dedicated data
 * nodes: that way the requests which read indices are sent from the coordinating node to another node. Actions which
 * Elasticsearch always sends to the local node (the enrich policy resolution and the dispatch of the query drivers) are
 * consequently not reported and cannot be covered by this test.
 *
 * ActionCompletenessIntTest cannot cover any of this: it runs on an embedded cluster, which has no ES|QL plugin at all.
 */
public class EsqlActionCompletenessIntTest {

    /**
     * ES|QL actions which are TransportActions and thus authorized by SearchGuardFilter
     */
    static final ImmutableSet<String> AUTHORIZED_BY_ACTION_FILTER = ImmutableSet.of(//
            "indices:data/read/esql", //
            "indices:data/read/esql/async/get", //
            "indices:data/read/esql/async/stop", //
            "indices:data/read/esql/resolve_views", //
            "indices:data/read/esql/resolve_fields", //
            "indices:data/read/esql/search_shards", //
            "indices:admin/esql/view/put", //
            "indices:admin/esql/view/get", //
            "indices:admin/esql/view/delete");

    /**
     * ES|QL actions which do not read any index data by themselves. These do not need to be authorized: the exchange
     * actions transport the pages of a running query between the nodes, and the compute action dispatches the drivers of a
     * query on the local node. Both only continue the work of a query which was authorized before.
     */
    static final ImmutableSet<String> NOT_READING_INDEX_DATA = ImmutableSet.of(//
            "indices:data/read/esql/compute", //
            "internal:data/read/esql/open_exchange", //
            "internal:data/read/esql/exchange", //
            "internal:data/read/esql/batch_exchange_status");

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new TestSgConfig.Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().clusterConfiguration(ClusterConfiguration.DEFAULT).sslEnabled().users(ADMIN)
            .enterpriseModulesEnabled().useExternalProcessCluster().build();

    @BeforeClass
    public static void createTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            assertThat(client.putJson("/sales", DocNode.of("settings.index.number_of_shards", 2, "mappings.properties.product.type", "keyword",
                    "mappings.properties.amount.type", "integer")), isOk());
            assertThat(client.putJson("/sales/_doc/1?refresh=true", DocNode.of("product", "apple", "amount", 1)), isCreated());

            assertThat(client.putJson("/_query/view/view_sales", DocNode.of("query", "FROM sales")), isOk());

            assertThat(client.putJson("/products", DocNode.of("settings.index.mode", "lookup", "mappings.properties.product.type", "keyword",
                    "mappings.properties.price.type", "integer")), isOk());
            assertThat(client.putJson("/products/_doc/1?refresh=true", DocNode.of("product", "apple", "price", 2)), isCreated());

            assertThat(client.putJson("/product_info",
                    DocNode.of("mappings.properties.product.type", "keyword", "mappings.properties.category.type", "keyword")), isOk());
            assertThat(client.putJson("/product_info/_doc/1?refresh=true", DocNode.of("product", "apple", "category", "pome")), isCreated());
            assertThat(client.putJson("/_enrich/policy/product_policy",
                    DocNode.of("match", DocNode.of("indices", "product_info", "match_field", "product", "enrich_fields", List.of("category")))),
                    isOk());
            assertThat(client.put("/_enrich/policy/product_policy/_execute"), isOk());
        }
    }

    @Test
    public void esqlActionsAreEitherAuthorizedOrKnownToNotReadIndexData() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            // Use every ES|QL feature we support, so that all of its transport actions are used at least once
            assertThat(client.postJson("/_query", DocNode.of("query", "FROM sales | STATS c = COUNT(*)")), isOk());
            assertThat(client.postJson("/_query", DocNode.of("query", "FROM view_sales | KEEP product")), isOk());
            assertThat(client.postJson("/_query", DocNode.of("query", "FROM sales | LOOKUP JOIN products ON product | KEEP price")), isOk());
            assertThat(client.postJson("/_query", DocNode.of("query", "FROM sales | ENRICH product_policy ON product | KEEP category")), isOk());

            GenericRestClient.HttpResponse response = client.postJson("/_query/async",
                    DocNode.of("query", "FROM sales | STATS c = COUNT(*)", "wait_for_completion_timeout", "0s", "keep_on_completion", true));
            assertThat(response, isOk());
            String asyncId = response.getBodyAsDocNode().getAsString("id");
            assertThat(client.get("/_query/async/" + asyncId + "?wait_for_completion_timeout=30s"), isOk());
        }

        TreeSet<String> observed = new TreeSet<>();

        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = client.get("/_nodes/stats/transport");
            assertThat(response, isOk());

            for (DocNode actions : response.getBodyAsDocNode().findNodesByJsonPath("nodes.*.transport.actions")) {
                for (String action : actions.keySet()) {
                    if (isEsqlAction(action)) {
                        observed.add(action);
                    }
                }
            }
        }

        // Make sure that the transport statistics really report the actions of the queries above. Otherwise, this test
        // would silently pass without checking anything. The data requests are always sent to another node, as the
        // coordinating node of this cluster holds no data.
        assertThat(observed, hasItem("indices:data/read/esql/data"));

        TreeSet<String> unclassified = new TreeSet<>(observed);
        unclassified.removeAll(AUTHORIZED_BY_ACTION_FILTER);
        unclassified.removeAll(SearchGuardRequestHandler.TRANSPORT_LEVEL_AUTHORIZED_ACTIONS);
        unclassified.removeAll(NOT_READING_INDEX_DATA);

        if (!unclassified.isEmpty()) {
            Assert.fail("Found ES|QL transport actions which are not classified by this test: " + unclassified
                    + "\nDecide for each action whether it reads index data. If it does and it is not a TransportAction,"
                    + " it must be added to SearchGuardRequestHandler.TRANSPORT_LEVEL_AUTHORIZED_ACTIONS."
                    + "\nAuthorized by SearchGuardFilter: " + AUTHORIZED_BY_ACTION_FILTER
                    + "\nAuthorized by SearchGuardRequestHandler: " + SearchGuardRequestHandler.TRANSPORT_LEVEL_AUTHORIZED_ACTIONS
                    + "\nNot reading index data: " + NOT_READING_INDEX_DATA);
        }
    }

    private static boolean isEsqlAction(String action) {
        return action.contains("/esql") || action.contains("esql/");
    }
}
