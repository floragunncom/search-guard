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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import java.net.InetSocketAddress;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Cross cluster ES|QL queries, with and without views and data streams.
 *
 * How cross cluster ES|QL is authorized: The coordinating cluster sends indices:data/read/esql/resolve_fields (a regular
 * action, checked by SearchGuardFilter on the remote cluster) and the transport-only request indices:data/read/esql/cluster
 * to the remote cluster. The remote cluster then runs its own indices:data/read/esql/search_shards and sends
 * indices:data/read/esql/data requests to its own data nodes. The user is forwarded to the remote cluster by Search Guard;
 * indices:data/read/esql/cluster and indices:data/read/esql/data are checked in SearchGuardRequestHandler on the remote
 * cluster against the user's roles configured on the remote cluster. See EsqlViewIntTest for an overview.
 *
 * Elasticsearch does not support remote views (FROM remote:view fails). A local view may reference remote indices.
 *
 * Denied access on the remote cluster: The remote cluster denies indices:data/read/esql/resolve_fields with a
 * security_exception (403). ES|QL does not treat this like an unavailable cluster: If no other index of the query
 * resolves, the query fails with that 403, regardless of skip_unavailable. If other indices resolve, the remote failure
 * is kept as cluster failure; the remote cluster is then skipped (HTTP 200, status "skipped" and an "Unknown index"
 * failure in _clusters.details) with skip_unavailable=true, or the query fails with "Unknown index" (HTTP 400) with
 * skip_unavailable=false. _clusters.details is only included with include_ccs_metadata=true.
 */
public class EsqlCcsIntTest {

    private static final TestCertificates certificatesContext = TestCertificates.builder()
            .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard").addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard").build();

    static final TestDataStream DS_REMOTE = TestDataStream.name("ds_remote").documentCount(10).seed(3).build();

    /**
     * On the coordinating cluster, the user has privileges on the local indices l_* and on all views. On the remote
     * cluster, the user has privileges on r_sales and on the data stream ds_remote, but not on r_hr.
     */
    static final TestSgConfig.User CCS_USER_COORD = new TestSgConfig.User("ccs_user").roles(new Role("ccs_user")
            .clusterPermissions("indices:data/read/esql/async/get", "indices:data/read/esql/async/stop").indexPermissions("SGS_READ")
            .on("l_*", "view_*"));
    static final TestSgConfig.User CCS_USER_REMOTE = new TestSgConfig.User("ccs_user").roles(new Role("ccs_user").clusterPermissions()
            .indexPermissions("SGS_READ").on("r_sales").dataStreamPermissions("SGS_READ").on("ds_*"));

    /**
     * Has no privileges on the remote cluster at all
     */
    static final TestSgConfig.User LOCAL_ONLY_USER_COORD = new TestSgConfig.User("local_only_user")
            .roles(new Role("local_only_user").clusterPermissions().indexPermissions("SGS_READ").on("l_*", "view_*"));
    static final TestSgConfig.User LOCAL_ONLY_USER_REMOTE = new TestSgConfig.User("local_only_user")
            .roles(new Role("local_only_user").clusterPermissions().indexPermissions().on());

    @ClassRule
    public static LocalCluster anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
            .clusterName("remote_cluster").users(CCS_USER_REMOTE, LOCAL_ONLY_USER_REMOTE).enterpriseModulesEnabled()
            .indexTemplates(TestIndexTemplate.DATA_STREAM_MINIMAL).dataStreams(DS_REMOTE).useExternalProcessCluster().build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext).remote("my_remote", anotherCluster)
            .users(CCS_USER_COORD, LOCAL_ONLY_USER_COORD).authzDebug(true).enterpriseModulesEnabled().useExternalProcessCluster().build();

    @BeforeClass
    public static void createTestData() throws Exception {
        // Elasticsearch requires an enterprise (or trial) license for cross cluster ES|QL queries
        for (LocalCluster c : new LocalCluster[] { anotherCluster, cluster }) {
            try (GenericRestClient client = c.getAdminCertRestClient()) {
                assertThat(client.post("/_license/start_trial?acknowledge=true"), isOk());
            }
        }

        // The remote cluster is configured as dynamic setting, so that skip_unavailable can be changed by the tests
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            InetSocketAddress remoteAddress = anotherCluster.getTransportAddress();
            assertThat(client.putJson("/_cluster/settings",
                    DocNode.of("persistent", DocNode.of("cluster.remote.my_remote.seeds", List.of(remoteAddress.getHostString() + ":" + remoteAddress.getPort()),
                            "cluster.remote.my_remote.skip_unavailable", true))),
                    isOk());
        }

        try (GenericRestClient client = anotherCluster.getAdminCertRestClient()) {
            createIndex(client, "r_sales");
            createIndex(client, "r_hr");
            indexDoc(client, "r_sales", "1", DocNode.of("product", "apple", "amount", 10));
            indexDoc(client, "r_sales", "2", DocNode.of("product", "pear", "amount", 20));
            indexDoc(client, "r_sales", "3", DocNode.of("product", "plum", "amount", 30));
            indexDoc(client, "r_hr", "1", DocNode.of("product", "n/a", "amount", 5000));
            indexDoc(client, "r_hr", "2", DocNode.of("product", "n/a", "amount", 6000));
            // A view with the same name as a local view; remote views are not supported by Elasticsearch
            createView(client, "view_remote", "FROM r_sales");
        }

        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            createIndex(client, "l_sales");
            indexDoc(client, "l_sales", "1", DocNode.of("product", "apple", "amount", 1));
            indexDoc(client, "l_sales", "2", DocNode.of("product", "pear", "amount", 2));
            createView(client, "view_remote", "FROM my_remote:r_sales");
            createView(client, "view_both", "FROM l_sales,my_remote:r_sales");
            createView(client, "view_remote_hr", "FROM my_remote:r_hr");
        }
    }

    private static void createIndex(GenericRestClient client, String index) throws Exception {
        assertThat(client.putJson("/" + index, DocNode.of("settings.index.number_of_shards", 2, "mappings.properties.product.type", "keyword",
                "mappings.properties.amount.type", "integer")), isOk());
    }

    private static void indexDoc(GenericRestClient client, String index, String id, DocNode doc) throws Exception {
        assertThat(client.putJson("/" + index + "/_doc/" + id + "?refresh=true", doc), isCreated());
    }

    private static void createView(GenericRestClient client, String name, String query) throws Exception {
        assertThat(client.putJson("/_query/view/" + name, DocNode.of("query", query)), isOk());
    }

    private static GenericRestClient.HttpResponse query(TestSgConfig.User user, String query) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            return client.postJson("/_query", DocNode.of("query", query, "include_ccs_metadata", true));
        }
    }

    private static GenericRestClient.HttpResponse count(TestSgConfig.User user, String from) throws Exception {
        return query(user, from + " | STATS c = COUNT(*)");
    }

    private static void setSkipUnavailable(boolean skipUnavailable) throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            assertThat(client.putJson("/_cluster/settings", DocNode.of("persistent", DocNode.of("cluster.remote.my_remote.skip_unavailable", skipUnavailable))),
                    isOk());
        }
    }

    @Test
    public void remoteIndex() throws Exception {
        assertThat(count(CCS_USER_COORD, "FROM my_remote:r_sales"), json(nodeAt("values", equalTo(List.of(List.of(3))))));
        assertThat(count(CCS_USER_COORD, "FROM my_remote:r_sales"), json(nodeAt("_clusters.details.my_remote.status", equalTo("successful"))));
    }

    @Test
    public void localAndRemoteIndex() throws Exception {
        assertThat(count(CCS_USER_COORD, "FROM l_sales,my_remote:r_sales"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
    }

    @Test
    public void remoteWildcardIsReducedToPermittedIndices() throws Exception {
        assertThat(count(CCS_USER_COORD, "FROM my_remote:r_*"), json(nodeAt("values", equalTo(List.of(List.of(3))))));
    }

    @Test
    public void remoteDataStream() throws Exception {
        assertThat(count(CCS_USER_COORD, "FROM my_remote:ds_remote"),
                json(nodeAt("values", equalTo(List.of(List.of(DS_REMOTE.getTestData().getRetainedDocuments().size()))))));
    }

    @Test
    public void remoteForbiddenIndex_skipUnavailable() throws Exception {
        setSkipUnavailable(true);
        assertThat(count(CCS_USER_COORD, "FROM my_remote:r_hr"), isForbidden());
        assertThat(count(CCS_USER_COORD, "FROM view_remote_hr"), isForbidden());

        GenericRestClient.HttpResponse response = count(CCS_USER_COORD, "FROM l_sales,my_remote:r_hr");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(2))))));
        assertThat(response, json(nodeAt("_clusters.details.my_remote.status", equalTo("skipped"))));
        // Elasticsearch records the skipped cluster with an "Unknown index" error, not with the original security_exception
        assertThat(response, json(nodeAt("_clusters.details.my_remote.failures[0].reason.type", equalTo("verification_exception"))));
        assertThat(response, json(nodeAt("_clusters.details.my_remote.failures[0].reason.reason", equalTo("Unknown index [my_remote:r_hr]"))));
    }

    @Test
    public void remoteForbiddenIndex_noSkipUnavailable() throws Exception {
        setSkipUnavailable(false);
        try {
            assertThat(count(CCS_USER_COORD, "FROM my_remote:r_hr"), isForbidden());
            assertThat(count(CCS_USER_COORD, "FROM view_remote_hr"), isForbidden());
            assertThat(count(CCS_USER_COORD, "FROM l_sales,my_remote:r_hr"), isBadRequest("error.reason", "*Unknown index [my_remote:r_hr]*"));
        } finally {
            setSkipUnavailable(true);
        }
    }

    @Test
    public void userWithoutRemotePrivileges_noSkipUnavailable() throws Exception {
        setSkipUnavailable(false);
        try {
            assertThat(count(LOCAL_ONLY_USER_COORD, "FROM l_sales"), json(nodeAt("values", equalTo(List.of(List.of(2))))));
            assertThat(count(LOCAL_ONLY_USER_COORD, "FROM my_remote:r_sales"), isForbidden());
            assertThat(count(LOCAL_ONLY_USER_COORD, "FROM view_remote"), isForbidden());
        } finally {
            setSkipUnavailable(true);
        }
    }

    @Test
    public void localViewOverRemoteIndex() throws Exception {
        assertThat(count(CCS_USER_COORD, "FROM view_remote"), json(nodeAt("values", equalTo(List.of(List.of(3))))));
        assertThat(count(CCS_USER_COORD, "FROM view_both"), json(nodeAt("values", equalTo(List.of(List.of(5))))));
    }

    /**
     * An asynchronous cross cluster query. The remote part is authorized in the same way as for a synchronous query; the
     * async result is owned by the submitting user (see EsqlAsyncIntTest).
     */
    @Test
    public void asyncRemoteQuery() throws Exception {
        setSkipUnavailable(true);
        String asyncId;

        try (GenericRestClient client = cluster.getRestClient(CCS_USER_COORD)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query/async",
                    DocNode.of("query", "FROM l_sales,my_remote:r_sales | STATS c = COUNT(*)", "wait_for_completion_timeout", "0s",
                            "keep_on_completion", true, "include_ccs_metadata", true));
            assertThat(response, isOk());
            asyncId = response.getBodyAsDocNode().getAsString("id");
            assertThat(response.getBody(), asyncId, notNullValue());
        }

        try (GenericRestClient client = cluster.getRestClient(CCS_USER_COORD)) {
            GenericRestClient.HttpResponse response = client.get("/_query/async/" + asyncId + "?wait_for_completion_timeout=30s");
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(5))))));
            assertThat(response, json(nodeAt("_clusters.details.my_remote.status", equalTo("successful"))));
        }
    }

    @Test
    public void asyncRemoteQuery_forbiddenRemoteIndex() throws Exception {
        setSkipUnavailable(true);

        try (GenericRestClient client = cluster.getRestClient(CCS_USER_COORD)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query/async",
                    DocNode.of("query", "FROM my_remote:r_hr | STATS c = COUNT(*)", "wait_for_completion_timeout", "30s", "keep_on_completion",
                            true, "include_ccs_metadata", true));
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void remoteViewIsNotSupportedByElasticsearch() throws Exception {
        GenericRestClient.HttpResponse response = count(CCS_USER_COORD, "FROM my_remote:view_remote");
        assertThat(response, not(isOk()));
    }
}
