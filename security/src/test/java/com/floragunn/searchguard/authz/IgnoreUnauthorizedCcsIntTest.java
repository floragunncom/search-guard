package com.floragunn.searchguard.authz;

import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.isBadRequest;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.matches;
import static com.floragunn.searchguard.test.RestMatchers.matchesDocCount;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.helper.PitHolder;
import com.floragunn.searchguard.test.helper.cluster.EsClientProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;


import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class IgnoreUnauthorizedCcsIntTest {

    @Parameter
    public String ccsMinimizeRoundtrips;

    @Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] { "ccs_minimize_roundtrips=false", "ccs_minimize_roundtrips=true" };
    }

    private static TestCertificates certificatesContext = TestCertificates.builder().ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard").addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard").build();

    static TestSgConfig.User LIMITED_USER_COORD_A = new TestSgConfig.User("limited_user_A").roles(//
            new Role("limited_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("a*"));

    static TestSgConfig.User LIMITED_USER_REMOTE_A = new TestSgConfig.User("limited_user_A").roles(//
            new Role("limited_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD",
                    "indices:admin/search/search_shards", "indices:admin/shards/search_shards", "indices:admin/resolve/cluster").on("a*"));

    static TestSgConfig.User LIMITED_USER_COORD_B = new TestSgConfig.User("limited_user_B").roles(//
            new Role("limited_user_b_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("b*"));

    static TestSgConfig.User LIMITED_USER_REMOTE_B = new TestSgConfig.User("limited_user_B").roles(//
            new Role("limited_user_b_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")
                    .indexPermissions("SGS_CRUD", "indices:admin/search/search_shards", "indices:admin/shards/search_shards").on("b*"));

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user").roles(//
            new Role("unlimited_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD",
                    "indices:admin/search/search_shards", "indices:admin/shards/search_shards", "indices:admin/resolve/cluster").on("*"));

    static TestIndex index_coord_a1 = TestIndex.name("a1").documentCount(100).seed(1).attr("prefix", "a").attr("cluster", "local").build();
    static TestIndex index_coord_a2 = TestIndex.name("a2").documentCount(110).seed(2).attr("prefix", "a").attr("cluster", "local").build();
    static TestIndex index_remote_a1 = TestIndex.name("a1").documentCount(120).seed(11).attr("prefix", "a").attr("cluster", "remote").build();
    static TestIndex index_remote_a2 = TestIndex.name("a2").documentCount(130).seed(12).attr("prefix", "a").attr("cluster", "remote").build();
    static TestIndex index_coord_b1 = TestIndex.name("b1").documentCount(51).seed(4).attr("prefix", "b").attr("cluster", "local").build();
    static TestIndex index_coord_b2 = TestIndex.name("b2").documentCount(52).seed(5).attr("prefix", "b").attr("cluster", "local").build();
    static TestIndex index_remote_b1 = TestIndex.name("b1").documentCount(53).seed(14).attr("prefix", "b").attr("cluster", "remote").build();
    static TestIndex index_remote_b2 = TestIndex.name("b2").documentCount(54).seed(15).attr("prefix", "b").attr("cluster", "remote").build();
    static TestIndex index_coord_c1 = TestIndex.name("c1").documentCount(5).seed(7).attr("prefix", "c").attr("cluster", "local").build();
    static TestIndex index_remote_r1 = TestIndex.name("r1").documentCount(5).seed(8).attr("prefix", "r").attr("cluster", "remote").build();

    static TestAlias xalias_coord_ab1 = new TestAlias("xalias_ab1", index_coord_a1, index_coord_a2, index_coord_b1);
    static TestAlias xalias_remote_ab1 = new TestAlias("xalias_ab1", index_remote_a1, index_remote_a2, index_remote_b1);

    static NamedWriteableRegistry nameRegistry;

    @ClassRule
    public static LocalCluster.Embedded anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
            .nodeSettings("searchguard.diagnosis.action_stack.enabled", true).users(LIMITED_USER_REMOTE_A, LIMITED_USER_REMOTE_B, UNLIMITED_USER)//
            .indices(index_remote_a1, index_remote_a2, index_remote_b1, index_remote_b2, index_remote_r1)//
            .aliases(xalias_remote_ab1)//
            .embedded().build();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
            .remote("my_remote", anotherCluster).nodeSettings("searchguard.diagnosis.action_stack.enabled", true)
            .nodeSettings("cluster.remote.my_remote.skip_unavailable", true).users(LIMITED_USER_COORD_A, LIMITED_USER_COORD_B, UNLIMITED_USER)//
            .indices(index_coord_a1, index_coord_a2, index_coord_b1, index_coord_b2, index_coord_c1)//
            .aliases(xalias_coord_ab1)//
            .embedded().build();

    @BeforeClass
    public static void beforeClass() {
        nameRegistry = cluster.getInjectable(NamedWriteableRegistry.class);
    }

    @Test
    public void search_noPattern() throws Exception {
        String query = "/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("hits.hits[*]", matches(index_coord_a1, index_coord_a2, index_coord_b1, index_coord_b2, index_coord_c1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches(index_coord_a1, index_coord_a2))));
        }
    }
    
    @Test
    public void search_localWildcard() throws Exception {
        String query = "*/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("hits.hits[*]", matches(index_coord_a1, index_coord_a2, index_coord_b1, index_coord_b2, index_coord_c1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches(index_coord_a1, index_coord_a2))));
        }
    }

    @Test
    public void search_localWildcard_withPit() throws Exception {
        String query = "/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (
                GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER);
                PitHolder pitHolder = PitHolder.of(restClient).post("/"+ "*" + "/_pit?keep_alive=1m")) {

            HttpResponse httpResponse = restClient.postJson(query, pitHolder.asSearchBody());

            if (ccsMinimizeRoundtrips.equals("ccs_minimize_roundtrips=true")) {
                Assert.assertThat(httpResponse, isBadRequest());
                Assert.assertThat(httpResponse, json(nodeAt("error.reason", containsString("[ccs_minimize_roundtrips] cannot be used with point in time"))));
            } else {
                Assert.assertThat(httpResponse, isOk());
                TestIndex[] expectedIndices = { index_coord_a1, index_coord_a2, index_coord_b1, index_coord_b2, index_coord_c1 };
                Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches(expectedIndices))));
                String[] expectedIndicesNames = ImmutableList.ofArray(expectedIndices) //
                        .map(TestIndex::getName)  //
                        .toArray(size -> new String[size]);
                Assert.assertThat(pitHolder.extractIndicesFromPit(nameRegistry), arrayContainingInAnyOrder(expectedIndicesNames));
            }
        }

        try (
                GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A);
                PitHolder pitHolder = PitHolder.of(restClient).post("/*/_pit?keep_alive=1m")) {
            HttpResponse httpResponse = restClient.postJson(query, pitHolder.asSearchBody());

            if (ccsMinimizeRoundtrips.equals("ccs_minimize_roundtrips=true")) {
                Assert.assertThat(httpResponse, isBadRequest());
                Assert.assertThat(httpResponse, json(nodeAt("error.reason", containsString("[ccs_minimize_roundtrips] cannot be used with point in time"))));
            } else {
                Assert.assertThat(httpResponse, isOk());
                Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches(index_coord_a1, index_coord_a2))));
                Assert.assertThat(
                        pitHolder.extractIndicesFromPit(nameRegistry),
                        arrayContainingInAnyOrder(index_coord_a1.getName(), index_coord_a2.getName()));
            }
        }
    }

    @Test
    public void search_localAll() throws Exception {
        String query = "_all/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("hits.hits[*]", matches(index_coord_a1, index_coord_a2, index_coord_b1, index_coord_b2, index_coord_c1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches(index_coord_a1, index_coord_a2))));
        }
    }

    @Test
    public void search_remoteWildcard() throws Exception {
        String query = "my_remote:*/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]",
                    matches("my_remote", index_remote_a1, index_remote_a2, index_remote_b1, index_remote_b2, index_remote_r1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches("my_remote", index_remote_a1, index_remote_a2))));
        }

    }

    @Test
    public void search_remoteWildcard_withPit() throws Exception {
        String query = "/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (
            GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER);
            PitHolder pitHolder = PitHolder.of(restClient).post("/my_remote:*/_pit?keep_alive=1m")) {
            HttpResponse httpResponse = restClient.postJson(query, pitHolder.asSearchBody());

            if (ccsMinimizeRoundtrips.equals("ccs_minimize_roundtrips=true")) {
                Assert.assertThat(httpResponse, isBadRequest());
                Assert.assertThat(httpResponse, json(nodeAt("error.reason", containsString("[ccs_minimize_roundtrips] cannot be used with point in time"))));
            } else {
                TestIndex[] expectedIndices = { index_remote_a1, index_remote_a2, index_remote_b1, index_remote_b2, index_remote_r1 };
                int expectedIndicesDocCount = Stream.of(expectedIndices).mapToInt(testIndex -> testIndex.getTestData().getRetainedDocuments().size()).sum();

                Assert.assertThat(httpResponse, isOk());
                Assert.assertThat(httpResponse, json(nodeAt("hits.hits[*]", hasSize(expectedIndicesDocCount))));
                Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches("my_remote", expectedIndices))));

                String[] expectedIndicesWithRemoteClusterPrefix = ImmutableList.ofArray(expectedIndices) //
                        .map(TestIndex::getName)  //
                        .map(indexName -> "my_remote:" + indexName) //
                        .toArray(size -> new String[size]);
                // test contract with ES - indices name are expected
                Assert.assertThat(
                        pitHolder.extractIndicesFromPit(nameRegistry),
                        arrayContainingInAnyOrder(expectedIndicesWithRemoteClusterPrefix));
            }
        }

        try (
            GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A);
            PitHolder pitHolder = PitHolder.of(restClient).post("/my_remote:*/_pit?keep_alive=1m")) {

            HttpResponse httpResponse = restClient.postJson(query, pitHolder.asSearchBody());

            if (ccsMinimizeRoundtrips.equals("ccs_minimize_roundtrips=true")) {
                Assert.assertThat(httpResponse, isBadRequest());
                Assert.assertThat(httpResponse, json(nodeAt("error.reason", containsString("[ccs_minimize_roundtrips] cannot be used with point in time"))));
            } else {
                TestIndex[] expectedIndices = { index_remote_a1, index_remote_a2 };
                int expectedIndicesDocCount = Stream.of(expectedIndices).mapToInt(testIndex -> testIndex.getTestData().getRetainedDocuments().size()).sum();

                Assert.assertThat(httpResponse, isOk());
                Assert.assertThat(httpResponse, json(nodeAt("hits.hits[*]", hasSize(expectedIndicesDocCount))));
                Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches("my_remote", expectedIndices))));

                String[] expectedIndicesWithRemoteClusterPrefix = ImmutableList.ofArray(expectedIndices) //
                        .map(TestIndex::getName)  //
                        .map(indexName -> "my_remote:" + indexName) //
                        .toArray(size -> new String[size]);
                // test contract with ES - indices name are expected
                Assert.assertThat(
                        pitHolder.extractIndicesFromPit(nameRegistry),
                        arrayContainingInAnyOrder(expectedIndicesWithRemoteClusterPrefix));
            }

        }

    }

    @Test
    public void search_remoteAll() throws Exception {
        String query = "my_remote:_all/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]",
                    matches("my_remote", index_remote_a1, index_remote_a2, index_remote_b1, index_remote_b2, index_remote_r1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches("my_remote", index_remote_a1, index_remote_a2))));
        }

    }

    @Test
    public void search_wildcardWildcard() throws Exception {
        String query = "*:*/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]",
                    matches("my_remote", index_remote_a1, index_remote_a2, index_remote_b1, index_remote_b2, index_remote_r1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches("my_remote", index_remote_a1, index_remote_a2))));
        }
    }

    @Test
    public void search_clusterWildcard() throws Exception {
        String query = "*:/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("hits.hits", empty())));
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.details.my_remote.status", equalTo("skipped"))));
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.details.my_remote.failures", hasSize(1))));
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.details.my_remote.failures[0].reason.reason", containsString("no such index"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("hits.hits", empty())));
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.details.my_remote.status", equalTo("skipped"))));
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.details.my_remote.failures", hasSize(1))));
            Assert.assertThat(httpResponse,
                    json(nodeAt("_clusters.details.my_remote.failures[0].reason.reason", equalTo("Insufficient permissions"))));
        }
    }

    @Test
    @Ignore("NoSuchRemoteClusterException: no such remote cluster: []")
    public void search_emptyClusterName() throws Exception {
        String query = ":*/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", empty())));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", empty())));
        }
    }

    @Test
    public void search_indexPattern() throws Exception {
        String query = "my_remote:a*/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches("my_remote", index_remote_a1, index_remote_a2))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches("my_remote", index_remote_a1, index_remote_a2))));
        }

    }

    @Test
    public void search_indexPattern_withPit() throws Exception {
        String query = "/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER);
                PitHolder pitHolder = PitHolder.of(restClient).post("/my_remote:a*/_pit?keep_alive=1m")) {

            HttpResponse httpResponse = restClient.postJson(query, pitHolder.asSearchBody());

            if (ccsMinimizeRoundtrips.equals("ccs_minimize_roundtrips=true")) {
                Assert.assertThat(httpResponse, isBadRequest());
                Assert.assertThat(httpResponse,
                        json(nodeAt("error.reason", containsString("[ccs_minimize_roundtrips] cannot be used with point in time"))));
            } else {
                Assert.assertThat(httpResponse, isOk());
                Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches("my_remote", index_remote_a1, index_remote_a2))));
                // test contract with ES - indices name are expected
                Assert.assertThat(pitHolder.extractIndicesFromPit(nameRegistry), arrayContainingInAnyOrder("my_remote:a1", "my_remote:a2"));
            }
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A);
                PitHolder pitHolder = PitHolder.of(restClient).post("/my_remote:a*/_pit?keep_alive=1m")) {

            HttpResponse httpResponse = restClient.postJson(query, pitHolder.asSearchBody());

            if (ccsMinimizeRoundtrips.equals("ccs_minimize_roundtrips=true")) {
                Assert.assertThat(httpResponse, isBadRequest());
                Assert.assertThat(httpResponse,
                        json(nodeAt("error.reason", containsString("[ccs_minimize_roundtrips] cannot be used with point in time"))));
            } else {
                Assert.assertThat(httpResponse, isOk());
                Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches("my_remote", index_remote_a1, index_remote_a2))));
                // test contract with ES - indices name are expected
                Assert.assertThat(pitHolder.extractIndicesFromPit(nameRegistry), arrayContainingInAnyOrder("my_remote:a1", "my_remote:a2"));
            }
        }

    }

    @Test
    public void search_staticIndices() throws Exception {
        String query = "my_remote:b1/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("my_remote:b1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("hits.hits", empty())));
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.details.my_remote.status", equalTo("skipped"))));
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.details.my_remote.failures", hasSize(1))));
            Assert.assertThat(httpResponse,
                    json(nodeAt("_clusters.details.my_remote.failures[0].reason.reason", equalTo("Insufficient permissions"))));
        }

    }

    @Test
    public void search_staticIndices_pit() throws Exception {
        String query = "/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER);
                PitHolder pitHolder = PitHolder.of(restClient).post("/my_remote:b1/_pit?keep_alive=1m")) {

            HttpResponse httpResponse = restClient.postJson(query, pitHolder.asSearchBody());

            if (ccsMinimizeRoundtrips.equals("ccs_minimize_roundtrips=true")) {
                Assert.assertThat(httpResponse, isBadRequest());
                Assert.assertThat(httpResponse,
                        json(nodeAt("error.reason", containsString("[ccs_minimize_roundtrips] cannot be used with point in time"))));
            } else {
                Assert.assertThat(httpResponse, isOk());
                Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("my_remote:b1"))));
                // test contract with ES - indices name are expected
                Assert.assertThat(pitHolder.extractIndicesFromPit(nameRegistry), arrayContainingInAnyOrder("my_remote:b1"));
            }
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A);
                PitHolder pitHolder = PitHolder.of(restClient).post("/my_remote:b1/_pit?keep_alive=1m")) {

            Assert.assertThat(pitHolder.getResponse(), isOk());
            Assert.assertThat(pitHolder.extractIndicesFromPit(nameRegistry), emptyArray());
        }

    }

    @Test
    public void search_staticIndices_ignoreUnavailable() throws Exception {
        String query = "my_remote:b1/_search?size=1000&ignore_unavailable=true&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", containsInAnyOrder("my_remote:b1"))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]._index", empty())));
        }

    }

    @Test
    public void search_staticIndicesRemoteAndLocal() throws Exception {
        String query = "my_remote:b1,b1,my_remote:a1,a1/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches(
                    ImmutableMap.of("a1", index_coord_a1, "b1", index_coord_b1, "my_remote:a1", index_remote_a1, "my_remote:b1", index_remote_b1)))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isForbidden());
        }

    }

    @Test
    public void search_staticIndicesRemoteAndLocal_ignoreUnavailable() throws Exception {
        String query = "my_remote:b1,b1,my_remote:a1,a1/_search?size=1000&ignore_unavailable=true&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("hits.hits[*]", matches(
                    ImmutableMap.of("a1", index_coord_a1, "b1", index_coord_b1, "my_remote:a1", index_remote_a1, "my_remote:b1", index_remote_b1)))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("hits.hits[*]", matches(ImmutableMap.of("a1", index_coord_a1, "my_remote:a1", index_remote_a1)))));
        }
    }

    @Test
    public void search_remoteAlias() throws Exception {
        String query = "my_remote:xalias_ab1/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("hits.hits[*]", matches("my_remote", index_remote_a1, index_remote_a2, index_remote_b1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, json(nodeAt("hits.hits", empty())));
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.details.my_remote.status", equalTo("skipped"))));
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.details.my_remote.failures", hasSize(1))));
            Assert.assertThat(httpResponse,
                    json(nodeAt("_clusters.details.my_remote.failures[0].reason.reason", equalTo("Insufficient permissions"))));
        }

    }

    @Test
    public void msearch_staticIndicesInURL() throws Exception {
        String query = "my_remote:a1/_msearch?" + ccsMinimizeRoundtrips;
        String msearchBody = "{}\n" //
                + "{\"size\":1000, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson(query, msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].hits.hits[*]", matches("my_remote", index_remote_a1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.postJson(query, msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].hits.hits[*]", matches("my_remote", index_remote_a1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_B)) {
            HttpResponse httpResponse = restClient.postJson(query, msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].hits.hits", containsInAnyOrder(empty()))));
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*]._clusters.details.my_remote.status", containsInAnyOrder("skipped"))));
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("responses[*]._clusters.details.my_remote.failures", containsInAnyOrder(hasSize(1)))));
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*]._clusters.details.my_remote.failures[0].reason.reason",
                    containsInAnyOrder("Insufficient permissions"))));
        }
    }

    @Test
    public void msearch_staticIndicesInURL_remoteAndLocal() throws Exception {
        String query = "a1,my_remote:a1/_msearch?" + ccsMinimizeRoundtrips;
        String msearchBody = "{}\n" //
                + "{\"size\":1000, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson(query, msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(
                    distinctNodesAt("responses[*].hits.hits[*]", matches(ImmutableMap.of("a1", index_coord_a1, "my_remote:a1", index_remote_a1)))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.postJson(query, msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(
                    distinctNodesAt("responses[*].hits.hits[*]", matches(ImmutableMap.of("a1", index_coord_a1, "my_remote:a1", index_remote_a1)))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_B)) {
            HttpResponse httpResponse = restClient.postJson(query, msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].error.type", containsInAnyOrder("security_exception"))));
        }
    }

    @Test
    public void msearch_staticIndicesInURL_remoteAndLocal_ignoreUnavailable() throws Exception {
        String query = "a1,my_remote:a1,b1,my_remote:b1/_msearch?" + ccsMinimizeRoundtrips;
        String msearchBody = "{\"ignore_unavailable\": true}\n" //
                + "{\"size\":1000, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson(query, msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].hits.hits[*]", matches(
                    ImmutableMap.of("a1", index_coord_a1, "my_remote:a1", index_remote_a1, "b1", index_coord_b1, "my_remote:b1", index_remote_b1)))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.postJson(query, msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(
                    distinctNodesAt("responses[*].hits.hits[*]", matches(ImmutableMap.of("a1", index_coord_a1, "my_remote:a1", index_remote_a1)))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_B)) {
            HttpResponse httpResponse = restClient.postJson(query, msearchBody);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(
                    distinctNodesAt("responses[*].hits.hits[*]", matches(ImmutableMap.of("b1", index_coord_b1, "my_remote:b1", index_remote_b1)))));
        }
    }

    @Test
    public void search_indicesAggregation_localWildcard() throws Exception {
        String query = "*/_search?" + ccsMinimizeRoundtrips;
        String body = "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":1000}}}}";

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("aggregations.indices.buckets",
                    matchesDocCount(index_coord_a1, index_coord_a2, index_coord_b1, index_coord_b2, index_coord_c1))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("aggregations.indices.buckets", matchesDocCount(index_coord_a1, index_coord_a2))));
        }

    }

    @Test
    public void search_indicesAggregation_localAndRemoteWildcard() throws Exception {
        String query = "my_remote:*,*/_search?" + ccsMinimizeRoundtrips;
        String body = "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":1000}}}}";

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("aggregations.indices.buckets",
                            matchesDocCount(ImmutableMap.of("a1", index_coord_a1).with("a2", index_coord_a2).with("b1", index_coord_b1)
                                    .with("b2", index_coord_b2).with("c1", index_coord_c1).with("my_remote:a1", index_remote_a1)
                                    .with("my_remote:a2", index_remote_a2).with("my_remote:b1", index_remote_b1).with("my_remote:b2", index_remote_b2)
                                    .with("my_remote:r1", index_remote_r1)))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("aggregations.indices.buckets", matchesDocCount(ImmutableMap.of("a1", index_coord_a1)
                    .with("a2", index_coord_a2).with("my_remote:a1", index_remote_a1).with("my_remote:a2", index_remote_a2)))));
        }

    }

    @Test
    public void search_indicesAggregation_localAndRemoteIndexPattern() throws Exception {
        String query = "my_remote:a*,b*/_search?" + ccsMinimizeRoundtrips;
        String body = "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":1000}}}}";

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("aggregations.indices.buckets", matchesDocCount(ImmutableMap.of("b1", index_coord_b1)
                    .with("b2", index_coord_b2).with("my_remote:a1", index_remote_a1).with("my_remote:a2", index_remote_a2)))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(distinctNodesAt("aggregations.indices.buckets",
                    matchesDocCount(ImmutableMap.of("my_remote:a1", index_remote_a1).with("my_remote:a2", index_remote_a2)))));
        }

    }

    @Test
    public void search_termsAggregation_localAndRemoteWildcard() throws Exception {
        String query = "my_remote:*,*/_search?" + ccsMinimizeRoundtrips;
        String body = "{\"size\":0,\"aggs\":{\"clusteragg\":{\"terms\":{\"field\":\"cluster.keyword\",\"size\":1000}}}}";

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.successful", is(2))));

            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("aggregations.clusteragg.buckets[?(@.key == 'local')].doc_count", containsInAnyOrder(300))));
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("aggregations.clusteragg.buckets[?(@.key == 'remote')].doc_count", containsInAnyOrder(342))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse, json(nodeAt("_clusters.successful", is(2))));

            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("aggregations.clusteragg.buckets[?(@.key == 'local')].doc_count", containsInAnyOrder(198))));
            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("aggregations.clusteragg.buckets[?(@.key == 'remote')].doc_count", containsInAnyOrder(236))));
        }

    }

    @Test
    public void search_termsAggregation_localNotFoundAndRemoteWildcard() throws Exception {
        String query = "my_remote:*,notfound/_search?" + ccsMinimizeRoundtrips;
        String body = "{\"size\":0,\"aggs\":{\"clusteragg\":{\"terms\":{\"field\":\"cluster.keyword\",\"size\":1000}}}}";

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isNotFound());
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            // This is slightly counter-intuitive, but correct:
            // The user does not have privileges for an index called notfound
            // Thus, as the index expression contains wildcards, it is removed
            // from the index expression. Thus, the search is successful.

            Assert.assertThat(httpResponse, isOk());
        }

    }

    @Test
    public void search_termsAggregation_localNotFoundAndRemoteWildcard_ignoreUnavailable() throws Exception {
        boolean roundtripsMinimized = ccsMinimizeRoundtrips.endsWith(String.valueOf(Boolean.TRUE));
        String query = "my_remote:*,notfound/_search?ignore_unavailable=true&" + ccsMinimizeRoundtrips;
        String body = "{\"size\":0,\"aggs\":{\"clusteragg\":{\"terms\":{\"field\":\"cluster.keyword\",\"size\":1000}}}}";

        Matcher<HttpResponse> clustersCountMatcherCssRoundtripsMinTrue = allOf(json(nodeAt("_clusters.successful", is(2))),
                json(nodeAt("_clusters.running", is(0))));

        // ES 8.16 introduce correction related to the way in which number or _clusters.running and _clusters.running is computed for local indices
        // For local indices state _clusters.running is not used, instead _clusters.successful is incremented,
        // please see: https://github.com/elastic/elasticsearch/commit/dec43e3f941d367733a0e9e0ec9cb8abf4b2931b
        Matcher<HttpResponse> clustersCountMatcherCssRoundtripsMinFalse = allOf(json(nodeAt("_clusters.successful", is(2))),
                json(nodeAt("_clusters.running", is(0))));

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());
            Assert.assertThat(httpResponse,
                    roundtripsMinimized ? clustersCountMatcherCssRoundtripsMinTrue : clustersCountMatcherCssRoundtripsMinFalse);

            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("aggregations.clusteragg.buckets[?(@.key == 'remote')].doc_count", containsInAnyOrder(342))));
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.postJson(query, body);

            Assert.assertThat(httpResponse, isOk());

            Assert.assertThat(httpResponse,
                    json(distinctNodesAt("aggregations.clusteragg.buckets[?(@.key == 'remote')].doc_count", containsInAnyOrder(236))));
        }

    }

    @Test
    public void resolve_cluster_local_static_match_ignore_unavailable_true_limited_user() throws Exception {
        String query = "/_resolve/cluster/b1?ignore_unavailable=true"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$['(local)'].matching_indices", equalTo(false))));
        }
    }

    @Test
    public void resolve_cluster_local_static_match_ignore_unavailable_false_limited_user() throws Exception {
        String query = "/_resolve/cluster/b1?ignore_unavailable=false"; //DNFOF disabled

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isForbidden());
            assertThat(httpResponse, json(distinctNodesAt("$.error.root_cause[0].type", equalTo("security_exception"))));
        }
    }

    @Test
    public void resolve_cluster_local_pattern_not_match_limited_user_limited_user() throws Exception {

        String query = "/_resolve/cluster/no_such_local_index*"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$['(local)'].matching_indices", equalTo(false))));
        }
    }

    @Test
    public void resolve_cluster_local_pattern_not_match() throws Exception {

        String query = "/_resolve/cluster/no_such_local_index*"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$['(local)'].matching_indices", equalTo(false))));
        }
    }

    @Test
    public void resolve_cluster_local_pattern_star_match() throws Exception {

        String query = "/_resolve/cluster/*"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$['(local)'].matching_indices", equalTo(true))));
        }
    }

    @Test
    public void resolve_cluster_remote_static_match() throws Exception {

        String query = "/_resolve/cluster/my_remote:b1"; //DNFOF disabled

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.matching_indices", equalTo(true))));
        }
    }

    @Test
    public void resolve_cluster_remote_static_match_ignore_unavailable_true_limited_user() throws Exception {

        String query = "/_resolve/cluster/my_remote:b1?ignore_unavailable=true"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.matching_indices", equalTo(false))));
        }
    }

    @Test
    public void resolve_cluster_remote_static_match_ignore_unavailable_false_limited_user() throws Exception {

        String query = "/_resolve/cluster/my_remote:b1"; //DNFOF disabled

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.error", equalTo("Insufficient permissions"))));
        }
    }

    @Test
    public void resolve_cluster_remote_pattern_not_match() throws Exception {

        String query = "/_resolve/cluster/my_remote:no_such_remote_index*"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.matching_indices", equalTo(false))));
        }
    }

    @Test
    public void resolve_cluster_remote_all_static_match() throws Exception {

        String query = "/_resolve/cluster/*:b1"; // DNFOF disabled

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.matching_indices", equalTo(true))));
        }
    }

    @Test
    public void resolve_cluster_remote_all_static_match_limited_user() throws Exception {

        String query = "/_resolve/cluster/*:b1";// DNFOF disabled?

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.error", equalTo("Insufficient permissions"))));
        }
    }

    @Test
    public void resolve_cluster_remote_and_local_static_match() throws Exception {
        String query = "/_resolve/cluster/my_remote:b1,b1"; //DNFOF disabled

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$['(local)'].matching_indices", equalTo(true))));
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.matching_indices", equalTo(true))));
        }
    }

    @Test
    public void resolve_cluster_remote_and_local_static_match_limited_user() throws Exception {
        String query = "/_resolve/cluster/my_remote:b1,b1"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isForbidden());
            assertThat(httpResponse, json(distinctNodesAt("$.error.root_cause[0].type", equalTo("security_exception"))));
        }
    }

    @Test
    public void resolve_cluster_remote_and_local_pattern_match() throws Exception {
        String query = "/_resolve/cluster/*:*,*"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$['(local)'].matching_indices", equalTo(true))));
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.matching_indices", equalTo(true))));
        }
    }

    @Test
    public void resolve_cluster_remote_and_local_pattern_match_user_not_allowed_to_access_endpoint() throws Exception {
        String query = "/_resolve/cluster/*:*,*"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_B)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$['(local)'].matching_indices", equalTo(false))));
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.matching_indices", equalTo(false))));
        }
    }

    @Test
    public void resolve_cluster_remote_and_local_pattern_match_and_not_match() throws Exception {
        String query = "/_resolve/cluster/*:no_such_remote_index*,*"; // DNFOF enabled

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(distinctNodesAt("$['(local)'].matching_indices", equalTo(true))));
            assertThat(httpResponse, json(distinctNodesAt("$.my_remote.matching_indices", equalTo(false))));
        }
    }

}
