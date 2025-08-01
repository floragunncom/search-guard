package com.floragunn.searchguard.authz;

import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.matches;
import static org.hamcrest.Matchers.containsInAnyOrder;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Covers only cases where the results differ from those with the option to skip unavailable clusters enabled {@link IgnoreUnauthorizedCcsIntTest}
 */
@RunWith(Parameterized.class)
public class IgnoreUnauthorizedCcsWithDisabledSkipUnavailableRemoteClusterIntTest {

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

    @ClassRule
    public static LocalCluster anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
            .nodeSettings("searchguard.diagnosis.action_stack.enabled", true).users(LIMITED_USER_REMOTE_A, LIMITED_USER_REMOTE_B, UNLIMITED_USER)//
            .indices(index_remote_a1, index_remote_a2, index_remote_b1, index_remote_b2, index_remote_r1)//
            .aliases(xalias_remote_ab1)//
            .build();

    @Parameter
    public static String ccsMinimizeRoundtrips;

    @Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] { "ccs_minimize_roundtrips=false", "ccs_minimize_roundtrips=true" };
    }

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
            .remote("my_remote", anotherCluster).nodeSettings("searchguard.diagnosis.action_stack.enabled", true)
            .nodeSettings("cluster.remote.my_remote.skip_unavailable", false).users(LIMITED_USER_COORD_A, LIMITED_USER_COORD_B, UNLIMITED_USER)//
            .indices(index_coord_a1, index_coord_a2, index_coord_b1, index_coord_b2, index_coord_c1)//
            .aliases(xalias_coord_ab1)//
            .embedded().build();

    @Test
    public void search_clusterWildcard() throws Exception {
        String query = "*:/_search?size=1000&" + ccsMinimizeRoundtrips;

        try (GenericRestClient restClient = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isNotFound());
        }

        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_COORD_A)) {
            HttpResponse httpResponse = restClient.get(query);

            Assert.assertThat(httpResponse, isForbidden());
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

            Assert.assertThat(httpResponse, isForbidden());
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

            Assert.assertThat(httpResponse, isForbidden());
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
            Assert.assertThat(httpResponse, json(distinctNodesAt("responses[*].error.type", containsInAnyOrder("security_exception"))));
        }
    }
}
