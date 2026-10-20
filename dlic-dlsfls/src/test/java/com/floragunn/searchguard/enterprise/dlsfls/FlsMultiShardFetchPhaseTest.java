package com.floragunn.searchguard.enterprise.dlsfls;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

import java.util.List;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.DlsFls;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Searches on indices with several shards are executed in two phases: query and fetch. The fetch phase is a separate request
 * which is - since ES 9.5 - routed via a local coordination action which only forwards the thread context headers captured by ES.
 * This test makes sure that the user is available in the fetch phase and that FLS and field masking are applied there.
 */
public class FlsMultiShardFetchPhaseTest {
    static final String INDEX = "documents_multi_shard";
    static final int DOC_COUNT = 10;

    static final Authc AUTHC = new Authc(new Authc.Domain("basic/internal_users_db"));
    static final DlsFls DLSFLS = new DlsFls().metrics("detailed");

    static final Role READER_EXCEPT_AUTHOR_ROLE = new Role("reader_except_author_role").clusterPermissions("*")
            .indexPermissions("indices:data/read/search").fls("~author").on(INDEX);
    static final Role READER_MASKED_AUTHOR_ROLE = new Role("reader_masked_author_role").clusterPermissions("*")
            .indexPermissions("indices:data/read/search").maskedFields("author").on(INDEX);
    static final Role READER_DLS_ROLE = new Role("reader_dls_role").clusterPermissions("*")
            .indexPermissions("indices:data/read/search").dls(DocNode.of("match.author", "Goethe")).on(INDEX);

    static final User READER_EXCEPT_AUTHOR_USER = new User("reader_except_author_user").roles(READER_EXCEPT_AUTHOR_ROLE.getName());
    static final User READER_MASKED_AUTHOR_USER = new User("reader_masked_author_user").roles(READER_MASKED_AUTHOR_ROLE.getName());
    static final User READER_DLS_USER = new User("reader_dls_user").roles(READER_DLS_ROLE.getName());

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .roles(READER_EXCEPT_AUTHOR_ROLE, READER_MASKED_AUTHOR_ROLE, READER_DLS_ROLE)
            .users(READER_EXCEPT_AUTHOR_USER, READER_MASKED_AUTHOR_USER, READER_DLS_USER).embedded().build();

    @BeforeClass
    public static void populateData() {
        Client client = cluster.getPrivilegedInternalNodeClient();
        client.admin().indices().create(new CreateIndexRequest(INDEX)
                .settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0))).actionGet();

        for (int i = 0; i < DOC_COUNT; i++) {
            String author = i % 2 == 0 ? "Goethe" : "Schiller";
            client.index(new IndexRequest(INDEX).id(String.valueOf(i)).setRefreshPolicy(IMMEDIATE)
                    .source(DocNode.of("title", "title " + i, "content", "content " + i, "author", author).toJsonString(), XContentType.JSON)).actionGet();
        }
    }

    @Test
    public void flsIsAppliedInFetchPhase() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(READER_EXCEPT_AUTHOR_USER)) {
            HttpResponse response = client.get("/" + INDEX + "/_search?size=" + DOC_COUNT);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            List<Map<String, Object>> hits = hits(response);
            assertThat(response.getBody(), hits.size(), equalTo(DOC_COUNT));
            for (Map<String, Object> hit : hits) {
                Map<?, ?> source = (Map<?, ?>) hit.get("_source");
                assertThat(response.getBody(), source, hasKey("title"));
                assertThat(response.getBody(), source, not(hasKey("author")));
            }
        }
    }

    @Test
    public void fieldMaskingIsAppliedInFetchPhase() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(READER_MASKED_AUTHOR_USER)) {
            HttpResponse response = client.get("/" + INDEX + "/_search?size=" + DOC_COUNT);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            List<Map<String, Object>> hits = hits(response);
            assertThat(response.getBody(), hits.size(), equalTo(DOC_COUNT));
            for (Map<String, Object> hit : hits) {
                Map<?, ?> source = (Map<?, ?>) hit.get("_source");
                assertThat(response.getBody(), source, hasKey("author"));
                assertThat(response.getBody(), source.get("author"), not(equalTo("Goethe")));
                assertThat(response.getBody(), source.get("author"), not(equalTo("Schiller")));
            }
        }
    }

    @Test
    public void dlsIsAppliedInFetchPhase() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(READER_DLS_USER)) {
            HttpResponse response = client.get("/" + INDEX + "/_search?size=" + DOC_COUNT);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            List<Map<String, Object>> hits = hits(response);
            assertThat(response.getBody(), hits.size(), equalTo(DOC_COUNT / 2));
            for (Map<String, Object> hit : hits) {
                Map<?, ?> source = (Map<?, ?>) hit.get("_source");
                assertThat(response.getBody(), source.get("author"), equalTo("Goethe"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hits(HttpResponse response) throws Exception {
        return response.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits").stream()
                .map(node -> (Map<String, Object>) node.toMap()).toList();
    }
}
