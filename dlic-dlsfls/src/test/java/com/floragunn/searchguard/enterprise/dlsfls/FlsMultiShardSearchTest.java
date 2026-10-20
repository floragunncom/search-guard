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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

import java.util.List;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
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
 * Verifies that FLS and field masking are applied to the hits of a search request which needs a separate fetch phase, i.e.,
 * a search on an index with several shards distributed over several nodes. Since ES 9.5, the fetch phase is routed through
 * TransportFetchPhaseCoordinationAction, which only propagates the request headers (but not the transient values) of the
 * thread context to the data nodes. See SearchGuardFilter.ensureIdentityHeaders().
 * <p>
 * Single shard indices do not exercise this code path, as ES then executes query and fetch in one step.
 */
public class FlsMultiShardSearchTest {
    static final String INDEX = "documents_multi_shard";
    static final String FIELD_TITLE = "title";
    static final String FIELD_CONTENT = "content";
    static final String FIELD_AUTHOR = "author";
    static final int DOC_COUNT = 50;

    static final Authc AUTHC = new Authc(new Authc.Domain("basic/internal_users_db"));
    static final DlsFls DLSFLS = new DlsFls().metrics("detailed");

    static final Role UNRESTRICTED_ROLE = new Role("unrestricted_role").clusterPermissions("*").indexPermissions("SGS_READ").on(INDEX);
    static final Role FLS_EXCLUDE_AUTHOR_ROLE = new Role("fls_exclude_author_role").clusterPermissions("*").indexPermissions("SGS_READ")
            .fls("~" + FIELD_AUTHOR).on(INDEX);
    static final Role FLS_TITLE_ONLY_ROLE = new Role("fls_title_only_role").clusterPermissions("*").indexPermissions("SGS_READ")
            .fls(FIELD_TITLE).on(INDEX);
    static final Role MASKED_AUTHOR_ROLE = new Role("masked_author_role").clusterPermissions("*").indexPermissions("SGS_READ")
            .maskedFields(FIELD_AUTHOR).on(INDEX);

    static final User UNRESTRICTED_USER = new User("unrestricted_user").roles(UNRESTRICTED_ROLE.getName());
    static final User FLS_EXCLUDE_AUTHOR_USER = new User("fls_exclude_author_user").roles(FLS_EXCLUDE_AUTHOR_ROLE.getName());
    static final User FLS_TITLE_ONLY_USER = new User("fls_title_only_user").roles(FLS_TITLE_ONLY_ROLE.getName());
    static final User MASKED_AUTHOR_USER = new User("masked_author_user").roles(MASKED_AUTHOR_ROLE.getName());

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .roles(UNRESTRICTED_ROLE, FLS_EXCLUDE_AUTHOR_ROLE, FLS_TITLE_ONLY_ROLE, MASKED_AUTHOR_ROLE)
            .users(UNRESTRICTED_USER, FLS_EXCLUDE_AUTHOR_USER, FLS_TITLE_ONLY_USER, MASKED_AUTHOR_USER).embedded().build();

    @BeforeClass
    public static void populateData() {
        Client client = cluster.getPrivilegedInternalNodeClient();
        client.admin().indices()
                .create(new CreateIndexRequest(INDEX).settings(Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 0)))
                .actionGet();

        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < DOC_COUNT; i++) {
            bulkRequest.add(new IndexRequest(INDEX).id(String.valueOf(i)).source(
                    DocNode.of(FIELD_TITLE, "title " + i, FIELD_CONTENT, "content " + i, FIELD_AUTHOR, "author " + i).toJsonString(), XContentType.JSON));
        }

        client.bulk(bulkRequest).actionGet();
    }

    @Test
    public void unrestrictedUserSeesAllFields() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(UNRESTRICTED_USER)) {
            HttpResponse response = client.get("/" + INDEX + "/_search?size=" + DOC_COUNT);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            List<DocNode> hits = hits(response);
            assertThat(hits, hasSize(DOC_COUNT));

            for (DocNode hit : hits) {
                DocNode source = hit.getAsNode("_source");
                assertThat(response.getBody(), source.keySet(), equalTo(DocNode.of(FIELD_TITLE, 1, FIELD_CONTENT, 1, FIELD_AUTHOR, 1).keySet()));
            }
        }
    }

    @Test
    public void flsExcludeIsAppliedToAllHits() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(FLS_EXCLUDE_AUTHOR_USER)) {
            HttpResponse response = client.get("/" + INDEX + "/_search?size=" + DOC_COUNT);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            List<DocNode> hits = hits(response);
            assertThat(hits, hasSize(DOC_COUNT));

            for (DocNode hit : hits) {
                DocNode source = hit.getAsNode("_source");
                assertThat(response.getBody(), source.hasNonNull(FIELD_AUTHOR), equalTo(false));
                assertThat(response.getBody(), source.hasNonNull(FIELD_TITLE), equalTo(true));
                assertThat(response.getBody(), source.hasNonNull(FIELD_CONTENT), equalTo(true));
            }
        }
    }

    @Test
    public void flsIncludeIsAppliedToAllHits() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(FLS_TITLE_ONLY_USER)) {
            HttpResponse response = client.get("/" + INDEX + "/_search?size=" + DOC_COUNT);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            List<DocNode> hits = hits(response);
            assertThat(hits, hasSize(DOC_COUNT));

            for (DocNode hit : hits) {
                DocNode source = hit.getAsNode("_source");
                assertThat(response.getBody(), source.keySet(), equalTo(DocNode.of(FIELD_TITLE, 1).keySet()));
            }
        }
    }

    @Test
    public void fieldMaskingIsAppliedToAllHits() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(MASKED_AUTHOR_USER)) {
            HttpResponse response = client.get("/" + INDEX + "/_search?size=" + DOC_COUNT);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            List<DocNode> hits = hits(response);
            assertThat(hits, hasSize(DOC_COUNT));

            for (DocNode hit : hits) {
                DocNode source = hit.getAsNode("_source");
                String author = source.getAsString(FIELD_AUTHOR);
                assertThat(response.getBody(), author, not(equalTo("author " + hit.getAsString("_id"))));
                assertThat(response.getBody(), source.getAsString(FIELD_TITLE), equalTo("title " + hit.getAsString("_id")));
            }
        }
    }

    private static List<DocNode> hits(HttpResponse response) throws Exception {
        return response.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits");
    }
}
