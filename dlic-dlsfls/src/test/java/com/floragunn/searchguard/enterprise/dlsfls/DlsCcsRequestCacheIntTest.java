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

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * With ccs_minimize_roundtrips=false, the shards of the remote cluster get the shard search requests directly from
 * the coordinating cluster. On the remote cluster, DLS is then only applied on the shard level. A DLS query which
 * uses "now" must keep such a search out of the shard request cache.
 */
public class DlsCcsRequestCacheIntTest {

    static final String INDEX_TERM_DLS = "events_term_dls";
    static final String INDEX_NOW_DLS = "events_now_dls";
    static final String[] REMOTE_PERMISSIONS = { "SGS_READ", "indices:admin/search/search_shards", "indices:admin/shards/search_shards" };

    static final TestCertificates CERTIFICATES = TestCertificates.builder().ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard").addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard").build();

    // The user's roles are sent to the remote cluster by name; the role with the DLS rules is only defined there.
    static final TestSgConfig.User USER_COORD = new TestSgConfig.User("dls_user")
            .roles(new Role("dls_ccs").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_READ").on("events_*"));

    static final TestSgConfig.User USER_REMOTE = new TestSgConfig.User("dls_user").roles(new Role("dls_ccs")
            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO") //
            .indexPermissions(REMOTE_PERMISSIONS).dls(DocNode.of("term.dept.value", "dept_a")).on(INDEX_TERM_DLS) //
            .indexPermissions(REMOTE_PERMISSIONS).dls(DocNode.of("range.ts.lte", "now")).on(INDEX_NOW_DLS));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));

    @ClassRule
    public static LocalCluster.Embedded remoteCluster = new LocalCluster.Builder().singleNode().sslEnabled(CERTIFICATES).enterpriseModulesEnabled()
            .authc(AUTHC).users(USER_REMOTE).embedded().build();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled(CERTIFICATES).enterpriseModulesEnabled()
            .authc(AUTHC).remote("my_remote", remoteCluster).users(USER_COORD).embedded().build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = remoteCluster.getAdminCertRestClient()) {
            HttpResponse configResponse = client.putJson("/_searchguard/config/authz_dlsfls", DocNode.of("dls.allow_now", true));
            assertThat(configResponse, isOk());

            for (String index : new String[] { INDEX_TERM_DLS, INDEX_NOW_DLS }) {
                HttpResponse response = client.putJson("/" + index, DocNode.of("settings.index.number_of_shards", 1, "mappings.properties",
                        DocNode.of("ts", DocNode.of("type", "date"), "dept", DocNode.of("type", "keyword"))));
                assertThat(response, isOk());
                response = client.putJson("/" + index + "/_doc/1?refresh=true", DocNode.of("ts", "2020-01-01T00:00:00Z", "dept", "dept_a"));
                assertThat(response.getBody(), response.getStatusCode(), equalTo(201));
            }
        }
    }

    @Test
    public void search_dlsQueryWithoutNow_isCached() throws Exception {
        search(INDEX_TERM_DLS);
        assertThat(requestCacheMissCount(INDEX_TERM_DLS), greaterThan(0));
    }

    @Test
    public void search_dlsQueryWithNow_isNotCached() throws Exception {
        search(INDEX_NOW_DLS);
        assertThat(requestCacheMissCount(INDEX_NOW_DLS), equalTo(0));
    }

    private static void search(String index) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_COORD)) {
            HttpResponse response = client
                    .get("/my_remote:" + index + "/_search?size=0&request_cache=true&ccs_minimize_roundtrips=false&track_total_hits=true");
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode().getAsNode("hits").getAsNode("total").get("value"), equalTo(1));
        }
    }

    private static int requestCacheMissCount(String index) throws Exception {
        try (GenericRestClient client = remoteCluster.getAdminCertRestClient()) {
            HttpResponse response = client.get("/" + index + "/_stats/request_cache");
            assertThat(response, isOk());
            return ((Number) response.getBodyAsDocNode().getAsNode("_all").getAsNode("total").getAsNode("request_cache").get("miss_count"))
                    .intValue();
        }
    }
}
