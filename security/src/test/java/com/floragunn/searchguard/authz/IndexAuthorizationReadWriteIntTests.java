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

package com.floragunn.searchguard.authz;

import static com.floragunn.searchguard.test.IndexApiMatchers.containsExactly;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedTo;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedToNone;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimited;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.junit.After;
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
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

import net.jcip.annotations.NotThreadSafe;

@RunWith(Parameterized.class)
@NotThreadSafe
public class IndexAuthorizationReadWriteIntTests {
    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    static TestIndex index_ar1 = TestIndex.name("index_ar1").documentCount(10).build();
    static TestIndex index_ar2 = TestIndex.name("index_ar2").documentCount(10).build();
    static TestIndex index_aw1 = TestIndex.name("index_aw1").documentCount(10).build();
    static TestIndex index_aw2 = TestIndex.name("index_aw2").documentCount(10).build();
    static TestIndex index_br1 = TestIndex.name("index_br1").documentCount(10).build();
    static TestIndex index_br2 = TestIndex.name("index_br2").documentCount(10).build();
    static TestIndex index_bw1 = TestIndex.name("index_bw1").documentCount(10).build();
    static TestIndex index_bw2 = TestIndex.name("index_bw2").documentCount(10).build();
    static TestIndex index_cr1 = TestIndex.name("index_cr1").documentCount(10).build();
    static TestIndex index_cw1 = TestIndex.name("index_cw1").documentCount(10).build();

    static TestAlias alias_ab1r = new TestAlias("alias_ab1r", index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_bw1);
    static TestAlias alias_ab1w = new TestAlias("alias_ab1w", index_aw1, index_aw2, index_bw1).writeIndex(index_aw1);

    static TestAlias alias_c1 = new TestAlias("alias_c1", index_cr1);

    static TestIndex index_bwx = TestIndex.name("index_bwx").documentCount(0).build(); // index_bwx is not initially created

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("index_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_a*")//
                            .indexPermissions("SGS_WRITE").on("index_aw*"))//
            .indexMatcher("read", limitedTo(index_ar1, index_ar2, index_aw1, index_aw2))//
            .indexMatcher("write", limitedTo(index_aw1, index_aw2))//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B = new TestSgConfig.User("limited_user_B")//
            .description("index_b*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_bw*"))//
            .indexMatcher("read", limitedTo(index_br1, index_br2, index_bw1, index_bw2, index_bwx))//
            .indexMatcher("write", limitedTo(index_bw1, index_bw2, index_bwx))//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B_CREATE_INDEX = new TestSgConfig.User("limited_user_B_create_index")//
            .description("index_b* with create index privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_bw*")//
                            .indexPermissions("SGS_CREATE_INDEX").on("index_bw*"))//
            .indexMatcher("read", limitedTo(index_br1, index_br2, index_bw1, index_bw2, index_bwx))//
            .indexMatcher("write", limitedTo(index_bw1, index_bw2, index_bwx))//
            .indexMatcher("create_index", limitedTo(index_bw1, index_bw2, index_bwx))//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B_MANAGE_INDEX = new TestSgConfig.User("limited_user_B_manage_index")//
            .description("index_b* with manage index privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_bw*")//
                            .indexPermissions("SGS_MANAGE").on("index_bw*"))//
            .indexMatcher("read", limitedTo(index_br1, index_br2, index_bw1, index_bw2, index_bwx))//
            .indexMatcher("write", limitedTo(index_bw1, index_bw2, index_bwx))//
            .indexMatcher("create_index", limitedTo(index_bw1, index_bw2, index_bwx))//
            .indexMatcher("manage_index", limitedTo(index_bw1, index_bw2, index_bwx))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_AB_MANAGE_INDEX = new TestSgConfig.User("limited_user_AB_manage_index")//
            .description("index_a*, index_b* with manage index privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_a*", "index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_aw*", "index_bw*")//
                            .indexPermissions("SGS_MANAGE").on("index_aw*", "index_bw*"))//
            .indexMatcher("read", limitedTo(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_br2, index_bw1, index_bw2, index_bwx))//
            .indexMatcher("write", limitedTo(index_aw1, index_aw2, index_bw1, index_bw2, index_bwx))//
            .indexMatcher("create_index", limitedTo(index_aw1, index_aw2, index_bw1, index_bw2, index_bwx))//
            .indexMatcher("manage_index", limitedTo(index_aw1, index_aw2, index_bw1, index_bw2, index_bwx))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_C = new TestSgConfig.User("limited_user_C")//
            .description("index_c*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("index_c*")//
                            .indexPermissions("SGS_WRITE").on("index_cw*"))//
            .indexMatcher("read", limitedTo(index_cr1, index_cw1))//
            .indexMatcher("write", limitedTo(index_cw1))//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_ALIAS_AB1 = new TestSgConfig.User("limited_user_alias_AB1")//
            .description("alias_ab1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/aliases/get").on("alias_ab1r")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/aliases/get", "SGS_WRITE").on("alias_ab1w"))//
            .indexMatcher("read", limitedTo(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_bw1, alias_ab1r))//
            .indexMatcher("write", limitedTo(index_aw1, index_aw2, index_bw1, index_bw2, alias_ab1w))//
            .indexMatcher("create_index", limitedTo(index_aw1, index_aw2, index_bw1, index_bw2))//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("get_alias", limitedTo(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_bw1, alias_ab1r, alias_ab1w));

    /* TODO
    static TestSgConfig.User LIMITED_USER_ALIAS_C1 = new TestSgConfig.User("limited_user_alias_C1")//
            .description("alias_c1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR").on("alias_c1"))//
            .indexMatcher("read", limitedTo(index_cr1, alias_c1))//
            .indexMatcher("get_alias", limitedToNone());
            */

    static TestSgConfig.User LIMITED_USER_NONE = new TestSgConfig.User("limited_user_none")//
            .description("no privileges for existing indices")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_CRUD", "SGS_INDICES_MONITOR").on("index_does_not_exist_*"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("write", limitedToNone())//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS = new TestSgConfig.User("invalid_user_index_permissions_for_alias")//
            .description("invalid: index permissions for alias")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_CRUD", "SGS_INDICES_MONITOR", "SGS_CREATE_INDEX").on("alias_ab1"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("write", limitedToNone())//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user")//
            .description("unlimited")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("*").on("*")//
                            .aliasPermissions("*").on("*")

            )//
            .indexMatcher("read", unlimited())//
            .indexMatcher("write", unlimited())//
            .indexMatcher("create_index", unlimited())//
            .indexMatcher("manage_index", unlimited())//
            .indexMatcher("get_alias", unlimited());

    /*
    static TestSgConfig.User LIMITED_USER_D = new TestSgConfig.User("limited_user_D").roles(//
            new Role("limited_user_d_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")
                    .indexPermissions("SGS_CRUD", "indices:admin/refresh", "indices:data/write/delete/byquery").on("d*"));
    
    static TestSgConfig.User LIMITED_USER_A_B1 = new TestSgConfig.User("limited_user_A_B1").roles(//
            new Role("limited_user_a_b1_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("a*")
                    .indexPermissions("SGS_CRUD").on("b1"));
    */

    /*
    static TestSgConfig.User LIMITED_USER_A_WITHOUT_ANALYZE = new TestSgConfig.User("limited_user_A_without_analyze").roles(//
            new Role("limited_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("indices:data/read*").on("a*"));
    */
    static List<TestSgConfig.User> USERS = ImmutableList.of(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_B_CREATE_INDEX, LIMITED_USER_B_MANAGE_INDEX,
            LIMITED_USER_AB_MANAGE_INDEX, LIMITED_USER_C, LIMITED_USER_ALIAS_AB1, LIMITED_USER_NONE, INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS,
            UNLIMITED_USER);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(USERS)//
            .indices(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_br2, index_bw1, index_bw2, index_cr1, index_cw1)//
            .aliases(alias_ab1r, alias_ab1w, alias_c1)//
            .authzDebug(true)//
            .build();

    final TestSgConfig.User user;

    @Test
    public void putDocument() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/index_bw1/_doc/put_test_1", DocNode.of("a", 1));
            assertThat(httpResponse, containsExactly(index_bw1).at("_index").but(user.indexMatcher("write")).whenEmpty(403));
        }
    }

    @Test
    public void putDocument_delete() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.putJson("/index_bw1/_doc/put_delete_test_1?refresh=true", DocNode.of("a", 1));
            assertThat(httpResponse, containsExactly(index_bw1).at("_index").but(user.indexMatcher("write")).whenEmpty(403));
            httpResponse = restClient.delete("/index_bw1/_doc/put_delete_test_1");
            assertThat(httpResponse, containsExactly(index_bw1).at("_index").but(user.indexMatcher("write")).whenEmpty(403));
        }
    }

    @Test
    public void putDocument_bulk() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/index_aw1/_doc/d1", "/index_bw1/_doc/d1", "/index_cw1/_doc/d1");

            HttpResponse httpResponse = restClient.putNdJson("/_bulk?refresh=true", //
                    DocNode.of("index._index", "index_aw1", "index._id", "d1"), DocNode.of("a", 1), //
                    DocNode.of("index._index", "index_bw1", "index._id", "d1"), DocNode.of("b", 1), //
                    DocNode.of("index._index", "index_cw1", "index._id", "d1"), DocNode.of("c", 1)//
            );
            assertThat(httpResponse, containsExactly(index_aw1, index_bw1, index_cw1).at("items[*].index[?(@.result == 'created')]._index")
                    .but(user.indexMatcher("write")).whenEmpty(200));

            // TODO test for absense of docs
        }
    }

    @Test
    public void putDocument_alias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/alias_ab1w/_doc/put_doc_alias_test_1", DocNode.of("a", 1));

            if (containsExactly(alias_ab1w).but(user.indexMatcher("write")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, containsExactly(index_aw1).at("_index").but(user.indexMatcher("write")).whenEmpty(403));
            }
        }
    }

    @Test
    public void putDocument_noExistingIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/index_bwx");

            HttpResponse httpResponse = restClient.putJson("/index_bwx/_doc/put_doc_non_existing_index_test_1", DocNode.of("a", 1));
            assertThat(httpResponse, containsExactly(index_bwx).at("_index").but(user.indexMatcher("create_index")).whenEmpty(403));
        }
    }
    
    @Test
    public void createIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/index_bwx", DocNode.EMPTY);
            assertThat(httpResponse, containsExactly(index_bwx).at("index").but(user.indexMatcher("create_index")).whenEmpty(403));
        }
    }

    @Test
    public void createIndex_deleteIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/index_bwx", DocNode.EMPTY);
            assertThat(httpResponse, containsExactly(index_bwx).at("index").but(user.indexMatcher("create_index")).whenEmpty(403));
            httpResponse = restClient.delete("/index_bwx");

            if (user.indexMatcher("manage_index").isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
            }
        }
    }

    @Test
    public void reindex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/index_bwx");

            HttpResponse httpResponse = restClient.postJson("/_reindex", DocNode.of("source.index", "index_br1", "dest.index", "index_bwx"));
            if (containsExactly(index_bwx).but(user.indexMatcher("create_index")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
            }

            // TODO test for absense of docs

        }
    }

    @Test
    public void reindex2() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/index_bwx");

            HttpResponse httpResponse = restClient.postJson("/_reindex", DocNode.of("source.index", "index_ar1", "dest.index", "index_bwx"));

            if (user == UNLIMITED_USER || user == LIMITED_USER_AB_MANAGE_INDEX) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }

            // TODO test for absense of docs

        }
    }

    @Test
    public void closeIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.post("/index_bw1/_close");
            assertThat(httpResponse, containsExactly(index_bw1).at("indices.keys()").but(user.indexMatcher("manage_index")).whenEmpty(403));
        } finally {
            try (Client client = cluster.getInternalNodeClient()) {
                client.admin().indices().open(new OpenIndexRequest("index_bw1")).actionGet();
            }
        }
    }

    @Test
    public void closeIndex_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.post("/*/_close");
            assertThat(httpResponse,
                    containsExactly(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_br2, index_bw1, index_bw2, index_cr1, index_cw1)
                            .at("indices.keys()").but(user.indexMatcher("manage_index")).whenEmpty(403));
        } finally {
            try (Client client = cluster.getInternalNodeClient()) {
                client.admin().indices().open(new OpenIndexRequest("*")).actionGet();
            }
        }
    }

    @Test
    public void closeIndex_openIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.post("/index_bw1/_close");
            assertThat(httpResponse, containsExactly(index_bw1).at("indices.keys()").but(user.indexMatcher("manage_index")).whenEmpty(403));
            httpResponse = restClient.post("/index_bw1/_open");

            if (containsExactly(index_bw1).but(user.indexMatcher("manage_index")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
            }
        } finally {
            try (Client client = cluster.getInternalNodeClient()) {
                client.admin().indices().open(new OpenIndexRequest("index_bw1")).actionGet();
            }
        }
    }

    @Test
    public void closeIndex_openIndex_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.post("/*/_close");
            assertThat(httpResponse,
                    containsExactly(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_br2, index_bw1, index_bw2, index_cr1, index_cw1)
                            .at("indices.keys()").but(user.indexMatcher("manage_index")).whenEmpty(403));
            httpResponse = restClient.post("/*/_open");
            if (containsExactly(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_br2, index_bw1, index_bw2, index_cr1, index_cw1)
                    .but(user.indexMatcher("manage_index")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
            }
        } finally {
            try (Client client = cluster.getInternalNodeClient()) {
                client.admin().indices().open(new OpenIndexRequest("*")).actionGet();
            }
        }
    }

    @After
    public void refresh() {
        try (Client client = cluster.getInternalNodeClient()) {
            client.admin().indices().refresh(new RefreshRequest("*")).actionGet();
        }
    }

    @Parameters(name = "{1}")
    public static Collection<Object[]> params() {
        List<Object[]> result = new ArrayList<>();

        for (TestSgConfig.User user : USERS) {
            result.add(new Object[] { user, user.getDescription() });
        }

        return result;
    }

    public IndexAuthorizationReadWriteIntTests(TestSgConfig.User user, String description) throws Exception {
        this.user = user;
    }

}
