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

package com.floragunn.searchguard.authz.int_tests;

import static com.floragunn.searchguard.test.IndexApiMatchers.containsExactly;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedTo;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedToNone;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimited;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimitedIncludingSearchGuardIndices;
import static com.floragunn.searchguard.test.RestMatchers.isBadRequest;
import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
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
import com.floragunn.searchguard.test.IndexApiMatchers.IndexMatcher;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

@RunWith(Parameterized.class)
@NotThreadSafe
public class IndexAuthorizationReadWriteIntTests {

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
    static TestIndex index_hidden = TestIndex.name("index_hidden").hidden().documentCount(1).seed(8).attr("prefix", "h").build();

    static TestAlias alias_ab1r = new TestAlias("alias_ab1r", index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_bw1);
    static TestAlias alias_ab1w = new TestAlias("alias_ab1w", index_aw1, index_aw2, index_bw1).writeIndex(index_aw1);
    static TestAlias alias_ab1w_nowriteindex = new TestAlias("alias_ab1w_nowriteindex", index_aw1, index_aw2, index_bw1);

    static TestAlias alias_c1 = new TestAlias("alias_c1", index_cr1, index_cw1);

    static TestIndex index_bwx1 = TestIndex.name("index_bwx1").documentCount(0).build(); // not initially created
    static TestIndex index_bwx2 = TestIndex.name("index_bwx2").documentCount(0).build(); // not initially created

    static TestAlias alias_bwx = new TestAlias("alias_bwx"); // not initially created

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("index_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("index_a*")//
                            .indexPermissions("SGS_WRITE").on("index_aw*"))//
            .indexMatcher("read", limitedTo(index_ar1, index_ar2, index_aw1, index_aw2))//
            .indexMatcher("write", limitedTo(index_aw1, index_aw2))//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B = new TestSgConfig.User("limited_user_B")//
            .description("index_b*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_bw*"))//
            .indexMatcher("read", limitedTo(index_br1, index_br2, index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("write", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B_CREATE_INDEX = new TestSgConfig.User("limited_user_B_create_index")//
            .description("index_b* with create index privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_bw*")//
                            .indexPermissions("SGS_CREATE_INDEX").on("index_bw*"))//
            .indexMatcher("read", limitedTo(index_br1, index_br2, index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("write", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("create_index", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B_MANAGE_INDEX = new TestSgConfig.User("limited_user_B_manage_index")//
            .description("index_b* with manage privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_bw*")//
                            .indexPermissions("SGS_MANAGE").on("index_bw*"))//
            .indexMatcher("read", limitedTo(index_br1, index_br2, index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("write", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("create_index", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("manage_index", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("manage_alias", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("get_alias", limitedTo());

    static TestSgConfig.User LIMITED_USER_B_MANAGE_INDEX_ALIAS = new TestSgConfig.User("limited_user_B_manage_index_alias")//
            .description("index_b*, alias_bwx* with manage privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_bw*")//
                            .indexPermissions("SGS_MANAGE").on("index_bw*")//
                            .aliasPermissions("SGS_MANAGE_ALIASES").on("alias_bwx*"))//
            .indexMatcher("read", limitedTo(index_br1, index_br2, index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("write", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("create_index", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("manage_index", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2, alias_bwx))//
            .indexMatcher("manage_alias", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2, alias_bwx))//
            .indexMatcher("get_alias", limitedTo(alias_bwx));

    static TestSgConfig.User LIMITED_USER_B_CREATE_INDEX_MANAGE_ALIAS = new TestSgConfig.User("limited_user_B_create_index")//
            .description("index_b* with create index privs and manage alias privs, alias_bwx* with manage alias privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_bw*")//
                            .indexPermissions("SGS_CREATE_INDEX", "SGS_MANAGE_ALIASES").on("index_bw*")//
                            .aliasPermissions("SGS_MANAGE_ALIASES").on("alias_bwx*"))//
            .indexMatcher("read", limitedTo(index_br1, index_br2, index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("write", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("create_index", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2, alias_bwx))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B_HIDDEN_MANAGE_INDEX_ALIAS = new TestSgConfig.User("limited_user_B_HIDDEN_anage_index_alias")//
            .description("index_b*, index_hidden*, alias_bwx* with manage privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("index_b*", "index_hidden*")//
                            .indexPermissions("SGS_WRITE").on("index_bw*", "index_hidden*")//
                            .indexPermissions("SGS_MANAGE").on("index_bw*", "index_hidden*")//
                            .aliasPermissions("SGS_MANAGE_ALIASES").on("alias_bwx*"))//
            .indexMatcher("read", limitedTo(index_br1, index_br2, index_bw1, index_bw2, index_bwx1, index_bwx2, index_hidden))//
            .indexMatcher("write", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2, index_hidden))//
            .indexMatcher("create_index", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2, index_hidden))//
            .indexMatcher("manage_index", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2, alias_bwx, index_hidden))//
            .indexMatcher("manage_alias", limitedTo(index_bw1, index_bw2, index_bwx1, index_bwx2, alias_bwx, index_hidden))//
            .indexMatcher("get_alias", limitedTo(alias_bwx));

    static TestSgConfig.User LIMITED_USER_AB_MANAGE_INDEX = new TestSgConfig.User("limited_user_AB_manage_index")//
            .description("index_a*, index_b* with manage index privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("index_a*", "index_b*")//
                            .indexPermissions("SGS_WRITE").on("index_aw*", "index_bw*")//
                            .indexPermissions("SGS_MANAGE").on("index_aw*", "index_bw*"))//
            .indexMatcher("read",
                    limitedTo(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_br2, index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("write", limitedTo(index_aw1, index_aw2, index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("create_index", limitedTo(index_aw1, index_aw2, index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("manage_index", limitedTo(index_aw1, index_aw2, index_bw1, index_bw2, index_bwx1, index_bwx2))//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_C = new TestSgConfig.User("limited_user_C")//
            .description("index_c*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh").on("index_c*")//
                            .indexPermissions("SGS_WRITE").on("index_cw*"))//
            .indexMatcher("read", limitedTo(index_cr1, index_cw1))//
            .indexMatcher("write", limitedTo(index_cw1))//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_AB1_ALIAS = new TestSgConfig.User("limited_user_alias_AB1")//
            .description("alias_ab1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/aliases/get").on("alias_ab1r")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/aliases/get", "SGS_WRITE", "indices:admin/refresh*")
                            .on("alias_ab1w*"))//
            .indexMatcher("read",
                    limitedTo(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_bw1, alias_ab1r, alias_ab1w, alias_ab1w_nowriteindex))//
            .indexMatcher("write", limitedTo(index_aw1, index_aw2, index_bw1, alias_ab1w, alias_ab1w_nowriteindex))//
            .indexMatcher("create_index", limitedTo(index_aw1, index_aw2, index_bw1))//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedTo(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_bw1, alias_ab1r, alias_ab1w));

    static TestSgConfig.User LIMITED_USER_AB1_ALIAS_READ_ONLY = new TestSgConfig.User("limited_user_alias_AB1_read_only")//
            .description("read/only on alias_ab1w, but with write privs in write index index_aw1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_WRITE", "indices:admin/refresh").on("index_aw1")//
                            .aliasPermissions("SGS_READ").on("alias_ab1w"))//
            .indexMatcher("read", limitedTo(index_aw1, index_aw2, index_bw1, alias_ab1w))//
            .indexMatcher("write", limitedTo(index_aw1, alias_ab1w)) // alias_ab1w is included because index_aw1 is the write index of alias_ab1w
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_ALIAS_C1 = new TestSgConfig.User("limited_user_alias_C1")//
            .description("alias_c1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_WRITE", "SGS_INDICES_MONITOR").on("alias_c1"))//
            .indexMatcher("read", limitedTo(index_cr1, index_cw1, alias_c1))//
            .indexMatcher("write", limitedTo(index_cr1, index_cw1, alias_c1)) // 
            .indexMatcher("create_index", limitedTo(index_cw1))//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedTo(alias_c1));

    static TestSgConfig.User LIMITED_READ_ONLY_ALL = new TestSgConfig.User("limited_read_only_all")//
            .description("read/only on *")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").on("*"))//
            .indexMatcher("read", unlimited())//
            .indexMatcher("write", limitedToNone())//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_READ_ONLY_A = new TestSgConfig.User("limited_read_only_A")//
            .description("read/only on index_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ").on("index_a*"))//
            .indexMatcher("read", limitedTo(index_ar1, index_ar2, index_aw1, index_aw2))//
            .indexMatcher("write", limitedToNone())//
            .indexMatcher("create_index", limitedToNone())//
            .indexMatcher("manage_index", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

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
            .indexMatcher("manage_alias", limitedToNone())//
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
            .indexMatcher("manage_alias", limitedToNone())//
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
            .indexMatcher("manage_alias", unlimited())//
            .indexMatcher("get_alias", unlimited());

    /**
     * The SUPER_UNLIMITED_USER authenticates with an admin cert, which will cause all access control code to be skipped.
     * This serves as a base for comparison with the default behavior.
     */
    static TestSgConfig.User SUPER_UNLIMITED_USER = new TestSgConfig.User("super_unlimited_user")//
            .description("super unlimited (admin cert)")//
            .adminCertUser()//
            .indexMatcher("read", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("write", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("create_index", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("manage_index", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("manage_alias", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("get_alias", unlimitedIncludingSearchGuardIndices());

    static List<TestSgConfig.User> USERS = ImmutableList.of(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_B_CREATE_INDEX, LIMITED_USER_B_MANAGE_INDEX,
            LIMITED_USER_B_MANAGE_INDEX_ALIAS, LIMITED_USER_B_HIDDEN_MANAGE_INDEX_ALIAS, LIMITED_USER_AB_MANAGE_INDEX, LIMITED_USER_C,
            LIMITED_USER_AB1_ALIAS, LIMITED_USER_AB1_ALIAS_READ_ONLY, LIMITED_USER_ALIAS_C1, LIMITED_READ_ONLY_ALL, LIMITED_READ_ONLY_A,
            LIMITED_USER_NONE, INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS, UNLIMITED_USER, SUPER_UNLIMITED_USER);

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled() //
            .nodeSettings("action.destructive_requires_name", false).users(USERS)//
            .indices(index_ar1, index_ar2, index_aw1, index_aw2, index_br1, index_br2, index_bw1, index_bw2, index_cr1, index_cw1, index_hidden)//
            .aliases(alias_ab1r, alias_ab1w, alias_ab1w_nowriteindex, alias_c1)//
            .authzDebug(true)//
            .embedded().build();

    final TestSgConfig.User user;

    @Test
    public void putDocument() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/index_bw1/_doc/put_test_1", DocNode.of("a", 1));
            assertThat(httpResponse, containsExactly(index_bw1).at("_index").but(user.indexMatcher("write")).whenEmpty(403));
        }
    }

    @Test
    public void deleteDocument() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user);
                GenericRestClient adminRestClient = cluster.getAdminCertRestClient().trackResources()) {
            // Initialization
            {
                HttpResponse httpResponse = adminRestClient.putJson("/index_bw1/_doc/put_delete_test_1?refresh=true", DocNode.of("a", 1));
                assertThat(httpResponse, isCreated());
            }

            HttpResponse httpResponse = restClient.delete("/index_bw1/_doc/put_delete_test_1");
            assertThat(httpResponse, containsExactly(index_bw1).at("_index").but(user.indexMatcher("write")).whenEmpty(403));
        }
    }

    @Test
    public void deleteByQuery_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user);
                GenericRestClient adminRestClient = cluster.getAdminCertRestClient().trackResources()) {

            HttpResponse httpResponse = adminRestClient.putJson("/index_bw1/_doc/put_delete_delete_by_query_b1?refresh=true",
                    DocNode.of("delete_by_query_test", "yes"));
            assertThat(httpResponse, isCreated());
            httpResponse = adminRestClient.putJson("/index_bw1/_doc/put_delete_delete_by_query_b2?refresh=true",
                    DocNode.of("delete_by_query_test", "no"));
            assertThat(httpResponse, isCreated());
            httpResponse = adminRestClient.putJson("/index_aw1/_doc/put_delete_delete_by_query_a1?refresh=true",
                    DocNode.of("delete_by_query_test", "yes"));
            assertThat(httpResponse, isCreated());
            httpResponse = adminRestClient.putJson("/index_aw1/_doc/put_delete_delete_by_query_a2?refresh=true",
                    DocNode.of("delete_by_query_test", "no"));
            assertThat(httpResponse, isCreated());

            httpResponse = restClient.postJson("/index_aw*,index_bw*/_delete_by_query?refresh=true&wait_for_completion=true",
                    DocNode.of("query.term.delete_by_query_test", "yes"));

            if (containsExactly(index_aw1, index_aw2, index_bw1, index_bw2).at("_index").but(user.indexMatcher("write")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
                int expectedDeleteCount = containsExactly(index_aw1, index_bw1).at("_index").but(user.indexMatcher("write")).size();
                assertThat(httpResponse, json(nodeAt("deleted", equalTo(expectedDeleteCount))));
            }
        }
    }

    @Test
    public void putDocument_bulk() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/index_aw1/_doc/new_doc_aw1", "/index_bw1/_doc/new_doc_bw1", "/index_cw1/_doc/new_doc_cw1");
            IndexMatcher writePrivileges = user.indexMatcher("write");

            DocNode testDoc = DocNode.of("a", 1);

            HttpResponse httpResponse = restClient.putNdJson("/_bulk?refresh=true", //
                    DocNode.of("index._index", "index_aw1", "index._id", "new_doc_aw1"), testDoc, //
                    DocNode.of("index._index", "index_bw1", "index._id", "new_doc_bw1"), testDoc, //
                    DocNode.of("index._index", "index_cw1", "index._id", "new_doc_cw1"), testDoc//
            );

            assertThat(httpResponse, containsExactly(index_aw1, index_bw1, index_cw1).at("items[*].index[?(@.result == 'created')]._index")
                    .but(writePrivileges).whenEmpty(200));

            // Verify presence/absence of the potentially created docs
            assertThat(cluster.documents(index_aw1, index_bw1, index_cw1), containsExactly(//
                    writePrivileges.covers(index_aw1) ? index_aw1.withAdditionalDocument("new_doc_aw1", testDoc) : index_aw1,
                    writePrivileges.covers(index_bw1) ? index_bw1.withAdditionalDocument("new_doc_bw1", testDoc) : index_bw1,
                    writePrivileges.covers(index_cw1) ? index_cw1.withAdditionalDocument("new_doc_cw1", testDoc) : index_cw1));

        }
    }

    @Test
    public void putDocument_alias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/alias_ab1w/_doc/put_doc_alias_test_1", DocNode.of("a", 1));

            if (containsExactly(alias_ab1w, index_aw1).but(user.indexMatcher("write")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, containsExactly(index_aw1).at("_index").but(user.indexMatcher("write")).whenEmpty(403));
            }
        }
    }

    @Test
    public void putDocument_alias_noWriteIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/alias_ab1w_nowriteindex/_doc/put_doc_alias_test_1", DocNode.of("a", 1));

            if (containsExactly(alias_ab1w_nowriteindex).but(user.indexMatcher("write")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isBadRequest());
            }
        }
    }

    @Test
    public void putDocument_bulk_alias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/index_aw1/_doc/put_doc_alias_bulk_test_1");

            HttpResponse httpResponse = restClient.putNdJson("/_bulk?refresh=true", //
                    DocNode.of("index._index", "alias_ab1w", "index._id", "put_doc_alias_bulk_test_1"), DocNode.of("a", 1) //
            );

            assertThat(httpResponse,
                    containsExactly(index_aw1).at("items[*].index[?(@.result == 'created')]._index").but(user.indexMatcher("write")).whenEmpty(200));
        }
    }

    @Test
    public void putDocument_noExistingIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/index_bwx1");

            HttpResponse httpResponse = restClient.putJson("/index_bwx1/_doc/put_doc_non_existing_index_test_1", DocNode.of("a", 1));
            assertThat(httpResponse, containsExactly(index_bwx1).at("_index").but(user.indexMatcher("create_index")).whenEmpty(403));
        }
    }

    @Test
    public void createIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/index_bwx1", DocNode.EMPTY);
            assertThat(httpResponse, containsExactly(index_bwx1).at("index").but(user.indexMatcher("create_index")).whenEmpty(403));
        }
    }

    @Test
    public void createIndex_deleteIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/index_bwx1", DocNode.EMPTY);
            assertThat(httpResponse, containsExactly(index_bwx1).at("index").but(user.indexMatcher("create_index")).whenEmpty(403));
            httpResponse = restClient.delete("/index_bwx1");

            if (user.indexMatcher("manage_index").isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
            }
        }
    }

    @Test
    public void createIndex_withAlias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/index_bwx1", DocNode.of("aliases.alias_bwx", DocNode.EMPTY));

            if (containsExactly(alias_bwx).but(user.indexMatcher("manage_alias")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, containsExactly(index_bwx1).at("index").but(user.indexMatcher("create_index")).whenEmpty(403));
            }
        }
    }

    @Test
    public void deleteAlias_staticIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user);
                GenericRestClient adminRestClient = cluster.getAdminCertRestClient().trackResources()) {
            restClient.deleteWhenClosed("/*/_alias/alias_bwx");

            // Initialization
            {
                HttpResponse httpResponse = adminRestClient.postJson("/_aliases",
                        DocNode.of("actions", DocNode.array(DocNode.of("add.index", "index_bw1", "add.alias", "alias_bwx"))));
                assertThat(httpResponse, isOk());
            }

            HttpResponse httpResponse = restClient.delete("/index_bw1/_aliases/alias_bwx");
            if (containsExactly(index_bw1, alias_bwx).isCoveredBy(user.indexMatcher("manage_alias"))) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void deleteAlias_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user);
                GenericRestClient adminRestClient = cluster.getAdminCertRestClient().trackResources()) {
            adminRestClient.deleteWhenClosed("/*/_alias/alias_bwx");

            // Initialization
            {
                HttpResponse httpResponse = adminRestClient.postJson("/_aliases",
                        DocNode.of("actions", DocNode.array(DocNode.of("add.index", "index_bw1", "add.alias", "alias_bwx"))));
                assertThat(httpResponse, isOk());
            }

            HttpResponse httpResponse = restClient.delete("/*/_aliases/alias_bwx");
            if (containsExactly(index_bw1, alias_bwx).isCoveredBy(user.indexMatcher("manage_alias"))) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void aliases_createAlias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/*/_alias/alias_bwx");

            HttpResponse httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("add.index", "index_bw1", "add.alias", "alias_bwx"))));
            if (containsExactly(index_bw1, alias_bwx).isCoveredBy(user.indexMatcher("manage_alias"))) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void aliases_createAlias_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/*/_alias/alias_bwx");

            HttpResponse httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("add.indices", DocNode.array("index_bw*"), "add.alias", "alias_bwx"))));
            if (containsExactly(index_bw1, index_bw2, alias_bwx).isCoveredBy(user.indexMatcher("manage_alias"))) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void aliases_deleteAlias_staticIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user);
                GenericRestClient adminRestClient = cluster.getAdminCertRestClient().trackResources()) {
            adminRestClient.deleteWhenClosed("/*/_alias/alias_bwx");

            // Initialization
            {
                HttpResponse httpResponse = adminRestClient.postJson("/_aliases",
                        DocNode.of("actions", DocNode.array(DocNode.of("add.index", "index_bw1", "add.alias", "alias_bwx"))));
                assertThat(httpResponse, isOk());
            }

            HttpResponse httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("remove.index", "index_bw1", "remove.alias", "alias_bwx"))));
            if (containsExactly(index_bw1, alias_bwx).isCoveredBy(user.indexMatcher("manage_alias"))) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void aliases_deleteAlias_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user);
                GenericRestClient adminRestClient = cluster.getAdminCertRestClient().trackResources()) {
            adminRestClient.deleteWhenClosed("/*/_alias/alias_bwx");

            // Initialization
            {
                HttpResponse httpResponse = adminRestClient.postJson("/_aliases", DocNode.of("actions",
                        DocNode.array(DocNode.of("add.indices", DocNode.array("index_bw1", "index_bw2"), "add.alias", "alias_bwx"))));
                assertThat(httpResponse, isOk());
            }

            HttpResponse httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("remove.index", "*", "remove.alias", "alias_bwx"))));
            if (containsExactly(index_bw1, index_bw2, alias_bwx).isCoveredBy(user.indexMatcher("manage_alias"))) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void aliases_removeIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user);
                GenericRestClient adminRestClient = cluster.getAdminCertRestClient().trackResources()) {
            adminRestClient.deleteWhenClosed("/*/_alias/alias_bwx");

            // Initialization
            {
                HttpResponse httpResponse = adminRestClient.putJson("/index_bwx1", DocNode.of("aliases.alias_bwx", DocNode.EMPTY));
                assertThat(httpResponse, isOk());
                httpResponse = adminRestClient.putJson("/index_bwx2", DocNode.of("aliases.alias_bwx", DocNode.EMPTY));
                assertThat(httpResponse, isOk());
            }

            HttpResponse httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("remove_index.index", "index_bwx1"))));

            if (containsExactly(index_bwx2).isCoveredBy(user.indexMatcher("manage_index"))) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void reindex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/index_bwx1");

            HttpResponse httpResponse = restClient.postJson("/_reindex", DocNode.of("source.index", "index_br1", "dest.index", "index_bwx1"));
            if (containsExactly(index_bwx1).but(user.indexMatcher("create_index")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
                assertThat(cluster.getAdminCertRestClient().get("/index_bwx1/_search"), isNotFound());                                
            } else {
                assertThat(httpResponse, isOk());
                assertThat(cluster.getAdminCertRestClient().get("/index_bwx1/_search"), isOk());                                
            }
        }
    }

    @Test
    public void reindex2() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/index_bwx1");

            HttpResponse httpResponse = restClient.postJson("/_reindex", DocNode.of("source.index", "index_ar1", "dest.index", "index_bwx1"));

            if (user == UNLIMITED_USER || user == SUPER_UNLIMITED_USER || user == LIMITED_USER_AB_MANAGE_INDEX) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void cloneIndex() throws Exception {
        String sourceIndex = "index_bw1";
        String targetIndex = "index_bwx1";

        Client client = cluster.getInternalNodeClient();
        client.admin().indices()
                .updateSettings(new UpdateSettingsRequest(sourceIndex).settings(Settings.builder().put("index.blocks.write", true).build()))
                .actionGet();

        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed(targetIndex);

            HttpResponse httpResponse = restClient.post(sourceIndex + "/_clone/" + targetIndex);

            assertThat(httpResponse, containsExactly(index_bwx1).at("index").but(user.indexMatcher("manage_index")).whenEmpty(403));

        } finally {
            cluster.getInternalNodeClient().admin().indices()
                    .updateSettings(new UpdateSettingsRequest(sourceIndex).settings(Settings.builder().put("index.blocks.write", false).build()))
                    .actionGet();
        }
    }

    @Test
    public void closeIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.post("/index_bw1/_close");
            assertThat(httpResponse, containsExactly(index_bw1).at("indices.keys()").but(user.indexMatcher("manage_index")).whenEmpty(403));
        } finally {
            cluster.getInternalNodeClient().admin().indices().open(new OpenIndexRequest("index_bw1")).actionGet();
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
            cluster.getInternalNodeClient().admin().indices().open(new OpenIndexRequest("*")).actionGet();
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
            cluster.getInternalNodeClient().admin().indices().open(new OpenIndexRequest("index_bw1")).actionGet();
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
            cluster.getInternalNodeClient().admin().indices().open(new OpenIndexRequest("*")).actionGet();
        }
    }

    @After
    public void refresh() {
        cluster.getInternalNodeClient().admin().indices().refresh(new RefreshRequest("*")).actionGet();
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
