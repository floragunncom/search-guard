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
 *
 */

package com.floragunn.signals.api;

import static com.floragunn.searchguard.test.RestMatchers.isBadRequest;
import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.awaitility.Awaitility;
import org.hamcrest.BaseMatcher;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.FrontendMultiTenancy;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.Tenant;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.actions.account.delete.TenantDeleteAccountAction;
import com.floragunn.signals.actions.account.get.TenantGetAccountAction;
import com.floragunn.signals.actions.account.put.TenantPutAccountAction;
import com.floragunn.signals.actions.account.search.SearchAccountAction;
import com.floragunn.signals.actions.account.search.TenantSearchAccountAction;
import com.floragunn.signals.actions.watch.put.PutWatchAction;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;

public class TenantAccountApiTest {
    private static final String TENANT_1 = "account_tenant_1";
    private static final String TENANT_2 = "account_tenant_2";

    private static final User USER_1 = tenantUser("account_user_1", TENANT_1);
    private static final User USER_2 = tenantUser("account_user_2", TENANT_2);
    private static final User FRONTEND_SERVER_USER = new User("account_frontend_server");
    private static final User SEARCH_ONLY_USER = new User("account_search_only").roles(new Role("account_search_only_role")
            .clusterPermissions(SearchAccountAction.NAME));

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .enterpriseModulesEnabled()
            .nodeSettings("signals.enterprise.enabled", false)
            .frontendMultiTenancy(new FrontendMultiTenancy(true).index(".kibana").serverUser(FRONTEND_SERVER_USER.getName()))
            .tenants(new Tenant(TENANT_1), new Tenant(TENANT_2))
            .users(USER_1, USER_2, FRONTEND_SERVER_USER, SEARCH_ONLY_USER)
            .enableModule(SignalsModule.class).waitForComponents("signals").embedded().build();

    @Test
    public void tenantCrudAndSearchArePrivilegeScoped() throws Exception {
        String globalPath = "/_signals/account/email/global_account";
        String tenant1Path = "/_signals/account/" + TENANT_1 + "/email/tenant_1_account";
        String tenant2Path = "/_signals/account/" + TENANT_2 + "/email/tenant_2_account";
        String accountJson = "{\"host\":\"127.0.0.1\",\"port\":25}";

        try (GenericRestClient admin = cluster.getAdminCertRestClient(); GenericRestClient user1 = cluster.getRestClient(USER_1);
                GenericRestClient user2 = cluster.getRestClient(USER_2); GenericRestClient searchOnly = cluster.getRestClient(SEARCH_ONLY_USER)) {
            try {
                assertThat(admin.putJson(globalPath, accountJson), isCreated());
                assertThat(user1.putJson(tenant1Path, accountJson), isCreated());
                assertThat(user2.putJson(tenant2Path, accountJson), isCreated());

                assertThat(user1.get(tenant1Path), isOk());
                assertThat(user1.get(tenant2Path), isForbidden());
                assertThat(user1.putJson(tenant2Path, accountJson), isForbidden());
                assertThat(user1.delete(tenant2Path), isForbidden());

                assertSearchVisibility(user1.get("/_signals/account/_search"), "global_account", null, "tenant_1_account", "tenant_2_account");
                assertSearchVisibility(user1.get("/_signals/account/" + TENANT_1 + "/_search"), "global_account", "tenant_1_account",
                        "tenant_2_account");
                assertThat(user1.get("/_signals/account/" + TENANT_2 + "/_search"), isForbidden());

                assertSearchVisibility(user2.get("/_signals/account/_search"), "global_account", null, "tenant_1_account", "tenant_2_account");
                assertSearchVisibility(user2.get("/_signals/account/" + TENANT_2 + "/_search"), "global_account", "tenant_2_account",
                        "tenant_1_account");

                assertSearchVisibility(searchOnly.get("/_signals/account/_search"), "global_account", null, "tenant_1_account",
                        "tenant_2_account");
                assertThat(searchOnly.get("/_signals/account/" + TENANT_1 + "/_search"), isForbidden());
            } finally {
                admin.delete(globalPath);
                user1.delete(tenant1Path);
                user2.delete(tenant2Path);
            }
        }
    }

    @Test
    public void tenantAccountCanOnlyBeUsedByWatchInMatchingTenant() throws Exception {
        String accountPath = "/_signals/account/" + TENANT_1 + "/email/tenant_only_account";
        String watch1Path = "/_signals/watch/" + TENANT_1 + "/matching_account_watch";
        String watch2Path = "/_signals/watch/" + TENANT_2 + "/foreign_account_watch";
        String accountJson = "{\"host\":\"127.0.0.1\",\"port\":25}";
        Watch watch = new WatchBuilder("account_watch").cronTrigger("0 0 12 31 12 ? 2075").then().email("subject")
                .account("tenant_only_account").body("body").to("to@example.test").name("send_email").build();

        try (GenericRestClient user1 = cluster.getRestClient(USER_1); GenericRestClient user2 = cluster.getRestClient(USER_2)) {
            try {
                assertThat(user1.putJson(accountPath, accountJson), isCreated());

                Awaitility.await("Tenant account is available to watch validation").atMost(Duration.ofSeconds(5))
                        .pollInterval(Duration.ofMillis(100))
                        .untilAsserted(() -> assertThat(user1.putJson(watch1Path, watch.toJson()), isCreated()));

                HttpResponse foreignResponse = user2.putJson(watch2Path, watch.toJson());
                assertThat(foreignResponse, isBadRequest("error", "*Account does not exist*"));
            } finally {
                user1.delete(watch1Path);
                user2.delete(watch2Path);
                user1.delete(accountPath);
            }
        }
    }

    private static User tenantUser(String name, String tenant) {
        return new User(name).roles(new Role(name + "_role").clusterPermissions(SearchAccountAction.NAME)
                .withTenantPermission(TenantGetAccountAction.NAME, TenantPutAccountAction.NAME, TenantDeleteAccountAction.NAME,
                        TenantSearchAccountAction.NAME, PutWatchAction.NAME)
                .on(tenant));
    }

    private static void assertSearchVisibility(HttpResponse response, String globalId, String visibleTenantId, String... hiddenTenantIds) {
        List<BaseMatcher<?>> idMatchers = new ArrayList<>();
        idMatchers.add(nodeAt("$.hits.hits[*]._id", hasItem(containsString("email/" + globalId))));
        if (visibleTenantId != null) {
            idMatchers.add(nodeAt("$.hits.hits[*]._id", hasItem(containsString(visibleTenantId))));
        }
        for (String hiddenTenantId : hiddenTenantIds) {
            idMatchers.add(nodeAt("$.hits.hits[*]._id", not(hasItem(containsString(hiddenTenantId)))));
        }

        assertThat(response, allOf(isOk(), json(idMatchers.toArray(new BaseMatcher<?>[0]))));
    }

}
