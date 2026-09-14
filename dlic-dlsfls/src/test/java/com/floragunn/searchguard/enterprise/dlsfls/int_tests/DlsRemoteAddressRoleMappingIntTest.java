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

package com.floragunn.searchguard.enterprise.dlsfls.int_tests;

import static com.floragunn.searchguard.test.IndexApiMatchers.containsExactly;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.search.SearchService;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.RoleMapping;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class DlsRemoteAddressRoleMappingIntTest {
    static final String REMOTE_ADDRESS_FOR_ROLE_MAPPING = "192.0.2.1";

    static TestIndex index = TestIndex.name("index").documentCount(100).seed(1).attr("prefix", "a")//
            .setting("index.number_of_shards", 5).build();

    static TestSgConfig.User user = new TestSgConfig.User("remote_address_dls_user")//
            .indexMatcher("read", limitedTo(index.filteredBy(node -> node.getAsString("dept").startsWith("dept_a"))));

    static Role role = new Role("remote_address_dls_role")//
            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")//
            .indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on(index.getName());

    static RoleMapping roleMapping = new RoleMapping(role.getName()).ips(REMOTE_ADDRESS_FOR_ROLE_MAPPING);

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"))//
            .trustedProxies("127.0.0.1");
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().metrics("detailed");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC)//
            .user(user)//
            .roles(role)//
            .roleMapping(roleMapping)//
            .indices(index)//
            .authzDebug(true)//
            .logRequests()//
            .dlsFls(DLSFLS)//
            .nodeSettings(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), true)//
            .build();

    @Test
    public void search_withDlsRoleMappedByRemoteAddress_chunkedFetch() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user,
                new BasicHeader("X-Forwarded-For", REMOTE_ADDRESS_FOR_ROLE_MAPPING))) {
            HttpResponse httpResponse = restClient.get("/" + index.getName() + "/_search?size=1000");

            assertThat(httpResponse, containsExactly(index).at("hits.hits[*]").but(user.indexMatcher("read")));
        }
    }
}
