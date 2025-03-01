/*
 * Copyright 2025 floragunn GmbH
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

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class BasicRoleMappingIntTests {
    static TestSgConfig.User SIMPLE_USER = new TestSgConfig.User("simple_user")//
            .backendRoles("backend_role_1", "backend_role_2", "backend_role_unmapped");

    static TestSgConfig.RoleMapping ROLE_MAPPING[] = new TestSgConfig.RoleMapping[] {
            new TestSgConfig.RoleMapping("role_1").backendRoles("backend_role_1"),
            new TestSgConfig.RoleMapping("role_2").backendRoles("backend_role_2") //
    };

    static List<TestSgConfig.User> USERS = ImmutableList.of(SIMPLE_USER);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(USERS).roleMapping(ROLE_MAPPING).build();
    
    /**
     * Moved from test_internal_authorizer.sh
     */
    @Test
    public void basicUserMapping_authInfo() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(SIMPLE_USER)) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("sg_roles", containsInAnyOrder("role_1", "role_2"))));
        }
    }


}
