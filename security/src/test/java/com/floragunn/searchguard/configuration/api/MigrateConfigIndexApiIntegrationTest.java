/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.configuration.api;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocWriter;
import com.floragunn.searchguard.authc.internal_users_db.InternalUser;
import com.floragunn.searchguard.authc.rest.RestAuthcConfig;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class MigrateConfigIndexApiIntegrationTest {
    static TestSgConfig.User TEST_USER_A = new TestSgConfig.User("test_user_a").roles(//
            new Role("test_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("a*"));

    static TestSgConfig.User TEST_USER_B = new TestSgConfig.User("test_user_b").roles(//
            new Role("unlimited_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("*"));

    static TestIndex index_a1 = TestIndex.name("a1").documentCount(100).seed(1).attr("prefix", "a").build();
    static TestIndex index_b1 = TestIndex.name("b1").documentCount(51).seed(4).attr("prefix", "b").build();

    @Test
    public void test() throws Exception {
        try (LocalCluster.Embedded cluster = new LocalCluster.Builder().embedded().singleNode().sslEnabled()//
                .configIndexName("searchguard")//
                .users(TEST_USER_A, TEST_USER_B)//
                .indices(index_a1, index_b1)//
                .start()) {

            ConfigurationRepository configurationRespository = cluster.getInjectable(ConfigurationRepository.class);
            Assert.assertEquals("searchguard", configurationRespository.getEffectiveSearchGuardIndex());
            SgDynamicConfiguration<InternalUser> oldUsersConfig = configurationRespository.getConfiguration(CType.INTERNALUSERS);
            SgDynamicConfiguration<com.floragunn.searchguard.authz.config.Role> oldRolesConfig = configurationRespository
                    .getConfiguration(CType.ROLES);
            SgDynamicConfiguration<RestAuthcConfig> oldAuthcConfig = configurationRespository.getConfiguration(CType.AUTHC);

            try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
                GenericRestClient.HttpResponse response = restClient.post("/_searchguard/config/migrate_index");

                System.out.println(response.getBody());

                Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

                Thread.sleep(500);

                Assert.assertEquals(".searchguard", configurationRespository.getEffectiveSearchGuardIndex());

                SgDynamicConfiguration<InternalUser> newUsersConfig = configurationRespository.getConfiguration(CType.INTERNALUSERS);
                SgDynamicConfiguration<com.floragunn.searchguard.authz.config.Role> newRolesConfig = configurationRespository
                        .getConfiguration(CType.ROLES);
                SgDynamicConfiguration<RestAuthcConfig> newAuthcConfig = configurationRespository.getConfiguration(CType.AUTHC);

                Assert.assertTrue(oldUsersConfig != newUsersConfig);
                Assert.assertTrue(oldRolesConfig != newRolesConfig);
                Assert.assertTrue(oldAuthcConfig != newAuthcConfig);

                Assert.assertEquals(DocWriter.json().writeAsString(oldUsersConfig.getCEntries()),
                        DocWriter.json().writeAsString(newUsersConfig.getCEntries()));
                Assert.assertEquals(DocWriter.json().writeAsString(oldRolesConfig.getCEntries()),
                        DocWriter.json().writeAsString(newRolesConfig.getCEntries()));
                Assert.assertEquals(DocWriter.json().writeAsString(oldAuthcConfig.getCEntries()),
                        DocWriter.json().writeAsString(newAuthcConfig.getCEntries()));
            }
        }
    }
}
