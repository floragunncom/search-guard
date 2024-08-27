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
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.floragunn.searchguard.test.RestMatchers.*;

import org.elasticsearch.cluster.service.ClusterService;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class MetadataUpdateIntTest {
    private static final Logger log = LogManager.getLogger(MetadataUpdateIntTest.class);

    static TestSgConfig.User TEST_USER = new TestSgConfig.User("test_user")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_CRUD", "SGS_INDICES_MONITOR", "indices:admin/refresh*", "indices:admin/auto_create", "SGS_MANAGE_ALIASES").on("index_a*")
                            .aliasPermissions("SGS_MANAGE_ALIASES").on("alias_a*"));

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled() //
            .users(TEST_USER)//
            .authzDebug(true)//
            .embedded().build();

    @Test
    public void test() throws Exception {
        ClusterService clusterService = cluster.getInjectable(ClusterService.class);

        try (GenericRestClient restClient = cluster.getRestClient(TEST_USER)) {
            log.info("V1 " + clusterService.state().metadata().version());

            HttpResponse httpResponse = restClient.putJson("/index_a1/_doc/put_test_1", DocNode.of("a", 1));
            assertThat(httpResponse, isCreated());

            log.info("V2 " + clusterService.state().metadata().version());

            Thread.sleep(100);

            log.info("V3 " + clusterService.state().metadata().version());

            httpResponse = restClient.putJson("/index_a2/_doc/put_test_1", DocNode.of("a", 1));
            assertThat(httpResponse, isCreated());

            log.info("V4 " + clusterService.state().metadata().version());

            httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("add.index", "index_a1", "add.alias", "alias_a"))));
            assertThat(httpResponse, isOk());
            
            log.info("V5 " + clusterService.state().metadata().version());


            httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("add.index", "index_a2", "add.alias", "alias_a"))));
            assertThat(httpResponse, isOk());
            
            log.info("V6 " + clusterService.state().metadata().version());

        }
    }

}
