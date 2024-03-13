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

import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class DataStreamAuthorizationIntTest {

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A").roles(//
            new Role("limited_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("a*", "za*")
                    .dataStreamPermissions("*").on("ds_a*"));

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user").roles(//
            new Role("unlimited_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("*")
                    .dataStreamPermissions("*").on("*"));

    static TestIndex index_a1 = TestIndex.name("a1").documentCount(10).seed(1).attr("prefix", "a").build();
    static TestIndex index_a2 = TestIndex.name("a2").documentCount(10).seed(2).attr("prefix", "a").build();
    static TestIndex index_a3 = TestIndex.name("a3").documentCount(10).seed(3).attr("prefix", "a").build();
    static TestIndex index_b1 = TestIndex.name("b1").documentCount(10).seed(4).attr("prefix", "b").build();
    static TestIndex index_b2 = TestIndex.name("b2").documentCount(10).seed(5).attr("prefix", "b").build();
    static TestIndex index_b3 = TestIndex.name("b3").documentCount(10).seed(6).attr("prefix", "b").build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(LIMITED_USER_A, UNLIMITED_USER)//
            .indices(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3)//
            .build();

    @Test
    public void create() throws Exception {
        createDataStreamTemplates("test", "ds_*", DocNode.EMPTY);
        
        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.put("/_data_stream/ds_x");
            assertThat(httpResponse, isForbidden());

            httpResponse = restClient.put("/_data_stream/ds_a_ds1");
            assertThat(httpResponse, isOk());
        }
    }

    private void createDataStreamTemplates(String name, String pattern, DocNode mappingProperties) throws Exception {

        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String componentTemplate = name + "_component_template";

            HttpResponse httpResponse = client.putJson("/_component_template/" + componentTemplate, //
                    DocNode.of("template.mappings.properties",
                            mappingProperties.with("@timestamp", DocNode.of("type", "date", "format", "date_optional_time||epoch_millis"))));
            assertThat(httpResponse, isOk());

            httpResponse = client.putJson("/_index_template/" + name, DocNode.of("index_patterns", DocNode.array(pattern), "data_stream",
                    DocNode.EMPTY, "composed_of", DocNode.array(componentTemplate)));

            assertThat(httpResponse, isOk());
        }
    }
}
