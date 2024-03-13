/*
 * Copyright 2023 floragunn GmbH
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
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class GenericTypeLevelConfigApiTest {
    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().user(ADMIN_USER).build();

    @Test
    public void deleteTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse getResponseOld = client.get("/_searchguard/config");
            Assert.assertEquals(getResponseOld.getBody(), "true",
                    DocNode.wrap(DocReader.json().read(getResponseOld.getBody())).getAsNode("config").getAsString("exists"));

            HttpResponse deleteResponse = client.delete("/_searchguard/config/" + CType.CONFIG.getName());
            Assert.assertEquals(deleteResponse.getBody(), 200, deleteResponse.getStatusCode());

            HttpResponse getResponseNew = client.get("/_searchguard/config");
            Assert.assertTrue(getResponseNew.getBody(),
                    DocNode.wrap(DocReader.json().read(getResponseNew.getBody())).getAsNode("config").getAsNode("content").isEmpty());
            Assert.assertEquals(getResponseNew.getBody(), "false",
                    DocNode.wrap(DocReader.json().read(getResponseNew.getBody())).getAsNode("config").getAsString("exists"));

            deleteResponse = client.delete("/_searchguard/config/" + CType.CONFIG.getName());
            Assert.assertEquals(deleteResponse.getBody(), 404, deleteResponse.getStatusCode());
        }
    }

}
