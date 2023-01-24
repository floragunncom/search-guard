/*
 * Copyright 2021 floragunn GmbH
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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.patch.JsonPathPatch;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.jayway.jsonpath.JsonPath;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class ApiIntegrationTest {
    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().user(ADMIN_USER).build();

    @Test
    public void patchAuthc() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            client.patch("/_searchguard/config/authc", new JsonPathPatch(new JsonPathPatch.Operation(JsonPath.compile("debug"), true)));
        }
    }

    @Test
    public void putLicenseKeyBadEncoding() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = client.putJson("/_searchguard/license/key", DocNode.of("key", "ggfgf"));
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Invalid base64 encoding"));
        }
    }

    @Test
    public void putLicenseKeyBadContent() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = client.putJson("/_searchguard/license/key", DocNode.of("key", "aGVsbG8K"));
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Cannot find license signature"));
        }
    }
}
