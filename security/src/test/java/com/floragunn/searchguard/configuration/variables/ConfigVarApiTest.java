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

package com.floragunn.searchguard.configuration.variables;

import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class ConfigVarApiTest {

    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().user(ADMIN_USER).build();

    @Test
    public void putGetDeleteTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            String secretId = "test_var";
            String secretContent = "Blabla";
            String secretPath = "/_searchguard/config/vars/" + secretId;

            HttpResponse response = client.putJson(secretPath, DocWriter.json().writeAsString(ImmutableMap.of("value", secretContent)));

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            Thread.sleep(20);

            response = client.get(secretPath);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), secretContent, response.getBodyAsDocNode().get("data", "value"));

            response = client.get("/_searchguard/config/vars");
            DocNode responseDoc = response.getBodyAsDocNode();

            Assert.assertEquals(response.getBody(), secretContent, responseDoc.get("data", secretId, "value"));

            response = client.delete(secretPath);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            Thread.sleep(50);

            response = client.get(secretPath);

            Assert.assertEquals(response.getBody(), 404, response.getStatusCode());
        }
    }

    @Test
    public void indexMappingTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.putJson("/_searchguard/config/vars/m1", DocWriter.json().writeAsString(ImmutableMap.of("value", 1)));
            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.putJson("/_searchguard/config/vars/m2", DocWriter.json().writeAsString(ImmutableMap.of("value", "foo")));
            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.putJson("/_searchguard/config/vars/m3",
                    DocWriter.json().writeAsString(ImmutableMap.of("value", ImmutableMap.of("a", 1, "b", 2))));
            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            Thread.sleep(20);

            response = client.get("/_searchguard/config/vars/m1");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), 1, response.getBodyAsDocNode().get("data", "value"));

            response = client.get("/_searchguard/config/vars/m2");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "foo", response.getBodyAsDocNode().get("data", "value"));

            response = client.get("/_searchguard/config/vars/m3");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), ImmutableMap.of("a", 1, "b", 2), response.getBodyAsDocNode().get("data", "value"));
        }
    }

    @Test
    public void encryptedPutGetDeleteTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            String secretId = "test_secret";
            String secretContent = "Foobar";
            String secretPath = "/_searchguard/config/vars/" + secretId;

            HttpResponse response = client.putJson(secretPath,
                    DocWriter.json().writeAsString(ImmutableMap.of("value", secretContent, "encrypt", true)));

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            Thread.sleep(20);

            response = client.get(secretPath);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertNotNull(response.getBody(), response.getBodyAsDocNode().get("data", "encrypted", "value"));

            response = client.get("/_searchguard/config/vars");
            DocNode responseDoc = response.getBodyAsDocNode();

            Assert.assertNotNull(response.getBody(), responseDoc.get("data", secretId, "encrypted", "value"));

            response = client.delete(secretPath);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            Thread.sleep(50);

            response = client.get(secretPath);

            Assert.assertEquals(response.getBody(), 404, response.getStatusCode());
        }
    }

    @Test
    public void putIfNoneMatchTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            String secretId = "test_if_n_m";
            String secretContent = "Blabla";
            String secretPath = "/_searchguard/config/vars/" + secretId;

            HttpResponse response = client.putJson(secretPath, DocWriter.json().writeAsString(ImmutableMap.of("value", secretContent)),
                    new BasicHeader("If-None-Match", "*"));

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            Thread.sleep(20);

            response = client.putJson(secretPath, DocWriter.json().writeAsString(ImmutableMap.of("value", secretContent + "2")),
                    new BasicHeader("If-None-Match", "*"));

            Assert.assertEquals(response.getBody(), 412, response.getStatusCode());

            response = client.putJson(secretPath, DocWriter.json().writeAsString(ImmutableMap.of("value", secretContent + "2")));

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

        }
    }

    @Test
    public void putTestWithoutAdminCert() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {

            HttpResponse updateResponse = client.putJson("/_searchguard/config/vars/foobar", DocWriter.json().writeAsString(DocNode.of("a", "b")));

            Assert.assertEquals(updateResponse.getBody(), 403, updateResponse.getStatusCode());
        }
    }
}
