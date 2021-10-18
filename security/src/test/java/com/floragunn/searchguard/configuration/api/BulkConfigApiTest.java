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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.google.common.collect.ImmutableMap;

public class BulkConfigApiTest {
    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    public static TestCertificates testCertificates = TestCertificates.builder()
            .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard")
            .build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled(testCertificates).user(ADMIN_USER).build();

    @Test
    public void getTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.get("/_searchguard/config");
            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            Assert.assertEquals(response.getBody(), "config", responseDoc.getAsNode("config").getAsNode("content").getAsNode("_sg_meta").get("type"));
            Assert.assertEquals(response.getBody(), "internalusers",
                    responseDoc.getAsNode("internalusers").getAsNode("content").getAsNode("_sg_meta").get("type"));
        }
    }

    @Test
    public void putTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.get("/_searchguard/config");
            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            Map<String, Object> tenants = new LinkedHashMap<>(responseDoc.getAsNode("tenants").getAsNode("content"));

            tenants.put("my_new_test_tenant", ImmutableMap.of("description", "Test Tenant"));

            DocNode updateRequestDoc = DocNode.of("tenants.content", tenants);

            HttpResponse updateResponse = client.putJson("/_searchguard/config", updateRequestDoc.toJsonString());

            Assert.assertEquals(updateResponse.getBody(), 200, updateResponse.getStatusCode());

            Thread.sleep(300);

            HttpResponse newGetResponse = client.get("/_searchguard/config");
            DocNode newGetResponseDoc = DocNode.wrap(DocReader.json().read(newGetResponse.getBody()));

            Assert.assertTrue(newGetResponse.getBody(),
                    newGetResponseDoc.getAsNode("tenants").getAsNode("content").get("my_new_test_tenant") != null);

            Assert.assertEquals(newGetResponse.getBody(), responseDoc.get("config"), newGetResponseDoc.get("config"));
        }

    }

    @Test
    public void putTestValidationError1() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.get("/_searchguard/config");
            System.out.println(response.toString());
            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            Map<String, Object> tenants = new LinkedHashMap<>(responseDoc.getAsNode("tenants").getAsNode("content"));

            tenants.put("my_new_test_tenant", ImmutableMap.of("xxx", "Test Tenant"));

            DocNode updateRequestDoc = DocNode.of("tenants.content", tenants);

            HttpResponse updateResponse = client.putJson("/_searchguard/config", updateRequestDoc.toJsonString());

            Assert.assertEquals(updateResponse.getBody(), 400, updateResponse.getStatusCode());

            DocNode updateResponseDoc = DocNode.wrap(DocReader.json().read(updateResponse.getBody()));

            Assert.assertEquals(updateResponse.getBody(), "'tenants.my_new_test_tenant.xxx': Unsupported attribute", updateResponseDoc.get("error.message"));
        }
    }

    @Test
    public void putTestValidationError2() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.get("/_searchguard/config");
            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            Map<String, Object> tenants = new LinkedHashMap<>(responseDoc.getAsNode("tenants").getAsNode("content"));

            tenants.put("my_new_test_tenant", ImmutableMap.of("xxx", "Test Tenant"));

            DocNode updateRequestDoc = DocNode.of("tenants.content", tenants, "foo.content", ImmutableMap.of("yyy", "Bla"));

            HttpResponse updateResponse = client.putJson("/_searchguard/config", updateRequestDoc.toJsonString());

            Assert.assertEquals(updateResponse.getBody(), 400, updateResponse.getStatusCode());

            DocNode updateResponseDoc = DocNode.wrap(DocReader.json().read(updateResponse.getBody()));

            Assert.assertEquals(updateResponse.getBody(), "'foo': Invalid config type: foo", updateResponseDoc.get("error.message"));
        }
    }

    @Test
    public void putTestWithoutAdminCert() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {

            HttpResponse updateResponse = client.putJson("/_searchguard/config", DocWriter.json().writeAsString(DocNode.of("a", "b")));

            Assert.assertEquals(updateResponse.getBody(), 403, updateResponse.getStatusCode());
        }
    }

    @Test
    public void getTestWithoutAdminCert() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {

            HttpResponse updateResponse = client.get("/_searchguard/config");

            Assert.assertEquals(updateResponse.getBody(), 403, updateResponse.getStatusCode());
        }
    }

    @Test
    public void getTestWithoutAdminCertWithAllowedAction() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder().sslEnabled(testCertificates).user(ADMIN_USER)
                .nodeSettings("searchguard.actions.admin_only", Collections.emptyList()).build()) {
            try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {

                HttpResponse updateResponse = client.get("/_searchguard/config");

                Assert.assertEquals(updateResponse.getBody(), 200, updateResponse.getStatusCode());
            }
        }
    }
}
