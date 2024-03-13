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

import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;

public class BulkConfigApiTest {
    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .sslEnabled().user(ADMIN_USER).build();

    @Test
    public void getTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.get("/_searchguard/config");
            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            Assert.assertEquals(response.getBody(), "basic/internal_users_db", responseDoc.getAsNode("authc").getAsNode("content").getAsListOfNodes("auth_domains").get(0).get("type"));
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
            Assert.assertNotNull(updateResponse.getBody(), updateResponse.getBodyAsDocNode().get("data", "tenants", "etag"));

            Thread.sleep(300);

            HttpResponse newGetResponse = client.get("/_searchguard/config");
            DocNode newGetResponseDoc = DocNode.wrap(DocReader.json().read(newGetResponse.getBody()));

            Assert.assertTrue(newGetResponse.getBody(),
                    newGetResponseDoc.getAsNode("tenants").getAsNode("content").get("my_new_test_tenant") != null);

            Assert.assertEquals(newGetResponse.getBody(), responseDoc.get("config"), newGetResponseDoc.get("config"));
        }

    }

    @Test
    public void configVarTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.putJson("/_searchguard/config/vars/bulk_test", DocNode.of("value", "bar"));
            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.putJson("/_searchguard/config/vars/bulk_test_encrypted", DocNode.of("value", "foo", "encrypt", true));
            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.get("/_searchguard/config");
            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            Assert.assertEquals(response.getBody(), "bar", responseDoc.get("config_vars", "content", "bulk_test", "value"));
            Assert.assertNotNull(response.getBody(), responseDoc.get("config_vars", "content", "bulk_test_encrypted", "encrypted"));
            Assert.assertNull(response.getBody(), responseDoc.get("config_vars", "content", "bulk_test_encrypted", "value"));

            response = client.delete("/_searchguard/config/vars/bulk_test_encrypted");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            Thread.sleep(20);
            response = client.get("/_searchguard/config/vars/bulk_test_encrypted");
            Assert.assertEquals(response.getBody(), 404, response.getStatusCode());

            DocNode updateRequestDoc = DocNode.of("config_vars.content", responseDoc.get("config_vars", "content"));
            System.out.println(updateRequestDoc.toJsonString());
            HttpResponse updateResponse = client.putJson("/_searchguard/config", updateRequestDoc);

            Assert.assertEquals(updateResponse.getBody(), 200, updateResponse.getStatusCode());

            Thread.sleep(20);
            response = client.get("/_searchguard/config/vars/bulk_test_encrypted");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
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

            DocNode updateResponseDoc = updateResponse.getBodyAsDocNode();

            Assert.assertEquals(updateResponse.getBody(), "'tenants.my_new_test_tenant.xxx': Unsupported attribute",
                    updateResponseDoc.getAsNode("error").get("message"));
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

            DocNode updateResponseDoc = updateResponse.getBodyAsDocNode();

            Assert.assertEquals(updateResponse.getBody(), "'foo': Invalid config type: foo", updateResponseDoc.getAsNode("error").get("message"));
        }
    }

    @Test
    public void putTestValidationError3_staticEntriesShouldBeRejected() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.get("/_searchguard/config");
            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            Map<String, Object> tenants = new LinkedHashMap<>(responseDoc.getAsNode("tenants").getAsNode("content"));

            tenants.put("my_new_test_tenant", ImmutableMap.of("description", "Test Tenant", "static", true));

            DocNode updateRequestDoc = DocNode.of("tenants.content", tenants);

            HttpResponse updateResponse = client.putJson("/_searchguard/config", updateRequestDoc.toJsonString());

            Assert.assertEquals(updateResponse.getBody(), 400, updateResponse.getStatusCode());

            DocNode updateResponseDoc = updateResponse.getBodyAsDocNode();

            Assert.assertEquals(
                    updateResponse.getBody(), "'tenants.content.my_new_test_tenant': Invalid value",
                    updateResponseDoc.getAsNode("error").get("message")
            );
            Assert.assertEquals(
                    updateResponse.getBody(), "Non-static entry",
                    updateResponseDoc.getAsNode("error").getAsNode("details").getAsListOfNodes("tenants.content.my_new_test_tenant").get(0).get("expected")
            );
        }
    }

    @Test
    public void putTestValidationError4_frontendAuthcLoginPageWithRelativePathShouldBeRejected() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.get("/_searchguard/config");
            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            Map<String, Object> frontendAuthc = new LinkedHashMap<>(responseDoc.getAsNode("frontend_authc").getAsNode("content"));

            frontendAuthc.put("default", ImmutableMap.of("login_page", ImmutableMap.of("brand_image", "/relative/img.png")));

            DocNode updateRequestDoc = DocNode.of("frontend_authc.content", frontendAuthc);

            HttpResponse updateResponse = client.putJson("/_searchguard/config", updateRequestDoc.toJsonString());

            Assert.assertEquals(updateResponse.getBody(), 400, updateResponse.getStatusCode());

            DocNode updateResponseDoc = updateResponse.getBodyAsDocNode();

            Assert.assertEquals(
                    updateResponse.getBody(), "'frontend_authc.default.login_page.brand_image': Must be an absolute URI",
                    updateResponseDoc.getAsNode("error").get("message")
            );
        }
    }

    @Test
    public void putTestValidationError5_rolesWhichAssignsPermsToNoExistentTenantsShouldBeRejected() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            String tenantName = "missing1";
            HttpResponse response = client.get("/_searchguard/config");
            DocNode responseDoc = response.getBodyAsDocNode();

            Map<String, Object> roles = new LinkedHashMap<>(responseDoc.getAsNode("roles").getAsNode("content"));

            roles.put("my-role", ImmutableMap.of("tenant_permissions", ImmutableMap.of(
                    "tenant_patterns", Collections.singletonList(tenantName + "*"), "allowed_actions", Collections.singletonList("*"))
            ));

            DocNode updateRequestDoc = DocNode.of("roles.content", roles);

            //update roles
            HttpResponse updateResponse = client.putJson("/_searchguard/config", updateRequestDoc);

            Assert.assertEquals(updateResponse.getBody(), HttpStatus.SC_BAD_REQUEST, updateResponse.getStatusCode());

            DocNode updateResponseDoc = updateResponse.getBodyAsDocNode();

            Assert.assertEquals(
                    updateResponse.getBody(), "Tenant pattern: '" + tenantName + "*' does not match any tenant",
                    updateResponseDoc.getAsNode("error").getAsNode("details").getAsListOfNodes("roles.my-role").get(0).get("error")
            );

            //add tenant
            Map<String, Object> tenants = new LinkedHashMap<>(responseDoc.getAsNode("tenants").getAsNode("content"));
            tenants.put(tenantName, ImmutableMap.of("description", "tenant"));
            response = client.putJson("/_searchguard/config", DocNode.of("tenants.content", tenants));
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            //update roles
            updateResponse = client.putJson("/_searchguard/config", updateRequestDoc);
            Assert.assertEquals(updateResponse.getBody(), HttpStatus.SC_OK, updateResponse.getStatusCode());
        }
    }

    @Test
    public void putRoleWhichAssignsPermsToGlobalTenant() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            String tenantName = "SGS_GLOBAL_TENANT";
            HttpResponse response = client.get("/_searchguard/config");
            DocNode responseDoc = response.getBodyAsDocNode();

            Map<String, Object> roles = new LinkedHashMap<>(responseDoc.getAsNode("roles").getAsNode("content"));

            roles.put("my-role", ImmutableMap.of("tenant_permissions", ImmutableMap.of(
                    "tenant_patterns", Collections.singletonList(tenantName + "*"), "allowed_actions", Collections.singletonList("*"))
            ));

            DocNode updateRequestDoc = DocNode.of("roles.content", roles);

            //update roles
            HttpResponse updateResponse = client.putJson("/_searchguard/config", updateRequestDoc);

            Assert.assertEquals(updateResponse.getBody(), HttpStatus.SC_OK, updateResponse.getStatusCode());
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
        try (LocalCluster cluster = new LocalCluster.Builder().sslEnabled().user(ADMIN_USER)
                .nodeSettings("searchguard.admin_only_actions", Collections.singletonList("x/x")).start()) {
            try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {

                HttpResponse updateResponse = client.get("/_searchguard/config");

                Assert.assertEquals(updateResponse.getBody(), 200, updateResponse.getStatusCode());
            }
        }
    }
}
