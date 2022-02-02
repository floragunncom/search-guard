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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.UUID;

import org.apache.http.message.BasicHeader;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;

public class InternalUsersConfigApiTest {

    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new TestSgConfig.Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().user(ADMIN_USER).build();

    @Test
    public void addUser_shouldAddUserIfPasswordIsPresent() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            DocNode userData = DocNode.of("password", "pass");

            HttpResponse response = client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString());

            assertEquals(201, response.getStatusCode());
            assertEquals(response.getBody(), "Internal User " + userName + " has been created", response.getBodyAsDocNode().get("message"));
        }
    }

    @Ignore
    @Test
    public void addUser_shouldFailWhenPasswordIsMissing() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            DocNode userData = DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "backend_roles", asList("backendRole1", "backendRole2"),
                    "attributes", ImmutableMap.of("a", "aAttributeValue"));

            HttpResponse response = client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString());

            assertEquals(400, response.getStatusCode());    
        }
    }

    @Test
    public void addUser_shouldFailWhenPasswordIsBlank() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            DocNode userData = DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "backend_roles", asList("backendRole1", "backendRole2"),
                    "attributes", ImmutableMap.of("a", "aAttributeValue"), "password", "");

            HttpResponse response = client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString());

            assertEquals(400, response.getStatusCode());
        }
    }

    @Test
    public void addUser_shouldSaveCorrectData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            DocNode userData = DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "backend_roles", asList("backendRole1", "backendRole2"),
                    "attributes", ImmutableMap.of("a", "aAttributeValue"), "password", "pass");
            client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString());

            HttpResponse response = client.get("/_searchguard/internal_users/" + userName);

            assertEquals(200, response.getStatusCode());
            assertEquals(response.getBody(), userData.without("password"), response.getBodyAsDocNode().getAsNode("data"));
        }
    }

    @Test
    public void addUser_shouldSaveWithEmptyBackendRolesIfMissingInARequest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            DocNode userData = DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "attributes", ImmutableMap.of("a", "b"), "password",
                    "pass");
            client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString());

            HttpResponse response = client.get("/_searchguard/internal_users/" + userName);

            assertEquals(200, response.getStatusCode());
            assertEquals(ImmutableMap.of("attributes", ImmutableMap.of("a", "b"), "search_guard_roles", Arrays.asList("sgRole1", "sgRole2")),
                    response.getBodyAsDocNode().getAsNode("data").toMap());
        }
    }

    @Test
    public void addUser_shouldSaveWithEmptyAttributesIfMissingInARequest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            DocNode userData = DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "backend_roles", asList("backendRole1", "backendRole2"),
                    "password", "pass");
            client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString());

            HttpResponse response = client.get("/_searchguard/internal_users/" + userName);

            assertEquals(200, response.getStatusCode());
            assertEquals(ImmutableMap.of("backend_roles", Arrays.asList("backendRole1", "backendRole2"), "search_guard_roles",
                    Arrays.asList("sgRole1", "sgRole2")), response.getBodyAsDocNode().getAsNode("data").toMap());
        }
    }

    @Test
    public void getUser_shouldFailWhenUserNotFound() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();

            HttpResponse response = client.get("/_searchguard/internal_users/" + userName);

            assertEquals(404, response.getStatusCode());
        }
    }

    @Test
    public void getUser_shouldReturnUserData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            DocNode userData = DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "backend_roles", asList("backendRole1", "backendRole2"),
                    "attributes", ImmutableMap.of("a", "aAttributeValue"), "password", "pass");
            client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString());

            HttpResponse response = client.get("/_searchguard/internal_users/" + userName);

            assertEquals(200, response.getStatusCode());
            assertEquals(response.getBody(), userData.without("password"), response.getBodyAsDocNode().getAsNode("data"));
            assertTrue(response.getHeaders().toString(), response.getHeaders().stream().anyMatch(h -> h.getName().equalsIgnoreCase("ETag")));
        }
    }

    @Test
    public void addUser_concurrencyControl() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            DocNode userData = DocNode.of("password", "pass");

            HttpResponse response = client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString());
            assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.get("/_searchguard/internal_users/" + userName);
            assertEquals(response.getBody(), 200, response.getStatusCode());

            String eTag = response.getHeaderValue("ETag");
            assertNotNull(response.getHeaders().toString(), eTag);

            userData = DocNode.of("password", "xyz");
            response = client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString(), new BasicHeader("If-Match", eTag));
            assertEquals(response.getBody(), 200, response.getStatusCode());

            userData = DocNode.of("password", "abc");
            response = client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString(), new BasicHeader("If-Match", eTag));
            assertEquals(response.getBody(), 412, response.getStatusCode());
            assertTrue(response.getBody(), response.getBody().contains("Unable to update configuration due to concurrent modification"));
        }
    }

    @Test
    public void patchUser_concurrencyControl() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            DocNode userData = DocNode.of("password", "pass", "description", "foo");

            HttpResponse response = client.putJson("/_searchguard/internal_users/" + userName, userData.toJsonString());
            assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.get("/_searchguard/internal_users/" + userName);
            assertEquals(response.getBody(), 200, response.getStatusCode());

            String eTag = response.getHeaderValue("ETag");
            assertNotNull(response.getHeaders().toString(), eTag);

            userData = DocNode.of("backend_roles", Arrays.asList("a", "b", "c"));
            response = client.patchJsonMerge("/_searchguard/internal_users/" + userName, userData, new BasicHeader("If-Match", eTag));
            assertEquals(response.getBody(), 200, response.getStatusCode());

            userData = DocNode.of("password", "abc");
            response = client.patchJsonMerge("/_searchguard/internal_users/" + userName, userData.toJsonString(), new BasicHeader("If-Match", eTag));
            assertEquals(response.getBody(), 412, response.getStatusCode());
            assertTrue(response.getBody(), response.getBody().contains("Unable to update configuration due to concurrent modification"));

            response = client.get("/_searchguard/internal_users/" + userName);
            assertEquals(response.getBody(), DocNode.of("description", "foo", "backend_roles", Arrays.asList("a", "b", "c")).toMap(),
                    response.getBodyAsDocNode().get("data"));
        }
    }

    @Test
    public void deleteUser_shouldInformThatUserNotFound() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();

            HttpResponse response = client.delete("/_searchguard/internal_users/" + userName);

            assertEquals(404, response.getStatusCode());
            assertEquals(response.getBody(), "Internal User " + userName + " does not exist", response.getBodyAsDocNode().get("error", "message"));
        }
    }

    @Test
    public void deleteUser_shouldDeleteUser() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            userExists(client, userName);

            HttpResponse response = client.delete("/_searchguard/internal_users/" + userName);

            assertEquals(200, response.getStatusCode());
        }
    }

    private void userExists(GenericRestClient client, String userName) throws Exception {
        HttpResponse response = client.putJson("/_searchguard/internal_users/" + userName, validUserData().toJsonString());
        assertEquals(201, response.getStatusCode());
    }

    private DocNode validUserData() {
        return DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "backend_roles", asList("backendRole1", "backendRole2"), "attributes",
                ImmutableMap.of("a", "aAttributeValue"), "password", "pass");
    }

    private String randomUserName() {
        return "userName_" + UUID.randomUUID();
    }

}