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

import java.util.Arrays;
import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocParseException;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
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

            assertEquals(200, response.getStatusCode());
            assertEquals("User " + userName + " has been added", getMessage(response));
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
            assertEquals("{message='password': Invalid value, details={password=[{error=Invalid value, value=null, expected=Password}]}}",
                    getError(response));
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
            assertEquals("{backend_roles=[backendRole1, backendRole2], attributes={a=aAttributeValue}, search_guard_roles=[sgRole1, sgRole2]}",
                    getData(response));
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
            assertEquals("{message=User " + userName + " not found}", getError(response));
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
            assertEquals("{backend_roles=[backendRole1, backendRole2], attributes={a=aAttributeValue}, search_guard_roles=[sgRole1, sgRole2]}",
                    getData(response));
        }
    }

    @Ignore
    @Test
    public void updateUser_shouldReturnMessageThatUserNotFound() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            HttpResponse response = client.patch("/_searchguard/internal_users/" + userName,
                    DocNode.of("backend_roles", asList("backendRole2", "backendRole3")).toJsonString());

            assertEquals(response.getBody(), 404, response.getStatusCode());
            assertEquals("{message=User " + userName + " not found}", getError(response));
        }
    }

    @Ignore
    @Test
    public void updateUser_shouldBeAbleToUpdateBackendRoles() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            client.putJson("/_searchguard/internal_users/" + userName,
                    DocNode.of("backend_roles", asList("backendRole1", "backendRole2"), "password", "pass").toJsonString());

            HttpResponse response = client.patch("/_searchguard/internal_users/" + userName,
                    DocNode.of("backend_roles", asList("backendRole2", "backendRole3")).toJsonString());

            assertEquals(200, response.getStatusCode());
            assertEquals("User " + userName + " has been updated", getMessage(response));
        }
    }

    @Ignore
    @Test
    public void updateUser_shouldBeAbleToUpdateSearchGuardRoles() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            client.putJson("/_searchguard/internal_users/" + userName,
                    DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "password", "pass").toJsonString());

            HttpResponse response = client.patch("/_searchguard/internal_users/" + userName,
                    DocNode.of("search_guard_roles", asList("sgRole1", "sgRole3")).toJsonString());

            assertEquals(200, response.getStatusCode());
            assertEquals("User " + userName + " has been updated", getMessage(response));
        }
    }

    @Ignore
    @Test
    public void updateUser_shouldBeAbleToUpdateAttributes() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            client.putJson("/_searchguard/internal_users/" + userName,
                    DocNode.of("attributes", ImmutableMap.of("a", "aValue"), "password", "pass").toJsonString());

            HttpResponse response = client.patch("/_searchguard/internal_users/" + userName,
                    DocNode.of("attributes", ImmutableMap.of("a", "bValue")).toJsonString());

            assertEquals(200, response.getStatusCode());
            assertEquals("User " + userName + " has been updated", getMessage(response));
        }
    }

    @Ignore
    @Test
    public void updateUser_shouldBeAbleToUpdatePassword() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            client.putJson("/_searchguard/internal_users/" + userName, DocNode.of("password", "pass").toJsonString());

            HttpResponse response = client.patch("/_searchguard/internal_users/" + userName, DocNode.of("password", "pass3").toJsonString());

            assertEquals(200, response.getStatusCode());
            assertEquals("User " + userName + " has been updated", getMessage(response));
        }
    }

    @Ignore
    @Test
    public void updateUser_shouldUpdateData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            client.putJson("/_searchguard/internal_users/" + userName,
                    DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "backend_roles", asList("backendRole1", "backendRole2"),
                            "attributes", ImmutableMap.of("a", "aAttributeValue"), "password", "pass").toJsonString());

            client.patch("/_searchguard/internal_users/" + userName,
                    DocNode.of("search_guard_roles", asList("sgRole1", "sgRole3"), "backend_roles", asList("backendRole1", "backendRole3"),
                            "attributes", ImmutableMap.of("a", "aValue", "b", "bValue"), "password", "pass").toJsonString());

            HttpResponse response = client.get("/_searchguard/internal_users/" + userName);

            assertEquals(200, response.getStatusCode());
            assertEquals("{backend_roles=[backendRole1, backendRole3], attributes={a=aValue, b=bValue}, search_guard_roles=[sgRole1, sgRole3]}",
                    getData(response));
        }
    }

    @Test
    public void deleteUser_shouldInformThatUserNotFound() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();

            HttpResponse response = client.delete("/_searchguard/internal_users/" + userName);

            assertEquals(404, response.getStatusCode());
            assertEquals("{message=User " + userName + " for deletion not found}", getError(response));
        }
    }

    @Test
    public void deleteUser_shouldDeleteUser() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            String userName = randomUserName();
            userExists(client, userName);

            HttpResponse response = client.delete("/_searchguard/internal_users/" + userName);

            assertEquals(204, response.getStatusCode());
        }
    }

    private void userExists(GenericRestClient client, String userName) throws Exception {
        HttpResponse response = client.putJson("/_searchguard/internal_users/" + userName, validUserData().toJsonString());
        assertEquals(200, response.getStatusCode());
    }

    private DocNode validUserData() {
        return DocNode.of("search_guard_roles", asList("sgRole1", "sgRole2"), "backend_roles", asList("backendRole1", "backendRole2"), "attributes",
                ImmutableMap.of("a", "aAttributeValue"), "password", "pass");
    }

    private String randomUserName() {
        return "userName_" + UUID.randomUUID();
    }

    private String getError(HttpResponse response) throws DocParseException {
        DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));
        return responseDoc.getAsString("error");
    }

    private String getMessage(HttpResponse response) throws DocParseException {
        DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));
        return responseDoc.getAsString("message");
    }

    private String getData(HttpResponse response) throws DocParseException {
        DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));
        return responseDoc.getAsString("data");
    }
}