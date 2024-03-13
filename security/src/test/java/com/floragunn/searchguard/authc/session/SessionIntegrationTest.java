/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.session;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.BearerAuthorization;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class SessionIntegrationTest {

    static TestSgConfig.User BASIC_USER = new TestSgConfig.User("basic_user").roles("sg_all_access");
    static TestSgConfig.User NO_ROLES_USER = new TestSgConfig.User("no_roles_user");
    
    static TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(
            new TestSgConfig.Authc.Domain("basic/internal_users_db").skipOriginatingIps("127.0.0.22")).trustedProxies("127.0.0.42");
    
    static TestSgConfig TEST_SG_CONFIG = new TestSgConfig().resources("session")
            .authc(AUTHC)
            .frontendAuthc("default", new TestSgConfig.FrontendAuthc()
                    .authDomain(new TestSgConfig.FrontendAuthDomain("basic").label("Basic Login")))//
            .frontendAuthc("test_fe", new TestSgConfig.FrontendAuthc()
                    .authDomain(new TestSgConfig.FrontendAuthDomain(TestApiAuthenticationFrontend.class.getName()).label("Test Login")))
            .user(NO_ROLES_USER).user(BASIC_USER);

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin")
            .resources("session").sgConfig(TEST_SG_CONFIG).sslEnabled().embedded().build();

    @Test
    public void startSession_basic() throws Exception {
        String token;

        try (GenericRestClient restClient = cluster.getRestClient()) {
            HttpResponse response = restClient.postJson("/_searchguard/auth/session", basicAuthRequest(BASIC_USER));

            System.out.println(response.getBody());

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());
            token = response.getBodyAsDocNode().getAsString("token");

            Assert.assertNotNull(response.getBody(), token);
        }

        try (GenericRestClient restClient = cluster.getRestClient(new BearerAuthorization(token))) {
            HttpResponse response = restClient.get("/_searchguard/authinfo");

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), BASIC_USER.getName(), response.getBodyAsDocNode().getAsString("user_name"));
        }
    }
    
    @Test
    public void startSession_header() throws Exception {
        String token;

        try (GenericRestClient restClient = cluster.getRestClient(BASIC_USER)) {
            HttpResponse response = restClient.post("/_searchguard/auth/session/with_header");

            System.out.println(response.getBody());

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            token = response.getBodyAsDocNode().getAsString("token");
            
            Assert.assertNotNull(response.getBody(), token);
        }

        try (GenericRestClient restClient = cluster.getRestClient(new BearerAuthorization(token))) {
            HttpResponse response = restClient.get("/_searchguard/authinfo");

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), BASIC_USER.getName(), response.getBodyAsDocNode().get("user_name"));
        }
    }
    
    @Test
    public void startSession_trustedProxy() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient()) {
            restClient.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 42 }));
            HttpResponse response = restClient.postJson("/_searchguard/auth/session", basicAuthRequest(BASIC_USER),
                    new BasicHeader("X-Forwarded-For", "127.0.0.21"));

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());
        }

        try (GenericRestClient restClient = cluster.getRestClient()) {
            restClient.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 42 }));
            HttpResponse response = restClient.postJson("/_searchguard/auth/session", basicAuthRequest(BASIC_USER),
                    new BasicHeader("X-Forwarded-For", "127.0.0.22"));

            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());
        }

        try (GenericRestClient restClient = cluster.getRestClient()) {
            HttpResponse response = restClient.postJson("/_searchguard/auth/session", basicAuthRequest(BASIC_USER),
                    new BasicHeader("X-Forwarded-For", "127.0.0.22"));

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());
        }
    }


    @Test
    public void nonDefaultConfigTest() throws Exception {
        String token;

        try (GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            HttpResponse response = restClient.get("/_searchguard/auth/config?config_id=test_fe");
            Assert.assertEquals(response.getBody(), "test", response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).get("method"));
            Assert.assertEquals(response.getBody(), 1, response.getBodyAsDocNode().getAsListOfNodes("auth_methods").size());
        }

        try (GenericRestClient restClient = cluster.getRestClient()) {
            HttpResponse response = restClient.postJson("/_searchguard/auth/session",
                    testAuthRequest("test_user", "config_id", "test_fe", "roles", "backend_role_all_access"));

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            token = response.getBodyAsDocNode().getAsString("token");
            Assert.assertNotNull(response.getBody(), token);
        }

        try (GenericRestClient restClient = cluster.getRestClient(new BearerAuthorization(token))) {
            HttpResponse response = restClient.get("/_searchguard/authinfo");

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "test_user", response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), "backend_role_all_access",
                    response.getBodyAsDocNode().getAsListOfNodes("backend_roles").get(0).toString());
        }
    }

    @Test
    public void configsAreSeparatedTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient()) {
            HttpResponse response = restClient.postJson("/_searchguard/auth/session",
                    testAuthRequest("test_user", "roles", "backend_role_all_access"));
            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());

        }
    }

    @Test
    public void noRolesTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient()) {
            HttpResponse response = restClient.postJson("/_searchguard/auth/session", basicAuthRequest(NO_ROLES_USER));
            Assert.assertEquals(response.getBody(), 403, response.getStatusCode());
            Assert.assertEquals("The user 'no_roles_user' is not allowed to log in.", response.getBodyAsDocNode().get("error"));
        }
    }
    
    @Test
    public void autogeneratedKeysAreEncrypted() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            HttpResponse response = restClient.get("/_searchguard/config/vars/sessions_signing_key");
            
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            System.out.println(response.getBody());

        }
    }

    @Test
    public void justBasicAuthWithoutFrontendConfigTest() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder().resources("session").user(BASIC_USER).authc(AUTHC).sslEnabled().singleNode().start()) {
            String token;

            try (GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
                HttpResponse response = restClient.get("/_searchguard/auth/config");
                Assert.assertEquals(response.getBody(), "basic", response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).get("method"));
                Assert.assertEquals(response.getBody(), 1, response.getBodyAsDocNode().getAsListOfNodes("auth_methods").size());
            }

            try (GenericRestClient restClient = cluster.getRestClient()) {
                HttpResponse response = restClient.postJson("/_searchguard/auth/session", basicAuthRequest(BASIC_USER));

                System.out.println(response.getBody());

                Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

                token = response.getBodyAsDocNode().getAsString("token");
                Assert.assertNotNull(response.getBody(), token);
            }

            try (GenericRestClient restClient = cluster.getRestClient(new BearerAuthorization(token))) {
                HttpResponse response = restClient.get("/_searchguard/authinfo");

                Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
                Assert.assertEquals(response.getBody(), BASIC_USER.getName(), response.getBodyAsDocNode().get("user_name"));
            }
        }
    }

    private static Map<String, Object> basicAuthRequest(TestSgConfig.User user, Object... additionalAttrs) {
        Map<String, Object> result = new HashMap<>();

        result.put("mode", "basic");
        result.put("user", user.getName());
        result.put("password", user.getPassword());

        if (additionalAttrs != null && additionalAttrs.length > 0) {
            for (int i = 0; i < additionalAttrs.length; i += 2) {
                result.put(additionalAttrs[i].toString(), additionalAttrs[i + 1]);
            }
        }

        return result;
    }

    private static Map<String, Object> testAuthRequest(String userName, Object... additionalAttrs) {
        Map<String, Object> result = new HashMap<>();

        result.put("mode", "test");
        result.put("user", userName);
        result.put("secret", "indeed");

        if (additionalAttrs != null && additionalAttrs.length > 0) {
            for (int i = 0; i < additionalAttrs.length; i += 2) {
                result.put(additionalAttrs[i].toString(), additionalAttrs[i + 1]);
            }
        }

        return result;
    }

}
