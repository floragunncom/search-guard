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

package com.floragunn.searchguard;

import java.net.InetAddress;
import java.util.Arrays;

import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.google.common.collect.ImmutableMap;

public class BackendRegistryTests {

    private static TestSgConfig.User TEST_USER = new TestSgConfig.User("test_user").roles("SGS_ALL_ACCESS");
    private static TestSgConfig.User BLOCK_TEST_USER = new TestSgConfig.User("block_test_user").roles("SGS_ALL_ACCESS");
    private static TestSgConfig.User BLOCK_WILDCARD_TEST_USER = new TestSgConfig.User("block_wildcard_test_user").roles("SGS_ALL_ACCESS");

    private static TestSgConfig SG_CONFIG = new TestSgConfig()//
            .authc(new TestSgConfig.AuthcDomain("ip_selected_domain", 0).httpAuthenticator("basic").backend("noop").enabledOnlyForIps("127.0.0.4/30")) //
            .authc(new TestSgConfig.AuthcDomain("base_domain", 1).challengingAuthenticator("basic").backend("internal"))//
            .xff("127.0.0.44")//
            .user(TEST_USER)//
            .user(BLOCK_TEST_USER)//
            .user(BLOCK_WILDCARD_TEST_USER);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().sgConfig(SG_CONFIG).build();

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @Test
    public void when_user_is_skipped_then_authentication_should_fail() throws Exception {
        // In the community version, we only have two authc backends: internal and noop. 
        // In order to test skip_users here, we need to utilitze the trick that the noop backend authenticates any user.

        TestSgConfig sgConfig = new TestSgConfig()//
                .authc(new TestSgConfig.AuthcDomain("skipping_domain", 0).httpAuthenticator("basic").backend("noop").skipUsers("skipped_user")) //
                .authc(new TestSgConfig.AuthcDomain("base_domain", 1).challengingAuthenticator("basic").backend("internal"));

        try (LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().sgConfig(sgConfig).build()) {

            try (GenericRestClient restClient = cluster.getRestClient("any_name", "any_password")) {
                // This request is answered by the first authc backend, namely the noop backend. Any non-skipped user is authenticated at this point
                HttpResponse response = restClient.get("_searchguard/authinfo?pretty");
                Assert.assertEquals(response.toString(), HttpStatus.SC_OK, response.getStatusCode());
            }

            try (GenericRestClient restClient = cluster.getRestClient("skipped_user", "any_password")) {
                // The first backend (noop) skips over for this given user=peter, the second backend doesn't know this user, thus leading to a HTTP 401 response
                HttpResponse response = restClient.get("_searchguard/authinfo?pretty");
                Assert.assertEquals(response.toString(), HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
            }
        }
    }

    @Test
    public void when_user_is_blocked_then_authentication_should_fail() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(BLOCK_TEST_USER)) {
            HttpResponse response = restClient.get("_searchguard/authinfo?pretty");

            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            cluster.updateSgConfig(CType.BLOCKS, "block_" + BLOCK_TEST_USER.getName(),
                    ImmutableMap.of("type", "name", "value", Arrays.asList(BLOCK_TEST_USER.getName()), "verdict", "disallow"));

            response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        }
    }

    @Test
    public void when_user_is_blocked_then_authentication_should_fail_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(BLOCK_WILDCARD_TEST_USER)) {
            HttpResponse response = restClient.get("_searchguard/authinfo?pretty");

            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            cluster.updateSgConfig(CType.BLOCKS, "block_" + BLOCK_WILDCARD_TEST_USER.getName(),
                    ImmutableMap.of("type", "name", "value", Arrays.asList("block_wildcard_*"), "verdict", "disallow"));

            response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        }
    }

    @Test
    public void when_ip_is_blocked_then_authentication_should_fail() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(TEST_USER)) {
            restClient.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 99 }));

            HttpResponse response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            cluster.updateSgConfig(CType.BLOCKS, "block_ip",
                    ImmutableMap.of("type", "ip", "value", Arrays.asList("127.0.0.99"), "verdict", "disallow"));

            response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        }
    }

    @Test
    public void when_ip_is_blocked_from_net_then_authentication_should_fail() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(TEST_USER)) {
            restClient.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 90 }));

            HttpResponse response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            cluster.updateSgConfig(CType.BLOCKS, "block_ip",
                    ImmutableMap.of("type", "net_mask", "value", Arrays.asList("127.0.0.88/29"), "verdict", "disallow"));

            response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        }
    }

    @Test
    public void when_xff_ip_is_blocked_from_net_then_authentication_should_fail() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(TEST_USER, new BasicHeader("X-Forwarded-For", "10.11.12.13"))) {
            restClient.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 44 }));

            HttpResponse response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            cluster.updateSgConfig(CType.BLOCKS, "block_ip",
                    ImmutableMap.of("type", "net_mask", "value", Arrays.asList("10.11.12.8/29"), "verdict", "disallow"));

            response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        }
    }

    @Test
    public void testFailureRateLimitingXff() throws Exception {
        TestSgConfig sgConfig = new TestSgConfig()//
                .authc(new TestSgConfig.AuthcDomain("base_domain", 1).challengingAuthenticator("basic").backend("internal"))//
                .xff("127.0.0.1")//
                .authFailureListener(new TestSgConfig.AuthFailureListener("ip_rate_limiting", "ip", 3))//
                .user(TEST_USER);

        try (LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().sgConfig(sgConfig).build()) {

            Header xffHeader = new BasicHeader("X-Forwarded-For", "10.14.15.16");

            try (GenericRestClient authRestClient = cluster.getRestClient(TEST_USER, xffHeader);
                    GenericRestClient unauthRestClient = cluster.getRestClient("any_name", "any_password", xffHeader)) {
                HttpResponse response = authRestClient.get("_searchguard/authinfo?pretty");
                Assert.assertEquals(response.toString(), HttpStatus.SC_OK, response.getStatusCode());

                response = unauthRestClient.get("_searchguard/authinfo?pretty");
                Assert.assertEquals(response.toString(), HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());

                response = authRestClient.get("_searchguard/authinfo?pretty");
                Assert.assertEquals(response.toString(), HttpStatus.SC_OK, response.getStatusCode());

                for (int i = 0; i < 3; i++) {
                    response = unauthRestClient.get("_searchguard/authinfo?pretty");
                    Assert.assertEquals(response.toString(), HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
                }

                response = authRestClient.get("_searchguard/authinfo?pretty");
                Assert.assertEquals(response.toString(), HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
            }
        }
    }

    @Test
    public void testEnabledOnlyForHosts() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("any_name", "any_password")) {
            HttpResponse response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());

            restClient.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 2 }));
            response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());

            restClient.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 5 }));
            response = restClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        }

    }

}
