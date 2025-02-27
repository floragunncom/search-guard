/*
 * Copyright 2015-2025 floragunn GmbH
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

package com.floragunn.searchguard.int_tests;

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static com.floragunn.searchguard.test.RestMatchers.singleNodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.tasks.Task;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.junit.AsyncAssert;

@RunWith(Suite.class)
@Suite.SuiteClasses({ FoundationalTests.StandardConfig.class, FoundationalTests.SpecialConfig.class })
public class FoundationalTests {
    static TestSgConfig.User ALL_ACCESS_USER = new TestSgConfig.User("all_access").roles("SGS_ALL_ACCESS");

    /**
     * Tests using a standard configuration. Thus, these tests can share one initialized cluster.
     */
    public static class StandardConfig {

        @ClassRule
        public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
                .authc(new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"))).users(ALL_ACCESS_USER).build();

        /**
         * Moved from https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/blob/18365beff2276cf5f9182931a7c53e4bf03a3c4a/security-legacy/src/test/java/com/floragunn/searchguard/legacy/TaskTests.java
         */
        @Test
        public void testXOpaqueIdHeader() throws Exception {
            try (GenericRestClient restClient = cluster.getRestClient(ALL_ACCESS_USER)) {
                GenericRestClient.HttpResponse response = restClient.get("_tasks?group_by=parents&pretty",
                        new BasicHeader(Task.X_OPAQUE_ID_HTTP_HEADER, "myOpaqueId12"));

                assertThat(response, isOk());
                assertTrue(response.getBody().split("X-Opaque-Id").length > 2);
            }
        }

        /**
         * Moved from https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/blob/18365beff2276cf5f9182931a7c53e4bf03a3c4a/security-legacy/src/test/java/com/floragunn/searchguard/legacy/TaskTests.java
         */
        @Test
        public void testXOpaqueIdHeaderLowerCase() throws Exception {
            try (GenericRestClient restClient = cluster.getRestClient(ALL_ACCESS_USER)) {
                GenericRestClient.HttpResponse response = restClient.get("_tasks?group_by=parents&pretty",
                        new BasicHeader(Task.X_OPAQUE_ID_HTTP_HEADER.toLowerCase(), "myOpaqueId12"));

                assertThat(response, isOk());
                assertTrue(response.getBody().split("X-Opaque-Id").length > 2);

            }
        }

        /**
         * Moved from https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/blob/18365beff2276cf5f9182931a7c53e4bf03a3c4a/security-legacy/src/test/java/com/floragunn/searchguard/legacy/HealthTests.java
         */
        @Test
        public void testHealth() throws Exception {
            try (GenericRestClient restClient = cluster.getRestClient()) {
                GenericRestClient.HttpResponse response = restClient.get("_searchguard/health?pretty&mode=lenient");

                assertThat(response, isOk());
                assertTrue(response.getBody().contains("UP"));
                assertFalse(response.getBody().contains("DOWN"));
            }
        }

        
        /**
         * Moved from https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/blob/18365beff2276cf5f9182931a7c53e4bf03a3c4a/security-legacy/src/test/java/com/floragunn/searchguard/legacy/IntegrationTests.java
         */
        @Test
        public void testSgIndexSecurity() throws Exception {
            try (GenericRestClient restClient = cluster.getRestClient(ALL_ACCESS_USER)) {
                GenericRestClient.HttpResponse response = restClient.putJson(".searchguard/_mapping?pretty", "{\"properties\": {\"name\":{\"type\":\"text\"}}}");
                assertThat(response, isForbidden());
                
                response = restClient.putJson("*earc*gua*/_mapping?expand_wildcards=all", "{\"properties\": {\"name\":{\"type\":\"text\"}}}");
                assertThat(response, isForbidden());
                
                response = restClient.post(".searchguard/_close");
                assertThat(response, isForbidden());
                
                response = restClient.delete(".searchguard");
                assertThat(response, isForbidden());
                
                response = restClient.putJson(".searchguard/_settings", "{\"index\" : {\"number_of_replicas\" : 2}}");
                assertThat(response, isForbidden());   
            }
        }
    }

    /**
     * Tests using a special configuration each. Thus, these tests need each a cluster on their own.
     */
    public static class SpecialConfig {

        /**
         * Moved from https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/blob/18365beff2276cf5f9182931a7c53e4bf03a3c4a/dlic-fe-multi-tenancy/src/test/java/com/floragunn/searchguard/enterprise/femt/HttpIntegrationTests.java
         */
        @Test
        public void testHTTPSCompressionEnabled() throws Exception {
            try (LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
                    .authc(new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"))).users(ALL_ACCESS_USER)
                    .nodeSettings("http.compression", true).start()) {

                try (GenericRestClient restClient = cluster.getRestClient(ALL_ACCESS_USER)) {
                    GenericRestClient.HttpResponse response = restClient.get("_searchguard/sslinfo");

                    assertThat(response, isOk());
                    assertThat(response, json(nodeAt("ssl_protocol", is("TLSv1.2"))));

                    response = restClient.get("_nodes");
                    assertThat(response, json(singleNodeAt("nodes[*].settings.http.compression", is("true"))));
                }
            }
        }

        /**
         * Moved from https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/blob/18365beff2276cf5f9182931a7c53e4bf03a3c4a/security-legacy/src/test/java/com/floragunn/searchguard/legacy/InitializationIntegrationTests.java
         */
        @Test
        public void testDefaultConfig() throws Exception {
            try (LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().doNotInitializeSgConfig()
                    .nodeSettings("searchguard.allow_default_init_sgindex", true).start()) {

                try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {
                    AsyncAssert.awaitAssert("Index was initialized", () -> restClient.get("").getStatusCode() == 200, Duration.ofSeconds(60));
                }
            }
        }

        /**
         * Moved from https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/blob/18365beff2276cf5f9182931a7c53e4bf03a3c4a/security-legacy/src/test/java/com/floragunn/searchguard/legacy/InitializationIntegrationTests.java
         */
        @Test
        public void testDisabled() throws Exception {
            try (LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().doNotInitializeSgConfig()
                    .nodeSettings("searchguard.disabled", true).start()) {

                try (GenericRestClient restClient = cluster.getRestClientWithoutTls()) {
                    GenericRestClient.HttpResponse response = restClient.get("_search");

                    assertThat(response, isOk());
                }
            }
        }

    }

}
