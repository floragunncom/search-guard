/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authc;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.AdditionalUserInformation;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.UserMapping;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class RestAuthenticationIntegrationTests {

    static TestSgConfig.Role INDEX_PATTERN_WITH_ATTR = new TestSgConfig.Role("sg_index_pattern_with_attr_role")//
            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")//
            .indexPermissions("SGS_CRUD").on("/attr_test_${user.attrs.pattern|toRegexFragment}/");

    static TestSgConfig.User ALL_ACCESS = new TestSgConfig.User("all_access").roles(TestSgConfig.Role.ALL_ACCESS);

    static TestSgConfig.User USER_WITH_ATTRIBUTES = new TestSgConfig.User("user_with_attributes").roles(INDEX_PATTERN_WITH_ATTR)//
            .attr("a", 1).attr("b", 2).attr("c", Arrays.asList(3, 4, 5)).attr("d", "a");

    static TestSgConfig.User USER_WITH_ATTRIBUTES2 = new TestSgConfig.User("user_with_attributes2").roles(INDEX_PATTERN_WITH_ATTR)//
            .attr("a", 1).attr("b", 2).attr("c", Arrays.asList(3, 4, 5)).attr("d", Arrays.asList("a", "b", "c"));

    static TestSgConfig.User SUBJECT_PATTERN_USER_TEST = new TestSgConfig.User("subject_pattern_user").roles(INDEX_PATTERN_WITH_ATTR)//
            .attr("a", 1).attr("b", 2).attr("c", Arrays.asList(3, 4, 5)).attr("d", Arrays.asList("a", "c"));

    // Note: This user is supposed to be NOT picked up by the test.
    static TestSgConfig.User SKIP_TEST_USER = new TestSgConfig.User("skip_test_user").roles("skip_test_user_from_internal_users_db");

    static TestSgConfig.User ADDITIONAL_USER_INFORMATION_USER = new TestSgConfig.User("additional_user_information")
            .roles("additional_user_information_role").attr("additional", ImmutableMap.of("a", 1, "b", 2));

    static TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(//
            new Authc.Domain("basic/internal_users_db")//
                    .skipUsers("skip_test_*")//
                    .skipIps("127.0.0.16/30")//
                    .userMapping(new UserMapping().userNameFrom(
                            DocNode.of("json_path", "credentials.user_name", "pattern", "(all_access)|(user_.*)|(.+)@(?:subject_pattern_domain)"))
                            .attrsFrom("pattern", "user_entry.attributes.d")),
            new Authc.Domain("trusted_origin")//
                    .skipUsers("skip_test_*")//
                    .skipIps("127.0.0.16/30", "127.0.0.14")//
                    .userMapping(new UserMapping()//
                            .userNameFrom("request.headers.x-proxy-user")//
                            .rolesFrom("request.headers.x-proxy-roles")//
                            .rolesFrom(DocNode.of("json_path", "request.headers.x-proxy-roles-comma-separated", "split", ","))), //
            new Authc.Domain("trusted_origin")//
                    .id("trusted_origin_with_additional_user_information")//
                    .skipUsers("skip_test_*")//
                    .acceptIps("127.0.0.14")//
                    .additionalUserInformation(new AdditionalUserInformation("internal_users_db"))//
                    .userMapping(new UserMapping()//
                            .userNameFrom("request.headers.x-proxy-user")//
                            .rolesFrom("request.headers.x-proxy-roles")//
                            .attrsFrom("from_user_entry", "user_entry")), //
            new Authc.Domain("basic")//
                    .acceptUsers("skip_test_*")//
                    .skipUsers("skip_test_skip")//
                    .skipIps("127.0.0.16/30")//
                    .userMapping(new UserMapping().rolesStatic("skip_test_user_role_from_accept_users_auth_domain")), //
            new Authc.Domain("basic")//
                    .acceptUsers("skip_test_*")//
                    .skipUsers("skip_test_skip")//
                    .acceptIps("127.0.0.16/30")//
                    .userMapping(new UserMapping().rolesStatic("skip_test_user_role_from_accept_ips_auth_domain")), //
            new Authc.Domain("anonymous")//
                    .acceptIps("127.0.0.33")//
                    .userMapping(new UserMapping().rolesStatic("anon_role")), //
            new Authc.Domain("anonymous")//
                    .acceptIps("127.0.0.34")//
                    .userMapping(new UserMapping().userNameStatic("nobody").rolesStatic("anon_role")) //
    ).trustedProxies("127.0.0.12/30");

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(ALL_ACCESS, USER_WITH_ATTRIBUTES, USER_WITH_ATTRIBUTES2, SUBJECT_PATTERN_USER_TEST, ADDITIONAL_USER_INFORMATION_USER).authc(AUTHC)
            .embedded().build();

    @BeforeClass
    public static void initTestData() {
        try (Client tc = cluster.getInternalNodeClient()) {

            tc.index(new IndexRequest("attr_test_a").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"a\", \"amount\": 1010}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("attr_test_b").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"b\", \"amount\": 2020}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("attr_test_c").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"c\", \"amount\": 3030}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("attr_test_d").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"d\", \"amount\": 4040}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("attr_test_e").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"e\", \"amount\": 5050}",
                    XContentType.JSON)).actionGet();
        }
    }

    @Test
    public void userAttribute_indexPattern_integration() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(ALL_ACCESS)) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(5, searchResponse.getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(USER_WITH_ATTRIBUTES)) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(USER_WITH_ATTRIBUTES2)) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(3, searchResponse.getHits().getTotalHits().value);
        }
    }

    @Test
    public void username_pattern_integration() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("subject_pattern_user@subject_pattern_domain",
                SUBJECT_PATTERN_USER_TEST.getPassword())) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "subject_pattern_user", response.getBodyAsDocNode().get("user_name"));
        }
    }

    @Test
    public void trustedOrigin_roles_integration() throws Exception {

        try (GenericRestClient client = cluster.getRestClient()) {
            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 }));

            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo", new BasicHeader("x-proxy-user", "proxy_test_user"),
                    new BasicHeader("x-proxy-roles", "proxy_role1,proxy_role2"));
            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());

            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 13 }));

            response = client.get("/_searchguard/authinfo", new BasicHeader("x-proxy-user", "proxy_test_user"),
                    new BasicHeader("x-proxy-roles", "proxy_role1,proxy_role2"));
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "proxy_test_user", response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), Arrays.asList("proxy_role1,proxy_role2"), response.getBodyAsDocNode().get("backend_roles"));

            response = client.get("/_searchguard/authinfo", new BasicHeader("x-proxy-user", "proxy_test_user"),
                    new BasicHeader("x-proxy-roles", "proxy_role1"), new BasicHeader("x-proxy-roles", "proxy_role2"));
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), Arrays.asList("proxy_role1", "proxy_role2"), response.getBodyAsDocNode().get("backend_roles"));

            response = client.get("/_searchguard/authinfo", new BasicHeader("x-proxy-user", "proxy_test_user"),
                    new BasicHeader("x-proxy-roles-comma-separated", "proxy_role1,proxy_role2"), new BasicHeader("x-proxy-roles", "proxy_role3"));
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), ImmutableSet.of("proxy_role1", "proxy_role2", "proxy_role3"),
                    ImmutableSet.of((Collection<?>) response.getBodyAsDocNode().get("backend_roles")));
        }
    }

    @Test
    public void trustedOrigin_additionalUserInformation_integration() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(new BasicHeader("x-proxy-user", ADDITIONAL_USER_INFORMATION_USER.getName()),
                new BasicHeader("x-proxy-roles", "proxy_role1"), new BasicHeader("x-proxy-roles", "proxy_role2"))) {
            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 }));

            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());

            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 14 }));

            response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), ADDITIONAL_USER_INFORMATION_USER.getName(), response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), Arrays.asList("proxy_role1", "proxy_role2"), response.getBodyAsDocNode().get("backend_roles"));
            Assert.assertEquals(response.getBody(), Arrays.asList("additional_user_information_role"), response.getBodyAsDocNode().get("sg_roles"));
            Assert.assertEquals(response.getBody(), Arrays.asList("from_user_entry"), response.getBodyAsDocNode().get("attribute_names"));
        }
    }

    @Test
    public void skipUser_integration() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(SKIP_TEST_USER)) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "skip_test_user", response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), Arrays.asList("skip_test_user_role_from_accept_users_auth_domain"),
                    response.getBodyAsDocNode().get("backend_roles"));
        }

        try (GenericRestClient client = cluster.getRestClient("skip_test_skip", "password")) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());
        }
    }

    @Test
    public void skipIp_integration() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(SKIP_TEST_USER)) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "skip_test_user", response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), Arrays.asList("skip_test_user_role_from_accept_users_auth_domain"),
                    response.getBodyAsDocNode().get("backend_roles"));

            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 17 }));

            response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "skip_test_user", response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), Arrays.asList("skip_test_user_role_from_accept_ips_auth_domain"),
                    response.getBodyAsDocNode().get("backend_roles"));
        }
    }

    @Test
    public void anonymousAuth() throws Exception {
        try (GenericRestClient client = cluster.getRestClient()) {
            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 }));

            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());

            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 33 }));

            response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "anonymous", response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), Arrays.asList("anon_role"), response.getBodyAsDocNode().get("backend_roles"));

            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 34 }));

            response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "nobody", response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), Arrays.asList("anon_role"), response.getBodyAsDocNode().get("backend_roles"));
        }
    }

    @Test
    public void challenge() throws Exception {
        try (GenericRestClient client = cluster.getRestClient()) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());
            Assert.assertEquals(response.getHeaders().toString(), "Basic realm=\"Search Guard\"", response.getHeaderValue("WWW-Authenticate"));
        }
    }

    @Test
    public void jsonResponse() throws Exception {
        try (GenericRestClient client = cluster.getRestClient()) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo", new BasicHeader("Accept", "application/json"));
            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());
            Assert.assertEquals(response.getHeaders().toString(), "application/json", response.getHeaderValue("Content-Type"));
            Assert.assertEquals(response.getBody(), "Unauthorized", response.getBodyAsDocNode().get("error", "reason"));
            Assert.assertEquals(response.getBody(), 401, response.getBodyAsDocNode().get("status"));
        }
    }

    @Test
    public void jsonResponseEsClientParsing() throws Exception {
        try (RestClient lowLevelRestClient = cluster.getLowLevelRestClient()) {
            ElasticsearchClient client = new ElasticsearchClient(new RestClientTransport(lowLevelRestClient, new JacksonJsonpMapper()));

            try {
                client.cat().indices();
                Assert.fail();
            } catch (co.elastic.clients.elasticsearch._types.ElasticsearchException e) {
                Assert.assertEquals(e.toString(), 401, e.status());
            }
        }
    }

    @Test
    public void authDomainInfo() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(ALL_ACCESS)) {
            HttpResponse response = restClient.get("/_searchguard/authinfo");
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsString("user").startsWith("User all_access <basic/internal_users_db>"));
        }
    }

}
