/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;
import com.floragunn.searchsupport.json.BasicJsonReader;
import com.google.common.io.BaseEncoding;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

public class AuthTokenIntegrationTest {
    private static String SGCONFIG = //
            "_sg_meta:\n" + //
                    "  type: \"config\"\n" + //
                    "  config_version: 2\n" + //
                    "\n" + //
                    "sg_config:\n" + //
                    "  dynamic:\n" + //
                    "    auth_token_provider: \n" + //
                    "      enabled: true\n" + //
                    "      jwt_signing_key_hs512: \"" + TestJwk.OCT_1_K + "\"\n" + //
                    "      jwt_aud: \"searchguard_tokenauth\"\n" + //
                    "      max_validity: \"1y\"\n" + //
                    "      max_tokens_per_user: 10\n" + //
                    "    authc:\n" + //
                    "      authentication_domain_basic_internal:\n" + //
                    "        http_enabled: true\n" + //
                    "        transport_enabled: true\n" + //
                    "        order: 1\n" + //
                    "        http_authenticator:\n" + //
                    "          challenge: true\n" + //
                    "          type: \"basic\"\n" + //
                    "          config: {}\n" + //
                    "        authentication_backend:\n" + //
                    "          type: \"intern\"\n" + //
                    "          config:\n" + //
                    "            map_db_attrs_to_user_attrs:\n" + //
                    "              index: test_attr_1.c\n" + //
                    "              all: test_attr_1\n" + //
                    "      sg_issued_jwt_auth_domain:\n" + //
                    "        description: \"Authenticate via Json Web Tokens issued by Search Guard\"\n" + //
                    "        http_enabled: true\n" + //
                    "        transport_enabled: false\n" + //
                    "        order: 0\n" + //
                    "        http_authenticator:\n" + //
                    "          type: sg_auth_token\n" + //
                    "          challenge: false\n" + //
                    "        authentication_backend:\n" + //
                    "          type: sg_auth_token";

    static TestSgConfig sgConfig = new TestSgConfig().resources("authtoken").sgConfigSettings("", TestSgConfig.fromYaml(SGCONFIG));
    private static Configuration JSON_PATH_CONFIG = Configuration.defaultConfiguration().setOptions(Option.SUPPRESS_EXCEPTIONS);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin")
            .resources("authtoken").sslEnabled().sgConfig(sgConfig).build();

    private static RestHelper rh = null;

    @BeforeClass
    public static void setupDependencies() {
        rh = cluster.restHelper();
    }

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest("pub_test_deny").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "this_is",
                    "not_allowed_from_token")).actionGet();
            client.index(new IndexRequest("pub_test_allow_because_from_token").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "this_is", "allowed")).actionGet();
            client.index(new IndexRequest("user_attr_foo").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "this_is", "allowed"))
                    .actionGet();
            client.index(
                    new IndexRequest("user_attr_qux").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "this_is", "not_allowed"))
                    .actionGet();
            client.index(new IndexRequest("dls_user_attr").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "this_is", "allowed",
                    "a", "foo")).actionGet();
            client.index(new IndexRequest("dls_user_attr").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "this_is",
                    "not_allowed", "a", "qux")).actionGet();

        }

    }

    @Test
    public void basicTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

        request.setTokenName("my_new_token");

        Header auth = basicAuth("spock", "spock");

        System.out.println(request.toJson());

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        System.out.println(response.getBody());

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();
        Assert.assertNotNull(token);
        Assert.assertEquals("HS512", getJwtHeaderValue(token, "alg"));

        String tokenPayload = getJwtPayload(token);
        Map<String, Object> parsedTokenPayload = BasicJsonReader.readObject(tokenPayload);
        Assert.assertEquals(tokenPayload, "spock", JsonPath.parse(parsedTokenPayload).read("sub"));
        Assert.assertTrue(tokenPayload, JsonPath.using(JSON_PATH_CONFIG).parse(parsedTokenPayload).read("base.c") != null);

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("spock", "spock")) {
            SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                    .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

            searchResponse = client.search(
                    new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals("not_allowed_from_token", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));
        }

        for (int i = 0; i < 3; i++) {
            try (RestHighLevelClient client = cluster.getRestHighLevelClientForNode(i, new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

                try {

                    searchResponse = client.search(
                            new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                            RequestOptions.DEFAULT);
                    Assert.fail(searchResponse.toString());
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/read/search]"));
                }
            }
        }

    }

    @Test
    public void basicTestUnfrozenPrivileges() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));
        request.setFreezePrivileges(false);

        request.setTokenName("my_new_token_unfrozen_privileges");

        Header auth = basicAuth("spock", "spock");

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();

        Assert.assertNotNull(token);
        Assert.assertEquals("HS512", getJwtHeaderValue(token, "alg"));

        String tokenPayload = getJwtPayload(token);
        Map<String, Object> parsedTokenPayload = BasicJsonReader.readObject(tokenPayload);
        Assert.assertEquals(tokenPayload, "spock", JsonPath.parse(parsedTokenPayload).read("sub"));
        Assert.assertTrue(tokenPayload, JsonPath.using(JSON_PATH_CONFIG).parse(parsedTokenPayload).read("base.c") == null);

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("spock", "spock")) {
            SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                    .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

            searchResponse = client.search(
                    new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals("not_allowed_from_token", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));
        }

        for (int i = 0; i < 3; i++) {
            try (RestHighLevelClient client = cluster.getRestHighLevelClientForNode(i, new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

                try {

                    searchResponse = client.search(
                            new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                            RequestOptions.DEFAULT);
                    Assert.fail(searchResponse.toString());
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/read/search]"));
                }
            }
        }

    }

    @Test
    public void maxTokenCountTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

        request.setTokenName("my_new_token");

        Header auth = basicAuth("nagilum", "nagilum");

        for (int i = 0; i < 10; i++) {

            HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

            Assert.assertEquals(200, response.getStatusCode());
        }

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        System.out.println(response.getBody());

        Assert.assertEquals(403, response.getStatusCode());
        Assert.assertEquals("Cannot create token. Token limit per user exceeded. Max number of allowed tokens is 10",
                response.toJsonNode().at("/error/root_cause/0/reason").textValue());
    }

    @Test
    public void createTokenWithTokenForbidden() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("cluster_permissions: '*'\nindex_permissions:\n- index_patterns: '*'\n  allowed_actions: '*'"));

        request.setTokenName("my_new_token_with_with_i_am_trying_to_create_another_token");

        Header auth = basicAuth("spock", "spock");

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        System.out.println(response.getBody());

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();
        Assert.assertNotNull(token);

        Header tokenAuth = new BasicHeader("Authorization", "Bearer " + token);

        request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("cluster_permissions: '*'\nindex_permissions:\n- index_patterns: '*'\n  allowed_actions: '*'"));

        request.setTokenName("this_token_should_not_be_created");

        response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), tokenAuth);
        Assert.assertEquals(response.getBody(), 403, response.getStatusCode());
        Assert.assertTrue(response.getBody(), response.getBody().contains("no permissions for [cluster:admin:searchguard:authtoken/_own/create]"));
    }

    @Test
    public void userAttrTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: 'user_attr_*'\n  allowed_actions: '*'"));

        request.setTokenName("my_new_token");

        Header auth = basicAuth("picard", "picard");

        System.out.println(request.toJson());

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        System.out.println(response.getBody());

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();
        Assert.assertNotNull(token);

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("picard", "picard")) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("user_attr_foo").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

            try {
                searchResponse = client.search(
                        new SearchRequest("user_attr_qux").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/read/search]"));
            }
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("user_attr_foo").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

            try {
                searchResponse = client.search(
                        new SearchRequest("user_attr_qux").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/read/search]"));
            }
        }

    }

    @Test
    public void userAttrTestDls() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*'\n  allowed_actions: '*'"));

        request.setTokenName("my_new_token");

        Header auth = basicAuth("picard", "picard");

        System.out.println(request.toJson());

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        System.out.println(response.getBody());

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();
        Assert.assertNotNull(token);

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("dls_user_attr").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(2, searchResponse.getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("picard", "picard")) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("dls_user_attr").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("dls_user_attr").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));
        }

    }

    @Test
    public void revocationTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

        request.setTokenName("my_new_token");

        Header auth = basicAuth("spock", "spock");

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();
        String id = response.toJsonNode().get("id").asText();

        Assert.assertNotNull(token);
        Assert.assertNotNull(id);

        for (int i = 0; i < 3; i++) {
            try (RestHighLevelClient client = cluster.getRestHighLevelClientForNode(i, new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

                try {

                    searchResponse = client.search(
                            new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                            RequestOptions.DEFAULT);
                    Assert.fail(searchResponse.toString());
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/read/search]"));
                }
            }
        }

        response = rh.executeDeleteRequest("/_searchguard/authtoken/" + id, auth);
        Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        Thread.sleep(100);

        for (int i = 0; i < 3; i++) {

            try (RestHighLevelClient client = cluster.getRestHighLevelClientForNode(i, new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);
                Assert.fail(searchResponse.toString());

            } catch (ElasticsearchStatusException e) {
                Assert.assertEquals(e.getMessage(), RestStatus.UNAUTHORIZED, e.status());
            }

        }
    }

    @Test
    public void revocationWithoutSpecialPrivsTest() throws Exception {
        TestSgConfig sgConfig = AuthTokenIntegrationTest.sgConfig.clone()
                .sgConfigSettings("sg_config.dynamic.auth_token_provider.exclude_cluster_permissions", Collections.emptyList());

        try (LocalCluster cluster = new LocalCluster.Builder().nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin").resources("authtoken")
                .sslEnabled().sgConfig(sgConfig).build(); GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {

            try (Client client = cluster.getInternalClient()) {
                client.index(new IndexRequest("pub_test_allow_because_from_token").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                        "this_is", "allowed")).actionGet();
            }

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(RequestedPrivileges.totalWildcard());
            request.setTokenName("my_new_token_without_special_privs");
            request.setFreezePrivileges(false);

            GenericRestClient.HttpResponse response = restClient.postJson("/_searchguard/authtoken", request.toJson());

            Assert.assertEquals(200, response.getStatusCode());

            String token = response.toJsonNode().get("token").asText();
            String id = response.toJsonNode().get("id").asText();

            Assert.assertNotNull(token);
            Assert.assertNotNull(id);

            for (int i = 0; i < 3; i++) {

                try (RestHighLevelClient client = cluster.getRestHighLevelClientForNode(i, new BasicHeader("Authorization", "Bearer " + token))) {
                    SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                            .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                    Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                    Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

                }

            }

            response = restClient.delete("/_searchguard/authtoken/" + id);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Thread.sleep(100);

            for (int i = 0; i < 3; i++) {

                try (RestHighLevelClient client = cluster.getRestHighLevelClientForNode(i, new BasicHeader("Authorization", "Bearer " + token))) {

                    try {
                        SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                                .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                        Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                        Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));
                    } catch (ElasticsearchStatusException e) {
                        Assert.assertEquals(e.getMessage(), RestStatus.UNAUTHORIZED, e.status());
                    }
                }

            }
        }
    }

    @Test
    public void getAndSearchTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

        request.setTokenName("get_and_search_test_token");

        Header auth = basicAuth("spock", "spock");

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        Assert.assertEquals(200, response.getStatusCode());

        request.setTokenName("get_and_search_test_token_2");

        response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        Assert.assertEquals(200, response.getStatusCode());

        request.setTokenName("get_and_search_test_token_picard");

        response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), basicAuth("picard", "picard"));

        Assert.assertEquals(200, response.getStatusCode());

        String picardsTokenId = response.toJsonNode().get("id").textValue();

        response = rh.executeGetRequest("/_searchguard/authtoken/_search", auth);

        Assert.assertEquals(200, response.getStatusCode());
        Assert.assertFalse(response.getBody(), response.getBody().contains("\"picard\""));

        String searchRequest = "{\n" + //
                "    \"query\": {\n" + //
                "        \"wildcard\": {\n" + //
                "            \"token_name\": {\n" + //
                "                \"value\": \"get_and_search_test_*\"\n" + //
                "            }\n" + //
                "        }\n" + //
                "    }\n" + //
                "}";

        response = rh.executePostRequest("/_searchguard/authtoken/_search", searchRequest, auth);

        System.out.println(response.getBody());

        Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

        JsonNode jsonNode = response.toJsonNode();

        Assert.assertEquals(response.getBody(), 2, jsonNode.at("/hits/total/value").intValue());
        Assert.assertEquals(response.getBody(), "spock", jsonNode.at("/hits/hits/0/_source/user_name").textValue());
        Assert.assertEquals(response.getBody(), "spock", jsonNode.at("/hits/hits/1/_source/user_name").textValue());

        String id = jsonNode.at("/hits/hits/0/_id").textValue();
        String tokenName = jsonNode.at("/hits/hits/0/_source/token_name").textValue();

        response = rh.executeGetRequest("/_searchguard/authtoken/" + id, auth);

        Assert.assertEquals(response.getBody(), tokenName, response.toJsonNode().get("token_name").textValue());

        response = rh.executeGetRequest("/_searchguard/authtoken/" + picardsTokenId, auth);

        Assert.assertEquals(404, response.getStatusCode());

        response = rh.executePostRequest("/_searchguard/authtoken/_search", searchRequest, basicAuth("admin", "admin"));

        jsonNode = response.toJsonNode();

        Assert.assertEquals(response.getBody(), 3, jsonNode.at("/hits/total/value").intValue());
        Assert.assertTrue(response.getBody(), response.getBody().contains("\"spock\""));
        Assert.assertTrue(response.getBody(), response.getBody().contains("\"picard\""));
    }

    @Test
    public void encryptedAuthTokenTest() throws Exception {
        String sgConfigWithEncryption = //
                "_sg_meta:\n" + //
                        "  type: \"config\"\n" + //
                        "  config_version: 2\n" + //
                        "\n" + //
                        "sg_config:\n" + //
                        "  dynamic:\n" + //
                        "    auth_token_provider: \n" + //
                        "      enabled: true\n" + //
                        "      jwt_signing_key_hs512: \"" + TestJwk.OCT_512_1_K + "\"\n" + //
                        "      jwt_encryption_key_a256kw: \"" + TestJwk.OCT_256_1_K + "\"\n" + //
                        "      jwt_aud: \"searchguard_tokenauth\"\n" + //
                        "      max_validity: \"1y\"\n" + //
                        "    authc:\n" + //
                        "      authentication_domain_basic_internal:\n" + //
                        "        http_enabled: true\n" + //
                        "        transport_enabled: true\n" + //
                        "        order: 1\n" + //
                        "        http_authenticator:\n" + //
                        "          challenge: true\n" + //
                        "          type: \"basic\"\n" + //
                        "          config: {}\n" + //
                        "        authentication_backend:\n" + //
                        "          type: \"intern\"\n" + //
                        "          config:\n" + //
                        "            map_db_attrs_to_user_attrs:\n" + //
                        "              index: test_attr_1.c\n" + //
                        "              all: test_attr_1\n" + //
                        "      sg_issued_jwt_auth_domain:\n" + //
                        "        description: \"Authenticate via Json Web Tokens issued by Search Guard\"\n" + //
                        "        http_enabled: true\n" + //
                        "        transport_enabled: false\n" + //
                        "        order: 0\n" + //
                        "        http_authenticator:\n" + //
                        "          type: sg_auth_token\n" + //
                        "          challenge: false\n" + //
                        "        authentication_backend:\n" + //
                        "          type: sg_auth_token";

        TestSgConfig sgConfig = new TestSgConfig().resources("authtoken").sgConfigSettings("", TestSgConfig.fromYaml(sgConfigWithEncryption));

        try (LocalCluster cluster = new LocalCluster.Builder().resources("authtoken").sslEnabled().singleNode().sgConfig(sgConfig).build()) {
            RestHelper rh = cluster.restHelper();

            try (Client client = cluster.getInternalClient()) {
                client.index(new IndexRequest("pub_test_deny").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "this_is",
                        "not_allowed_from_token")).actionGet();
                client.index(new IndexRequest("pub_test_allow_because_from_token").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                        "this_is", "allowed")).actionGet();
            }

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            Header auth = basicAuth("spock", "spock");

            System.out.println(request.toJson());

            HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

            System.out.println(response.getBody());

            Assert.assertEquals(200, response.getStatusCode());

            String token = response.toJsonNode().get("token").asText();
            Assert.assertNotNull(token);
            Assert.assertEquals("A256KW", getJwtHeaderValue(token, "alg"));
            Assert.assertEquals("A256CBC-HS512", getJwtHeaderValue(token, "enc"));
            Assert.assertFalse("JWT payload seems to be unencrypted because it contains the user name in clear text: " + getJwtPayload(token),
                    getJwtPayload(token).contains("spock"));

            try (RestHighLevelClient client = cluster.getRestHighLevelClient("spock", "spock")) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

                searchResponse = client.search(
                        new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                Assert.assertEquals("not_allowed_from_token", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));
            }

            try (RestHighLevelClient client = cluster.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

                try {

                    searchResponse = client.search(
                            new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                            RequestOptions.DEFAULT);
                    Assert.fail(searchResponse.toString());
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/read/search]"));
                }
            }

        }
    }

    @Test
    public void ecSignedAuthTokenTest() throws Exception {
        String sgConfigWithEncryption = //
                "_sg_meta:\n" + //
                        "  type: \"config\"\n" + //
                        "  config_version: 2\n" + //
                        "\n" + //
                        "sg_config:\n" + //
                        "  dynamic:\n" + //
                        "    auth_token_provider: \n" + //
                        "      enabled: true\n" + //
                        "      jwt_signing_key: \n" + //
                        "        kty: EC\n" + // 
                        "        d: \"1nlQeqOq48OPWiDkmOIXLF_XBWUe9LSznBvWzPI4Ggo\"\n" + //
                        "        use: sig\n" + "        crv: P-256\n" + //
                        "        x: \"lBybOJZyK6r8Nx54Jn4cKoDUZgyOdLlsQ2EHk-7LStk\"\n" + //
                        "        y: \"BwSiCmlnS1CDetg_iuxBZKkh6VTMrra0aIT9dBeoCZU\"\n" + //
                        "        alg: ES256\n" + //
                        "      jwt_aud: \"searchguard_tokenauth\"\n" + //
                        "      max_validity: \"1y\"\n" + //
                        "    authc:\n" + //
                        "      authentication_domain_basic_internal:\n" + //
                        "        http_enabled: true\n" + //
                        "        transport_enabled: true\n" + //
                        "        order: 1\n" + //
                        "        http_authenticator:\n" + //
                        "          challenge: true\n" + //
                        "          type: \"basic\"\n" + //
                        "          config: {}\n" + //
                        "        authentication_backend:\n" + //
                        "          type: \"intern\"\n" + //
                        "          config:\n" + //
                        "            map_db_attrs_to_user_attrs:\n" + //
                        "              index: test_attr_1.c\n" + //
                        "              all: test_attr_1\n" + //
                        "      sg_issued_jwt_auth_domain:\n" + //
                        "        description: \"Authenticate via Json Web Tokens issued by Search Guard\"\n" + //
                        "        http_enabled: true\n" + //
                        "        transport_enabled: false\n" + //
                        "        order: 0\n" + //
                        "        http_authenticator:\n" + //
                        "          type: sg_auth_token\n" + //
                        "          challenge: false\n" + //
                        "        authentication_backend:\n" + //
                        "          type: sg_auth_token";

        TestSgConfig sgConfig = new TestSgConfig().resources("authtoken").sgConfigSettings("", TestSgConfig.fromYaml(sgConfigWithEncryption));

        try (LocalCluster cluster = new LocalCluster.Builder().resources("authtoken").sslEnabled().singleNode().sgConfig(sgConfig).build()) {
            RestHelper rh = cluster.restHelper();

            try (Client client = cluster.getInternalClient()) {
                client.index(new IndexRequest("pub_test_deny").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "this_is",
                        "not_allowed_from_token")).actionGet();
                client.index(new IndexRequest("pub_test_allow_because_from_token").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                        "this_is", "allowed")).actionGet();
            }

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            Header auth = basicAuth("spock", "spock");

            System.out.println(request.toJson());

            HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

            Assert.assertEquals(200, response.getStatusCode());

            String token = response.toJsonNode().get("token").asText();
            Assert.assertNotNull(token);
            Assert.assertEquals("ES256", getJwtHeaderValue(token, "alg"));
            Assert.assertTrue(getJwtPayload(token), getJwtPayload(token).contains("spock"));

            try (RestHighLevelClient client = cluster.getRestHighLevelClient("spock", "spock")) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

                searchResponse = client.search(
                        new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                Assert.assertEquals("not_allowed_from_token", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));
            }

            try (RestHighLevelClient client = cluster.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
                Assert.assertEquals("allowed", searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"));

                try {

                    searchResponse = client.search(
                            new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                            RequestOptions.DEFAULT);
                    Assert.fail(searchResponse.toString());
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/read/search]"));
                }
            }

        }
    }

    @Test
    public void sgAdminRestApiTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(RequestedPrivileges.parseYaml("cluster_permissions: ['*']"));

        request.setTokenName("rest_api_test_token");

        Header auth = basicAuth("admin", "admin");

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();
        Assert.assertNotNull(token);

        Header tokenAuth = new BasicHeader("Authorization", "Bearer " + token);

        response = rh.executeGetRequest("_searchguard/api/roles", auth);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        response = rh.executeGetRequest("_searchguard/api/roles", tokenAuth);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

    }

    @Test
    public void sgAdminRestApiForbiddenTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

        request.setTokenName("rest_api_test_token");

        Header auth = basicAuth("admin", "admin");

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();
        Assert.assertNotNull(token);

        Header tokenAuth = new BasicHeader("Authorization", "Bearer " + token);

        response = rh.executeGetRequest("_searchguard/api/roles", auth);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        response = rh.executeGetRequest("_searchguard/api/roles", tokenAuth);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

    }

    @Test
    public void sgAdminRestApiExclusionTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(RequestedPrivileges
                .parseYaml("cluster_permissions: ['*']\nexclude_cluster_permissions: ['cluster:admin:searchguard:configrestapi']"));

        request.setTokenName("rest_api_test_token");

        Header auth = basicAuth("admin", "admin");

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();
        Assert.assertNotNull(token);

        Header tokenAuth = new BasicHeader("Authorization", "Bearer " + token);

        response = rh.executeGetRequest("_searchguard/api/roles", auth);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        response = rh.executeGetRequest("_searchguard/api/roles", tokenAuth);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

    }

    @Test
    public void infoApiTest() throws Exception {
        HttpResponse response = rh.executeGetRequest("/_searchguard/authtoken/_info", basicAuth("admin", "admin"));

        Assert.assertEquals(200, response.getStatusCode());
        Assert.assertTrue(response.getBody(), response.toJsonNode().get("enabled").asBoolean());
    }

    private static String getJwtHeaderValue(String jwt, String headerName) throws IOException {
        int p = jwt.indexOf('.');
        String headerBase4 = jwt.substring(0, p);
        JsonNode jsonNode = DefaultObjectMapper.readTree(new String(BaseEncoding.base64Url().decode(headerBase4)));
        return jsonNode.get(headerName).textValue();
    }

    private static String getJwtPayload(String jwt) {
        int p = jwt.indexOf('.');
        int p2 = jwt.indexOf('.', p + 1);
        String headerBase4 = jwt.substring(p + 1, p2 != -1 ? p2 : jwt.length());
        return new String(BaseEncoding.base64Url().decode(headerBase4));
    }

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }
}
