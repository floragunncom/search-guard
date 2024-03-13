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

import java.util.Collections;
import java.util.Map;

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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.JvmEmbeddedEsCluster;
import com.google.common.io.BaseEncoding;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static com.floragunn.searchsupport.junit.matcher.ExceptionsMatchers.messageContainsMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
                    "      token_cache:\n" + //
                    "        expire_after_write: 70m\n" + //
                    "        max_size: 100\n" + //
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
    private static Configuration JSON_PATH_CONFIG = BasicJsonPathDefaultConfiguration.defaultConfiguration().setOptions(Option.SUPPRESS_EXCEPTIONS);

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin")
            .resources("authtoken").sslEnabled().sgConfig(sgConfig).enterpriseModulesEnabled().enableModule(AuthTokenModule.class).embedded().build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalNodeClient()) {
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
    public void tokenWithDefaultSigningKeyTest() throws Exception {
        final String tokenWithConfiguredSigningKey;
        final String tokenWithDefaultSigningKey;
        try {
            try (GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {
                CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                        RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

                request.setTokenName("token_with_configured_signing_key");

                HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                tokenWithConfiguredSigningKey = response.getBodyAsDocNode().getAsString("token");
                assertThat(tokenWithConfiguredSigningKey, notNullValue());
            }

            //remove configured signing key
            DocNode config = DocNode.of("enabled", true);
            DocNode updatedConfig = updateAuthTokenServiceConfig(config);
            assertThat(updatedConfig.get("jwt_signing_key"), nullValue());
            assertThat(updatedConfig.get("jwt_signing_key_hs512"), nullValue());

            try (GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {
                CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                        RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

                request.setTokenName("token_with_default_signing_key");

                HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

                System.out.println(response.getBody());

                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                tokenWithDefaultSigningKey = response.getBodyAsDocNode().getAsString("token");

                assertThat(tokenWithDefaultSigningKey, notNullValue());
                assertThat(getJwtHeaderValue(tokenWithDefaultSigningKey, "alg"), equalTo("HS512"));
            }

            try (GenericRestClient client = cluster.getRestClient(new BasicHeader("Authorization", "Bearer " + tokenWithConfiguredSigningKey))) {
                HttpResponse response = client.postJson("/pub_test_allow_because_from_token/_search", "{\"query\":{\"match_all\":{}}}");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_UNAUTHORIZED));

                response = client.postJson("/pub_test_deny/_search", "{\"query\":{\"match_all\":{}}}");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_UNAUTHORIZED));
            }

            try (GenericRestClient client = cluster.getRestClient(new BasicHeader("Authorization", "Bearer " + tokenWithDefaultSigningKey))) {
                HttpResponse response = client.postJson("/pub_test_allow_because_from_token/_search", "{\"query\":{\"match_all\":{}}}");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                response = client.postJson("/pub_test_deny/_search", "{\"query\":{\"match_all\":{}}}");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
            }
        } finally {
            //revert original auth token service config
            DocNode originalConfig = DocNode.parse(Format.YAML).from(SGCONFIG).findSingleNodeByJsonPath("sg_config.dynamic.auth_token_provider");
            DocNode updatedConfig = updateAuthTokenServiceConfig(originalConfig);
            assertThat(updatedConfig, anyOf(
                    hasEntry(equalTo("jwt_signing_key"), notNullValue()),
                    hasEntry(equalTo("jwt_signing_key_hs512"), notNullValue()))
            );
        }
    }

    @Test
    public void basicTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            assertThat(token, notNullValue());
            assertThat(getJwtHeaderValue(token, "alg"), equalTo("HS512"));

            String tokenPayload = getJwtPayload(token);
            Map<String, Object> parsedTokenPayload = DocReader.json().readObject(tokenPayload);
            assertThat(tokenPayload, JsonPath.using(BasicJsonPathDefaultConfiguration.defaultConfiguration())
                    .parse(parsedTokenPayload).read("sub"), equalTo("spock")
            );
            assertThat(tokenPayload, JsonPath.using(JSON_PATH_CONFIG).parse(parsedTokenPayload).read("base.c"), notNullValue());

            try (RestHighLevelClient client = cluster.getRestHighLevelClient("spock", "spock")) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                searchResponse = client.search(
                        new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("not_allowed_from_token"));
            }

            for (JvmEmbeddedEsCluster.Node node : cluster.nodes()) {
                try (RestHighLevelClient client = node.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                    SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                            .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                    assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                    assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                    assertThatThrown(() -> client.search(new SearchRequest("pub_test_deny")
                                    .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT),
                            messageContainsMatcher("Insufficient permissions")
                    );
                }
            }
        }

    }

    @Test
    public void basicTestUnfrozenPrivileges() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));
            request.setFreezePrivileges(false);

            request.setTokenName("my_new_token_unfrozen_privileges");

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");

            assertThat(token, notNullValue());
            assertThat(getJwtHeaderValue(token, "alg"), equalTo("HS512"));

            String tokenPayload = getJwtPayload(token);
            Map<String, Object> parsedTokenPayload = DocReader.json().readObject(tokenPayload);
            assertThat(tokenPayload, JsonPath.using(BasicJsonPathDefaultConfiguration.defaultConfiguration())
                    .parse(parsedTokenPayload).read("sub"), equalTo("spock")
            );
            assertThat(tokenPayload, JsonPath.using(JSON_PATH_CONFIG).parse(parsedTokenPayload).read("base.c"), nullValue());
            
            try (RestHighLevelClient client = cluster.getRestHighLevelClient("spock", "spock")) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                searchResponse = client.search(
                        new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("not_allowed_from_token"));
            }

            for (JvmEmbeddedEsCluster.Node node : cluster.nodes()) {
                try (RestHighLevelClient client = node.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                    SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                            .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                    assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                    assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                    assertThatThrown(() -> client.search(new SearchRequest("pub_test_deny")
                                    .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT),
                            messageContainsMatcher("Insufficient permissions")
                    );
                }
            }
        }
    }

    @Test
    public void maxTokenCountTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("nagilum", "nagilum")) {

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            for (int i = 0; i < 10; i++) {

                HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            }

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);
            
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
            assertThat(response.getBody(), 
                    response.getBodyAsDocNode().findSingleNodeByJsonPath("error.root_cause[0].reason").toString(), 
                    equalTo("Cannot create token. Token limit per user exceeded. Max number of allowed tokens is 10")
            );
        }
    }

    @Test
    public void createTokenWithTokenForbidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("cluster_permissions: '*'\nindex_permissions:\n- index_patterns: '*'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token_with_with_i_am_trying_to_create_another_token");

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);
            
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            assertThat(token, notNullValue());

            try (GenericRestClient tokenAuthRestClient = cluster.getRestClient(new BasicHeader("Authorization", "Bearer " + token))) {

                request = new CreateAuthTokenRequest(
                        RequestedPrivileges.parseYaml("cluster_permissions: '*'\nindex_permissions:\n- index_patterns: '*'\n  allowed_actions: '*'"));

                request.setTokenName("this_token_should_not_be_created");

                response = tokenAuthRestClient.postJson("/_searchguard/authtoken", request);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
                assertThat(response.getBody(), response.getBody(), containsString("Insufficient permissions"));
            }
        }
    }

    @Test
    public void userAttrTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("picard", "picard")) {

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: 'user_attr_*'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            System.out.println(request.toJson());

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            assertThat(token, notNullValue());

            try (RestHighLevelClient client = cluster.getRestHighLevelClient("picard", "picard")) {
                SearchResponse searchResponse = client.search(
                        new SearchRequest("user_attr_foo").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                assertThatThrown(() -> client.search(new SearchRequest("user_attr_qux")
                                .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT),
                        messageContainsMatcher("Insufficient permissions")
                );
            }

            try (RestHighLevelClient client = cluster.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(
                        new SearchRequest("user_attr_foo").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));;

                assertThatThrown(() -> client.search(new SearchRequest("user_attr_qux")
                                .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT),
                        messageContainsMatcher("Insufficient permissions")
                );
            }
        }
    }

    @Test
    public void userAttrTestDls() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("picard", "picard")) {

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            assertThat(token, notNullValue());

            try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
                SearchResponse searchResponse = client.search(
                        new SearchRequest("dls_user_attr").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
            }

            try (RestHighLevelClient client = cluster.getRestHighLevelClient("picard", "picard")) {
                SearchResponse searchResponse = client.search(
                        new SearchRequest("dls_user_attr").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));
            }

            try (RestHighLevelClient client = cluster.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(
                        new SearchRequest("dls_user_attr").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));
            }
        }
    }

    @Test
    public void revocationTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            String id = response.getBodyAsDocNode().getAsString("id");

            assertThat(token, notNullValue());
            assertThat(id, notNullValue());

            for (JvmEmbeddedEsCluster.Node node : cluster.nodes()) {
                try (RestHighLevelClient client = node.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                    SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                            .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                    assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                    assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                    assertThatThrown(() -> client.search(new SearchRequest("pub_test_deny")
                                    .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT),
                            messageContainsMatcher("Insufficient permissions")
                    );
                }
            }

            response = restClient.delete("/_searchguard/authtoken/" + id);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            Thread.sleep(100);

            for (JvmEmbeddedEsCluster.Node node : cluster.nodes()) {

                try (RestHighLevelClient client = node.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {

                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) assertThatThrown(
                            () -> client.search(new SearchRequest("pub_test_allow_because_from_token")
                                    .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT),
                            instanceOf(ElasticsearchStatusException.class)
                    );
                    assertThat(statusException.getMessage(), statusException.status(), equalTo(RestStatus.UNAUTHORIZED));
                }
            }
        }
    }

    @Test
    public void revocationWithoutSpecialPrivsTest() throws Exception {
        TestSgConfig sgConfig = AuthTokenIntegrationTest.sgConfig.clone()
                .sgConfigSettings("sg_config.dynamic.auth_token_provider.exclude_cluster_permissions", Collections.emptyList());

        try (LocalCluster.Embedded cluster = new LocalCluster.Builder().nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin").resources("authtoken")
                .sslEnabled().sgConfig(sgConfig).enterpriseModulesEnabled().enableModule(AuthTokenModule.class).embedded().start();
                GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {

            try (Client client = cluster.getInternalNodeClient()) {
                client.index(new IndexRequest("pub_test_allow_because_from_token").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                        "this_is", "allowed")).actionGet();
            }

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(RequestedPrivileges.totalWildcard());
            request.setTokenName("my_new_token_without_special_privs");
            request.setFreezePrivileges(false);

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request.toJson());

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            String id = response.getBodyAsDocNode().getAsString("id");

            assertThat(token, notNullValue());
            assertThat(id, notNullValue());

            for (JvmEmbeddedEsCluster.Node node : cluster.nodes()) {
                try (RestHighLevelClient client = node.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                    SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                            .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                    assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                    assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));
                }
            }

            response = restClient.delete("/_searchguard/authtoken/" + id);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            Thread.sleep(100);

            for (JvmEmbeddedEsCluster.Node node : cluster.nodes()) {
                try (RestHighLevelClient client = node.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) assertThatThrown(
                            () -> client.search(new SearchRequest("pub_test_allow_because_from_token")
                                    .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT),
                            instanceOf(ElasticsearchStatusException.class)
                    );
                    assertThat(statusException.getMessage(), statusException.status(), equalTo(RestStatus.UNAUTHORIZED));
                }
            }

        }
    }

    @Test
    public void getAndSearchTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("spock", "spock");
                GenericRestClient picardRestClient = cluster.getRestClient("picard", "picard");
                GenericRestClient adminRestClient = cluster.getRestClient("admin", "admin")) {

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

            request.setTokenName("get_and_search_test_token");

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            request.setTokenName("get_and_search_test_token_2");

            response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            request.setTokenName("get_and_search_test_token_picard");

            response = picardRestClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String picardsTokenId = response.getBodyAsDocNode().getAsString("token");

            response = restClient.get("/_searchguard/authtoken/_search");

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBody(), response.getBody(), not(containsString("\"picard\"")));

            String searchRequest = "{\n" + //
                    "    \"query\": {\n" + //
                    "        \"wildcard\": {\n" + //
                    "            \"token_name\": {\n" + //
                    "                \"value\": \"get_and_search_test_*\"\n" + //
                    "            }\n" + //
                    "        }\n" + //
                    "    }\n" + //
                    "}";

            response = restClient.postJson("/_searchguard/authtoken/_search", searchRequest);
            
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            DocNode jsonNode = response.getBodyAsDocNode();

            assertThat(response.getBody(), jsonNode.getAsNode("hits", "total", "value").toNumber(), equalTo(2));
            assertThat(response.getBody(), jsonNode.findSingleNodeByJsonPath("hits.hits[0]._source.user_name").toString(), equalTo("spock"));
            assertThat(response.getBody(), jsonNode.findSingleNodeByJsonPath("hits.hits[1]._source.user_name").toString(), equalTo("spock"));

            String id = jsonNode.getAsNode("hits").getAsListOfNodes("hits").get(0).getAsString("_id");
            String tokenName = jsonNode.getAsNode("hits").getAsListOfNodes("hits").get(0).getAsNode("_source").getAsString("token_name");

            response = restClient.get("/_searchguard/authtoken/" + id);

            assertThat(response.getBody(), response.getBodyAsDocNode().getAsString("token_name"), equalTo(tokenName));

            response = restClient.get("/_searchguard/authtoken/" + picardsTokenId);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));

            response = adminRestClient.postJson("/_searchguard/authtoken/_search", searchRequest);

            jsonNode = response.getBodyAsDocNode();

            assertThat(response.getBody(), jsonNode.get("hits", "total", "value"), equalTo(3));
            assertThat(response.getBody(), response.getBody(), containsString("\"spock\""));
            assertThat(response.getBody(), response.getBody(), containsString("\"picard\""));
        }
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

        try (LocalCluster.Embedded cluster = new LocalCluster.Builder().resources("authtoken").sslEnabled().singleNode().sgConfig(sgConfig)
                .enterpriseModulesEnabled().enableModule(AuthTokenModule.class).embedded().start();
                GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {

            try (Client client = cluster.getInternalNodeClient()) {
                client.index(new IndexRequest("pub_test_deny").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "this_is",
                        "not_allowed_from_token")).actionGet();
                client.index(new IndexRequest("pub_test_allow_because_from_token").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                        "this_is", "allowed")).actionGet();
            }

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            System.out.println(request.toJson());

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);
            
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            assertThat(token, notNullValue());
            assertThat(getJwtHeaderValue(token, "alg"), equalTo("A256KW"));
            assertThat(getJwtHeaderValue(token, "enc"), equalTo("A256CBC-HS512"));
            assertThat(
                    "JWT payload seems to be unencrypted because it contains the user name in clear text: " + getJwtPayload(token),
                    getJwtPayload(token), not(containsString("spock"))
            );

            try (RestHighLevelClient client = cluster.getRestHighLevelClient("spock", "spock")) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                searchResponse = client.search(
                        new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("not_allowed_from_token"));
            }

            try (RestHighLevelClient client = cluster.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                assertThatThrown(() -> client.search(new SearchRequest("pub_test_deny")
                                .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT),
                        messageContainsMatcher("Insufficient permissions")
                );
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

        try (LocalCluster.Embedded cluster = new LocalCluster.Builder().resources("authtoken").sslEnabled().singleNode().sgConfig(sgConfig)
                .enterpriseModulesEnabled().enableModule(AuthTokenModule.class).embedded().start();
                GenericRestClient restClient = cluster.getRestClient("spock", "spock")) {

            try (Client client = cluster.getInternalNodeClient()) {
                client.index(new IndexRequest("pub_test_deny").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "this_is",
                        "not_allowed_from_token")).actionGet();
                client.index(new IndexRequest("pub_test_allow_because_from_token").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                        "this_is", "allowed")).actionGet();
            }

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            System.out.println(request.toJson());

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            assertThat(token, notNullValue());
            assertThat(getJwtHeaderValue(token, "alg"), equalTo("ES256"));
            assertThat(getJwtPayload(token), getJwtPayload(token), containsString("spock"));

            try (RestHighLevelClient client = cluster.getRestHighLevelClient("spock", "spock")) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                searchResponse = client.search(
                        new SearchRequest("pub_test_deny").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())),
                        RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("not_allowed_from_token"));
            }

            try (RestHighLevelClient client = cluster.getRestHighLevelClient(new BasicHeader("Authorization", "Bearer " + token))) {
                SearchResponse searchResponse = client.search(new SearchRequest("pub_test_allow_because_from_token")
                        .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT);

                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("this_is"), equalTo("allowed"));

                assertThatThrown(() -> client.search(new SearchRequest("pub_test_deny")
                                .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery())), RequestOptions.DEFAULT),
                        messageContainsMatcher("Insufficient permissions")
                );
            }

        }
    }

    @Test
    public void sgAdminRestApiTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(RequestedPrivileges.parseYaml("cluster_permissions: ['*']"));

            request.setTokenName("rest_api_test_token");

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            assertThat(token, notNullValue());

            response = restClient.get("_searchguard/api/roles");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            try (GenericRestClient tokenAuthRestClient = cluster.getRestClient(new BasicHeader("Authorization", "Bearer " + token))) {
                response = restClient.get("_searchguard/api/roles");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            }
        }
    }

    @Test
    public void sgAdminRestApiForbiddenTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

            request.setTokenName("rest_api_test_token");

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            assertThat(token, notNullValue());

            response = restClient.get("_searchguard/api/roles");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            try (GenericRestClient tokenAuthRestClient = cluster.getRestClient(new BasicHeader("Authorization", "Bearer " + token))) {
                response = tokenAuthRestClient.get("_searchguard/api/roles");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
            }
        }
    }

    @Test
    public void sgAdminRestApiExclusionTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {

            CreateAuthTokenRequest request = new CreateAuthTokenRequest(RequestedPrivileges
                    .parseYaml("cluster_permissions: ['*']\nexclude_cluster_permissions: ['cluster:admin:searchguard:configrestapi']"));

            request.setTokenName("rest_api_test_token");

            HttpResponse response = restClient.postJson("/_searchguard/authtoken", request.toJson());

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String token = response.getBodyAsDocNode().getAsString("token");
            assertThat(token, notNullValue());

            response = restClient.get("_searchguard/api/roles");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            try (GenericRestClient tokenAuthRestClient = cluster.getRestClient(new BasicHeader("Authorization", "Bearer " + token))) {

                response = tokenAuthRestClient.get("_searchguard/api/roles");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
            }
        }
    }

    @Test
    public void infoApiTest() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {

            HttpResponse response = restClient.get("/_searchguard/authtoken/_info");

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBody(), response.getBodyAsDocNode().get("enabled"), equalTo(Boolean.TRUE));
        }
    }
    
    @Test
    public void bulkConfigApi() throws Exception {
        
        DocNode config = DocNode.of("jwt_signing_key_hs512", TestJwk.OCT_1_K, "max_tokens_per_user", 100, "enabled", true);
        
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {                        
            HttpResponse response = restClient.putJson("/_searchguard/config", DocNode.of("auth_token_service.content", config));                        
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            
            response = restClient.get("/_searchguard/config");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode().get("auth_token_service", "content"), equalTo(config.toMap()));
        }
    }

    private static String getJwtHeaderValue(String jwt, String headerName) throws DocumentParseException {
        int p = jwt.indexOf('.');
        String headerBase4 = jwt.substring(0, p);
        return DocNode.parse(Format.JSON).from(new String(BaseEncoding.base64Url().decode(headerBase4))).getAsString(headerName);
    }

    private static String getJwtPayload(String jwt) {
        int p = jwt.indexOf('.');
        int p2 = jwt.indexOf('.', p + 1);
        String headerBase4 = jwt.substring(p + 1, p2 != -1 ? p2 : jwt.length());
        return new String(BaseEncoding.base64Url().decode(headerBase4));
    }

    private DocNode updateAuthTokenServiceConfig(DocNode newConfig) throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            HttpResponse response = restClient.putJson("/_searchguard/config", DocNode.of("auth_token_service.content", newConfig));
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = restClient.get("/_searchguard/config");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode().get("auth_token_service", "content"), equalTo(newConfig.toMap()));
            return response.getBodyAsDocNode().findSingleNodeByJsonPath("auth_token_service.content");
        }
    }

}
