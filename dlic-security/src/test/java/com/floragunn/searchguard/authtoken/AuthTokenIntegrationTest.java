package com.floragunn.searchguard.authtoken;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

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
                    "          config: {}\n" + //
                    "        description: \"Migrated from v6\"\n" + //
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

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().resources("authtoken").sslEnabled().sgConfig(CType.CONFIG, SGCONFIG).build();

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
        }

    }

    @Test
    public void basicTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*_from_token'\n  allowed_actions: '*'"));

        request.setTokenName("my_new_token");

        Header auth = basicAuth("spock", "spock");

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        System.out.println(response.getBody());

        Assert.assertEquals(200, response.getStatusCode());

        String token = response.toJsonNode().get("token").asText();
        Assert.assertNotNull(token);

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

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }
}
