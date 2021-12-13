package com.floragunn.searchguard.auth;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;

public class InternalAuthenticationBackendIntegrationTests {
    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", "true") //
            .user("all_access", "secret", "sg_all_access") //
            .build();

    @BeforeClass
    public static void initTestData() {
        try (Client tc = cluster.getAdminCertClient()) {

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
    public void attributeIntegrationTest() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("all_access", "secret")) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(5, searchResponse.getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("user_with_attributes", "nagilum")) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(1, searchResponse.getHits().getTotalHits().value);
        }

    }

    @Test
    public void attributeRegexIntegrationTest() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("all_access", "secret")) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(5, searchResponse.getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("user_with_attributes2", "nagilum")) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(3, searchResponse.getHits().getTotalHits().value);
        }

    }
    
    @Test
    public void testAuthDomainInfo() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("all_access", "secret")) {
            HttpResponse response = restClient.get("/_searchguard/authinfo");
            Assert.assertTrue(response.getBody(), response.toJsonNode().path("user").asText().startsWith("User all_access <basic/internal>"));
        }
    }

}
