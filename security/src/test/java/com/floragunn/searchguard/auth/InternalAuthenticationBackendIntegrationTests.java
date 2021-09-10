package com.floragunn.searchguard.auth;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.Client;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

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
