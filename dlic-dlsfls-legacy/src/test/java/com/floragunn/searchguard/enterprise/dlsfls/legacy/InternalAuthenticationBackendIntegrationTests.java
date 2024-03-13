package com.floragunn.searchguard.enterprise.dlsfls.legacy;

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
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class InternalAuthenticationBackendIntegrationTests {

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("internal_user_db").enterpriseModulesEnabled().embedded().build();

    @Test
    public void dlsIntegrationTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            client.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"a\", \"amount\": 1010}",
                    XContentType.JSON)).actionGet();
            client.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"b\", \"amount\": 2020}",
                    XContentType.JSON)).actionGet();
            client.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"c\", \"amount\": 3030}",
                    XContentType.JSON)).actionGet();
            client.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"d\", \"amount\": 4040}",
                    XContentType.JSON)).actionGet();
            client.index(new IndexRequest("dls_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"e\", \"amount\": 5050}",
                    XContentType.JSON)).actionGet();
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "nagilum")) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("dls_test").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(5, searchResponse.getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("user_with_attributes", "nagilum")) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("dls_test").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(3, searchResponse.getHits().getTotalHits().value);
        }

    }
}
