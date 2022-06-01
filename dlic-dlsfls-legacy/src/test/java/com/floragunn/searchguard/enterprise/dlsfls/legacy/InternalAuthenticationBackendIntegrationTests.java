package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import org.junit.Assert;
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

import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class InternalAuthenticationBackendIntegrationTests {

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("internal_user_db").enterpriseModulesEnabled().build();

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
