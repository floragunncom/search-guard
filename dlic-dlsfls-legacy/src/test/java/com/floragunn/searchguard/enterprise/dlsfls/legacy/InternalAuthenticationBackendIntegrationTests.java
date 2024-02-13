package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.floragunn.searchguard.client.RestHighLevelClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.RequestOptions;
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
            SearchResponse searchResponse = client.search("dls_test",0,100);
            Assert.assertEquals(5L, searchResponse.hits().total().value());
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("user_with_attributes", "nagilum")) {
            SearchResponse searchResponse = client.search("dls_test",0,100);
            Assert.assertEquals(3L, searchResponse.hits().total().value());
        }

    }
}
