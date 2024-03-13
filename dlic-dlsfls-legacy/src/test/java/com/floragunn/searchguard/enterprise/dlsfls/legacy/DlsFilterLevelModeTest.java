package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableSet;

public class DlsFilterLevelModeTest {
    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().resources("dlsfls_legacy")
            .nodeSettings(ConfigConstants.SEARCHGUARD_DLS_MODE, "filter_level").enterpriseModulesEnabled().embedded().build();

    @BeforeClass
    public static void setupTestData() {
        try (Client client = cluster.getInternalNodeClient()) {

            client.index(new IndexRequest("deals_1").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"amount\": 10, \"acodes\": [6,7], \"keywords\": [\"test\", \"foo\", \"bar\"]}", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("deals_1").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"amount\": 1500, \"acodes\": [1], \"keywords\": [\"test\", \"foo\", \"bar\"]}", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("deals_1").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"amount\": 2500, \"acodes\": [1], \"keywords\": [\"foo\"]}", XContentType.JSON)).actionGet();

            client.index(new IndexRequest("deals_2").id("200").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"amount\": 20, \"acodes\": [1], \"keywords\": [\"test\", \"foo\", \"bar\"]}", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("deals_2").id("201").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"amount\": 2500, \"acodes\": [3], \"keywords\": [\"test\", \"foo\", \"bar\"]}", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("deals_2").id("202").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"amount\": 3500, \"acodes\": [3], \"keywords\": [\"foo\"]}", XContentType.JSON)).actionGet();

            client.index(new IndexRequest("users").id("sg_dls_lookup_user1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"acode\": [1,2,4]}",
                    XContentType.JSON)).actionGet();
            client.index(new IndexRequest("users").id("sg_dls_lookup_user2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"acode\": [2,3]}",
                    XContentType.JSON)).actionGet();
        }
    }

    @Test
    public void testDlsWithTermsLookupSingleIndexMatchAll() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            SearchResponse searchResponse = client.search(new SearchRequest("deals_1"), RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1", "2"),
                    Arrays.asList(searchResponse.getHits().getHits()).stream().map((h) -> h.getId()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("deals_1"), RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1", "2"),
                    Arrays.asList(searchResponse.getHits().getHits()).stream().map((h) -> h.getId()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("deals_1"), RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
        }
    }

    @Test
    public void testDlsWithTermsLookupWildcardIndexesMatchAll() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            SearchResponse searchResponse = client.search(new SearchRequest("deals_*"), RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 6, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1", "2", "200", "201", "202"),
                    Arrays.asList(searchResponse.getHits().getHits()).stream().map((h) -> h.getId()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("deals_*"), RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1", "2", "200"),
                    Arrays.asList(searchResponse.getHits().getHits()).stream().map((h) -> h.getId()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("deals_*"), RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("201", "202"),
                    Arrays.asList(searchResponse.getHits().getHits()).stream().map((h) -> h.getId()).collect(Collectors.toSet()));
        }
    }

    @Test
    public void testDlsWithTermsLookupSingleIndexMatchQuery() throws Exception {

        SearchRequest searchRequest = new SearchRequest("deals_1")
                .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("keywords", "test")));

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1"),
                    Arrays.asList(searchResponse.getHits().getHits()).stream().map((h) -> h.getId()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 1, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1"),
                    Arrays.asList(searchResponse.getHits().getHits()).stream().map((h) -> h.getId()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
        }
    }

    @Test
    public void testMultiSearch() throws Exception {

        SearchRequest searchRequest1 = new SearchRequest("deals_1")
                .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("keywords", "test")));

        SearchRequest searchRequest2 = new SearchRequest("deals_2")
                .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("keywords", "foo")));

        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.add(searchRequest1);
        multiSearchRequest.add(searchRequest2);

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            MultiSearchResponse multiSearchResponse = client.msearch(multiSearchRequest, RequestOptions.DEFAULT);

            Assert.assertEquals(multiSearchResponse.toString(), 2,
                    multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value);
            Assert.assertEquals(multiSearchResponse.toString(), 3,
                    multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value);

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            MultiSearchResponse multiSearchResponse = client.msearch(multiSearchRequest, RequestOptions.DEFAULT);
            System.out.println(Strings.toString(multiSearchResponse));

            Assert.assertEquals(multiSearchResponse.toString(), 1,
                    multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value);
            Assert.assertEquals(multiSearchResponse.toString(), 1,
                    multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            MultiSearchResponse multiSearchResponse = client.msearch(multiSearchRequest, RequestOptions.DEFAULT);

            System.out.println(Strings.toString(multiSearchResponse));
            Assert.assertEquals(multiSearchResponse.toString(), 0,
                    multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value);
            Assert.assertEquals(multiSearchResponse.toString(), 2,
                    multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value);
        }
    }

    @Test
    public void testDlsWithTermsLookupSingleIndexUnmatchedQuery() throws Exception {

        SearchRequest searchRequest = new SearchRequest("deals_1")
                .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("keywords", "xxxxxx")));

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getHits().getTotalHits().value);
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.getFailedShards());
        }
    }

    @Test
    public void testDlsWithTermsLookupGet() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("sg_dls_lookup_user1", "password")) {
            GenericRestClient.HttpResponse res = client.get("/deals_1/_doc/0?pretty");

            System.out.println(res.getBody());

            Assert.assertEquals(res.getBody(), HttpStatus.SC_NOT_FOUND, res.getStatusCode());

            res = client.get("/deals_1/_doc/1?pretty");

            System.out.println(res.getBody());

            Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
        }
    }

}
