package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.MsearchResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import com.floragunn.searchguard.client.RestHighLevelClient;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.RequestOptions;
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
        Client client = cluster.getInternalNodeClient();

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

    @Test
    public void testDlsWithTermsLookupSingleIndexMatchAll() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.search("deals_1");

            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1", "2"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.search("deals_1");

            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1", "2"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));;
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.search("deals_1");

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
        }
    }

    @Test
    public void testDlsWithTermsLookupWildcardIndexesMatchAll() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.search("deals_*");

            Assert.assertEquals(searchResponse.toString(), 6, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1", "2", "200", "201", "202"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.search("deals_*");

            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1", "2", "200"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.search("deals_*");

            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("201", "202"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }
    }

    @Test
    public void testDlsWithTermsLookupSingleIndexMatchQuery() throws Exception {

        co.elastic.clients.elasticsearch.core.SearchRequest searchRequest =
                new SearchRequest.Builder()
                        .index("deals_1")
                        .query(q->q.term(t->t.field("keywords").value(v->v.stringValue("test")))).build();

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 1, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
        }
    }

    @Test
    public void testMultiSearch() throws Exception {

        RequestItem searchRequest1 = new RequestItem.Builder()
                .body(b->b.query(bu->bu.term(t->t.field("keywords").value(v->v.stringValue("test")))))
                .header(h->h.index("deals_1"))
                .build();

        RequestItem searchRequest2 = new RequestItem.Builder()
                .body(b->b.query(bu->bu.term(t->t.field("keywords").value(v->v.stringValue("foo")))))
                .header(h->h.index("deals_2"))
                .build();

        MsearchRequest multiSearchRequest = new MsearchRequest.Builder()
                .searches(searchRequest1, searchRequest2)
                .maxConcurrentSearches(1l)
                .build();

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            MsearchResponse<Map> multiSearchResponse = client.getJavaClient().msearch(multiSearchRequest, Map.class);

            Assert.assertEquals(multiSearchResponse.toString(), 2,
                    multiSearchResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchResponse.toString(), 3,
                    multiSearchResponse.responses().get(1).result().hits().total().value());

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            MsearchResponse<Map> multiSearchResponse = client.getJavaClient().msearch(multiSearchRequest, Map.class);

            Assert.assertEquals(multiSearchResponse.toString(), 1,
                    multiSearchResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchResponse.toString(), 1,
                    multiSearchResponse.responses().get(1).result().hits().total().value());
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            MsearchResponse<Map> multiSearchResponse = client.getJavaClient().msearch(multiSearchRequest, Map.class);

            Assert.assertEquals(multiSearchResponse.toString(), 0,
                    multiSearchResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchResponse.toString(), 2,
                    multiSearchResponse.responses().get(1).result().hits().total().value());
        }
    }

    @Test
    public void testDlsWithTermsLookupSingleIndexUnmatchedQuery() throws Exception {

        co.elastic.clients.elasticsearch.core.SearchRequest searchRequest =
                new co.elastic.clients.elasticsearch.core.SearchRequest.Builder()
                        .index("deals_1")
                        .query(q->q.term(t->t.field("keywords").value(v->v.stringValue("xxxxxx")))).build();

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
        }
    }

    @Test
    public void testDlsWithTermsLookupGet() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("sg_dls_lookup_user1", "password")) {
            GenericRestClient.HttpResponse res = client.get("/deals_1/_doc/0?pretty");

            //System.out.println(res.getBody());

            Assert.assertEquals(res.getBody(), HttpStatus.SC_NOT_FOUND, res.getStatusCode());

            res = client.get("/deals_1/_doc/1?pretty");

            //System.out.println(res.getBody());

            Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
        }
    }

}
