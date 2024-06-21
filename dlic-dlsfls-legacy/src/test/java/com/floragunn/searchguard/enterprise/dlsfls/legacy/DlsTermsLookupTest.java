/*
 * Copyright 2021 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.MsearchResponse;
import co.elastic.clients.elasticsearch.core.MsearchTemplateRequest;
import co.elastic.clients.elasticsearch.core.MsearchTemplateResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.SearchTemplateRequest;
import co.elastic.clients.elasticsearch.core.SearchTemplateResponse;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.json.JsonData;
import com.floragunn.searchguard.client.RestHighLevelClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableSet;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;

public class DlsTermsLookupTest {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().resources("dlsfls_legacy").enterpriseModulesEnabled().embedded().build();

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
            SearchResponse<Map> searchResponse = client.search("deals_1");

            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1", "2"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            SearchResponse<Map> searchResponse = client.search("deals_1");

            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1", "2"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            SearchResponse<Map> searchResponse = client.search("deals_1");

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
        }
    }

    @Test
    public void testDlsWithTermsLookupWildcardIndexesMatchAll() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            SearchResponse<Map> searchResponse = client.search("deals_*");

            Assert.assertEquals(searchResponse.toString(), 6, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1", "2", "200", "201", "202"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            SearchResponse<Map> searchResponse = client.search("deals_*");

            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1", "2", "200"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            SearchResponse<Map> searchResponse = client.search("deals_*");

            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("201", "202"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }
    }

    @Test
    public void testDlsWithTermsLookupSingleIndexMatchQuery() throws Exception {

        SearchRequest searchRequest =
                new SearchRequest.Builder()
                        .index("deals_1")
                        .query(q->q.term(t->t.field("keywords").value(v->v.stringValue("test")))).build();

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 1, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1"),
                    searchResponse.hits().hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

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
            ////System.out.println(Strings.toString(multiSearchResponse));

            Assert.assertEquals(multiSearchResponse.toString(), 1,
                    multiSearchResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchResponse.toString(), 1,
                    multiSearchResponse.responses().get(1).result().hits().total().value());
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            MsearchResponse<Map> multiSearchResponse = client.getJavaClient().msearch(multiSearchRequest, Map.class);

            ////System.out.println(Strings.toString(multiSearchResponse));
            Assert.assertEquals(multiSearchResponse.toString(), 0,
                    multiSearchResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchResponse.toString(), 2,
                    multiSearchResponse.responses().get(1).result().hits().total().value());
        }
    }

    @Test
    public void testMultiSearch_maxConcurrentSearchRequests() throws Exception {

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
            ////System.out.println(Strings.toString(multiSearchResponse));

            Assert.assertEquals(multiSearchResponse.toString(), 1,
                    multiSearchResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchResponse.toString(), 1,
                    multiSearchResponse.responses().get(1).result().hits().total().value());
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            MsearchResponse<Map> multiSearchResponse = client.getJavaClient().msearch(multiSearchRequest, Map.class);

            ////System.out.println(Strings.toString(multiSearchResponse));
            Assert.assertEquals(multiSearchResponse.toString(), 0,
                    multiSearchResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchResponse.toString(), 2,
                    multiSearchResponse.responses().get(1).result().hits().total().value());
        }
    }

    
    @Test
    public void testDlsWithTermsLookupSingleIndexUnmatchedQuery() throws Exception {
        SearchRequest searchRequest =
                new SearchRequest.Builder()
                        .index("deals_1")
                        .query(q->q.term(t->t.field("keywords").value(v->v.stringValue("xxxxxx")))).build();

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.shards().failed().intValue());

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

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

    @Test
    public void testSearchTemplate() throws Exception {

        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest.Builder()
                .index("deals_1")
                .source("{\"query\": {\"term\": {\"keywords\": \"{{x}}\" } } }")
                .params(Map.of("x", JsonData.of("test")))
                .build();

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            SearchTemplateResponse<Map> searchTemplateResponse = client.getJavaClient()
                    .searchTemplate(searchTemplateRequest, Map.class);
            HitsMetadata<Map> searchResponse = searchTemplateResponse.hits();



            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchTemplateResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("0", "1"),
                    searchResponse.hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            SearchTemplateResponse<Map> searchTemplateResponse = client.getJavaClient()
                    .searchTemplate(searchTemplateRequest, Map.class);
            HitsMetadata<Map> searchResponse = searchTemplateResponse.hits();

            Assert.assertEquals(searchResponse.toString(), 1, searchResponse.total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchTemplateResponse.shards().failed().intValue());
            Assert.assertEquals(searchResponse.toString(), ImmutableSet.of("1"),
                    searchResponse.hits().stream().map((h) -> h.id()).collect(Collectors.toSet()));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            SearchTemplateResponse<Map> searchTemplateResponse = client.getJavaClient()
                    .searchTemplate(searchTemplateRequest, Map.class);
            HitsMetadata<Map> searchResponse = searchTemplateResponse.hits();

            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.total().value());
            Assert.assertEquals(searchResponse.toString(), 0, searchTemplateResponse.shards().failed().intValue());
        }
    }

    @Test
    public void testMultiSearchTemplate() throws Exception {

        MsearchTemplateRequest multiSearchRequest = new MsearchTemplateRequest.Builder()
                .searchTemplates(
                new co.elastic.clients.elasticsearch.core.msearch_template.RequestItem.Builder()
                        .header(h -> h.index("deals_1"))
                        .body(b->b
                                .source("{\"query\": {\"term\": {\"keywords\": \"{{x}}\" } } }")
                                .params(Map.of("x", JsonData.of("test")))).build())

                .searchTemplates(
                new co.elastic.clients.elasticsearch.core.msearch_template.RequestItem.Builder()
                        .header(h -> h.index("deals_2"))
                        .body(b->b
                                .source("{\"query\": {\"term\": {\"keywords\": \"{{x}}\" } } }")
                                .params(Map.of("x", JsonData.of("foo")))).build())
                .build();

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("admin", "admin")) {
            MsearchTemplateResponse<Map> multiSearchTemplateResponse = client.getJavaClient().msearchTemplate(multiSearchRequest, Map.class);

            Assert.assertEquals(multiSearchTemplateResponse.toString(), 2,
                    multiSearchTemplateResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchTemplateResponse.toString(), 3,
                    multiSearchTemplateResponse.responses().get(1).result().hits().total().value());

        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user1", "password")) {
            MsearchTemplateResponse<Map> multiSearchTemplateResponse = client.getJavaClient().msearchTemplate(multiSearchRequest, Map.class);

            Assert.assertEquals(multiSearchTemplateResponse.toString(), 1,
                    multiSearchTemplateResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchTemplateResponse.toString(), 1,
                    multiSearchTemplateResponse.responses().get(1).result().hits().total().value());
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("sg_dls_lookup_user2", "password")) {
            MsearchTemplateResponse<Map> multiSearchTemplateResponse = client.getJavaClient().msearchTemplate(multiSearchRequest, Map.class);

            Assert.assertEquals(multiSearchTemplateResponse.toString(), 0,
                    multiSearchTemplateResponse.responses().get(0).result().hits().total().value());
            Assert.assertEquals(multiSearchTemplateResponse.toString(), 2,
                    multiSearchTemplateResponse.responses().get(1).result().hits().total().value());
        }
    }

    @Test
    public void testDlsWithTermsLookupGetTLQDisabled() throws Exception {

        try (LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().resources("dlsfls_legacy").nodeSettings("searchguard.dls.mode", "lucene_level")
                .enterpriseModulesEnabled().embedded().start()) {
            Client elasticClient = cluster.getInternalNodeClient();

            elasticClient.index(new IndexRequest("deals").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"amount\": 10, \"acodes\": [6,7]}",
                    XContentType.JSON)).actionGet();
            elasticClient.index(new IndexRequest("deals").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"amount\": 1500, \"acodes\": [1]}",
                    XContentType.JSON)).actionGet();

            elasticClient.index(new IndexRequest("users").id("sg_dls_lookup_user1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"acode\": [1,2,4]}", XContentType.JSON)).actionGet();
            elasticClient.index(new IndexRequest("users").id("sg_dls_lookup_user2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"acode\": [2,3]}", XContentType.JSON)).actionGet();

            try (GenericRestClient client = cluster.getRestClient("sg_dls_lookup_user1", "password")) {

                GenericRestClient.HttpResponse res = client.get("/deals/_doc/0?pretty");

                Assert.assertEquals(res.getBody(), HttpStatus.SC_BAD_REQUEST, res.getStatusCode());

                res = client.get("/deals/_doc/1?pretty");

                Assert.assertEquals(res.getBody(), HttpStatus.SC_BAD_REQUEST, res.getStatusCode());

                Assert.assertTrue(res.getBody(), res.getBody().contains("async actions are left after rewrite"));
            }
        }
    }
}