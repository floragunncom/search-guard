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

import static com.floragunn.searchguard.enterprise.dlsfls.legacy.DlsTermsLookupAsserts.assertAccessCodesMatch;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.client.RestHighLevelClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.MgetRequest;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.MsearchResponse;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.mget.MultiGetOperation;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.msearch.MultiSearchResponseItem;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import co.elastic.clients.elasticsearch.core.search.Hit;

public class DlsTermsLookupTest2 {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled()
            .sgConfigSettings("sg_config.dynamic.do_not_fail_on_forbidden", true)
            .roles(new TestSgConfig.Role("sg_dls_tlq_lookup").clusterPermissions("*").indexPermissions("*").on("tlqdummy").indexPermissions("*").dls(
                    "{ \"terms\": { \"access_codes\": { \"index\": \"user_access_codes\", \"id\": \"${user.name}\", \"path\": \"access_codes\" } } }")
                    .on("tlqdocuments")

            ).user("tlq_1337", "password", "sg_dls_tlq_lookup").user("tlq_42", "password", "sg_dls_tlq_lookup")
            .user("tlq_1337_42", "password", "sg_dls_tlq_lookup").user("tlq_999", "password", "sg_dls_tlq_lookup")
            .user("tlq_empty_access_codes", "password", "sg_dls_tlq_lookup").user("tlq_no_codes", "password", "sg_dls_tlq_lookup")
            .user("tlq_no_entry_in_user_index", "password", "sg_dls_tlq_lookup").build();

    @BeforeClass
    public static void setupTestData() {
        Client client = cluster.getInternalNodeClient();

        // user access codes, basis for TLQ query
        client.index(new IndexRequest("user_access_codes").id("tlq_1337").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("user_access_codes").id("tlq_42").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"access_codes\": [42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("user_access_codes").id("tlq_1337_42").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("user_access_codes").id("tlq_999").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"access_codes\": [999] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("user_access_codes").id("tlq_empty_access_codes").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"access_codes\": [] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("user_access_codes").id("tlq_no_codes").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bla\": \"blub\" }", XContentType.JSON)).actionGet();

        // need to have keyword for bu field since we're testing aggregations
        client.admin().indices().create(new CreateIndexRequest("tlqdocuments")).actionGet();
        client.admin().indices().putMapping(new PutMappingRequest("tlqdocuments").source("bu", "type=keyword")).actionGet();

        // tlqdocuments, protected by TLQ
        client.index(new IndexRequest("tlqdocuments").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"AAA\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"AAA\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("3").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"AAA\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("4").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"BBB\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("5").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"BBB\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("6").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"BBB\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("7").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"CCC\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("8").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"CCC\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("9").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"CCC\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("10").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"DDD\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("11").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"DDD\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("12").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"DDD\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("13").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"EEE\", \"access_codes\": [1337] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("14").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"EEE\", \"access_codes\": [42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("15").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"EEE\", \"access_codes\": [1337, 42] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("16").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{ \"bu\": \"FFF\" }",
                XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("17").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"FFF\", \"access_codes\": [12345] }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdocuments").id("18").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"bu\": \"FFF\", \"access_codes\": [12345, 6789] }", XContentType.JSON)).actionGet();

        // we use a "bu" field here as well to test aggregations over multiple indices (TBD)
        client.admin().indices().create(new CreateIndexRequest("tlqdummy")).actionGet();
        client.admin().indices().putMapping(new PutMappingRequest("tlqdummy").source("bu", "type=keyword")).actionGet();

        // tlqdummy, not protected by TLQ
        client.index(new IndexRequest("tlqdummy").id("101").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"mykey\": \"101\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdummy").id("102").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"mykey\": \"102\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdummy").id("103").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"mykey\": \"103\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdummy").id("104").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"mykey\": \"104\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
        client.index(new IndexRequest("tlqdummy").id("105").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{ \"mykey\": \"105\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();

    }

    // ------------------------
    // Test search and msearch
    // ------------------------

    @Test
    public void testSimpleSearch_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("tlqdocuments");
            // 10 docs, all need to have access code 1337    
            Assert.assertEquals(searchResponse.toString(), 10, searchResponse.hits().total().value());
            // fields need to have 1337 access code
            assertAccessCodesMatch(searchResponse.hits().hits(), new Integer[] { 1337 });
        }
    }

    @Test
    public void testSimpleSearch_AccessCode_42() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_42", "password")) {
            SearchResponse<Map> searchResponse = client.search(("tlqdocuments"));
            // 10 docs, all need to have access code 42    
            Assert.assertEquals(searchResponse.toString(), 10, searchResponse.hits().total().value());
            // fields need to have 42 access code
            assertAccessCodesMatch(searchResponse.hits().hits(), new Integer[] { 42 });
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_1337_42() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337_42", "password")) {
            SearchResponse<Map> searchResponse = client.search(("tlqdocuments"));
            // 15 docs, all need to have either access code 1337 or 42    
            Assert.assertEquals(searchResponse.toString(), 15, searchResponse.hits().total().value());
            // fields need to have 42 or 1337 access code
            assertAccessCodesMatch(searchResponse.hits().hits(), new Integer[] { 42, 1337 });
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_999() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_999", "password")) {
            SearchResponse<Map> searchResponse = client.search(("tlqdocuments"));
            // no docs match, expect empty result    
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_emptyAccessCodes() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_empty_access_codes", "password")) {
            SearchResponse<Map> searchResponse = client.search(("tlqdocuments"));
            // user has an empty array for access codes, expect no error and empty search result
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_noAccessCodes() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_no_codes", "password")) {
            SearchResponse<Map> searchResponse = client.search(("tlqdocuments"));
            // user has no access code , expect no error and empty search result
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
        }
    }

    @Test
    public void testSimpleSearch_AccessCodes_noEntryInUserIndex() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_no_codes", "password")) {
            SearchResponse<Map> searchResponse = client.search(("tlqdocuments"));
            // user has no entry in user index, expect no error and empty search result
            Assert.assertEquals(searchResponse.toString(), 0, searchResponse.hits().total().value());
        }
    }

    @Test
    public void testSimpleSearch_AllIndices_All_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("_all", 0, 100);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, contains all documents
            // no access to user_access_codes must be granted 

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<Hit<Map>>tlqdummyHits = (searchResponse.hits().hits()).stream().filter((h) -> h.index().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            Set<Hit<Map>>tlqdocumentHits = (searchResponse.hits().hits()).stream()
                    .filter((h) -> h.index().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });

            // check no access to user_access_codes index
            Set<Hit<Map>>userAccessCodesHits = (searchResponse.hits().hits()).stream()
                    .filter((h) -> h.index().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_AllIndicesWildcard_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("*",0,100);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, contains all documents
            // no access to user_access_codes must be granted 

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<Hit<Map>>tlqdummyHits = (searchResponse.hits().hits()).stream().filter((h) -> h.index().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            Set<Hit<Map>>tlqdocumentHits = (searchResponse.hits().hits()).stream()
                    .filter((h) -> h.index().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });

            // check no access to user_access_codes index
            Set<Hit<Map>>userAccessCodesHits = (searchResponse.hits().hits()).stream()
                    .filter((h) -> h.index().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_ThreeIndicesWildcard_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("tlq*, user*", 0, 100);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, contains all documents
            // no access to user_access_codes must be granted 

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<Hit<Map>>tlqdummyHits = (searchResponse.hits().hits()).stream().filter((h) -> h.index().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<Hit<Map>>tlqdocumentHits = (searchResponse.hits().hits()).stream()
                    .filter((h) -> h.index().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });

            // check no access to user_access_codes index
            Set<Hit<Map>>userAccessCodesHits = (searchResponse.hits().hits()).stream()
                    .filter((h) -> h.index().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_TwoIndicesConcreteNames_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("tlqdocuments,tlqdummy",0,100);

            // assume hits from 2 indices:
            // - tlqdocuments, must contains only 10 docs with access code 1337
            // - tlqdummy, must contains all 5 documents

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<Hit<Map>>tlqdummyHits = (searchResponse.hits().hits()).stream().filter((h) -> h.index().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // ccheck 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered         
            Set<Hit<Map>>tlqdocumentHits = (searchResponse.hits().hits()).stream()
                    .filter((h) -> h.index().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
        }
    }

    @Test
    public void testMSearch_ThreeIndices_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            MsearchRequest.Builder multiSearchRequest = new MsearchRequest.Builder();
            multiSearchRequest.searches(
                    new RequestItem.Builder().body(b->b).header(h->h.index("tlqdummy")).build(),
                    new RequestItem.Builder().body(b->b).header(h->h.index("tlqdocuments")).build(),
                    new RequestItem.Builder().body(b->b).header(h->h.index("user_access_codes")).build());

            MsearchResponse<Map> searchResponse = client.getJavaClient().msearch(multiSearchRequest.build(), Map.class);

            List<MultiSearchResponseItem<Map>> responseItems = searchResponse.responses();

            // as per API order in response is the same as in the msearch request

            // check all 5 tlqdummy entries present
            List<Hit<Map>> tlqdummyHits = responseItems.get(0).result().hits().hits();
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            List<Hit<Map>> tlqdocumentHits = responseItems.get(1).result().hits().hits();
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });

            // check no access to user_access_codes index, just two indices in the response
            Assert.assertTrue(responseItems.get(2).failure() != null);
            Assert.assertTrue(responseItems.get(2).isFailure());

        }
    }

    // ------------------------
    // Test get and met
    // ------------------------

    @Test
    public void testGet_TlqDocumentsIndex_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            // user has 1337, document has 1337
            GetResponse<Map> searchResponse = client.get("tlqdocuments", "1");
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.found());
            assertAccessCodesMatch(searchResponse.source(), "access_codes", new Integer[] { 1337 });

            // user has 1337, document has 42, not visible
            searchResponse = client.get("tlqdocuments", "2");
            Assert.assertFalse(searchResponse.found());

            // user has 1337, document has 42 and 1337
            searchResponse = client.get("tlqdocuments", "3");
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.found());
            assertAccessCodesMatch(searchResponse.source(), "access_codes", new Integer[] { 1337 });

            // user has 1337, document has no access codes, not visible
            searchResponse = client.get("tlqdocuments", "16");
            Assert.assertFalse(searchResponse.found());

            // user has 1337, document has 12345, not visible
            searchResponse = client.get("tlqdocuments", "17");
            Assert.assertFalse(searchResponse.found());

            // user has 1337, document has 12345 and 6789, not visible
            searchResponse = client.get("tlqdocuments", "18");
            Assert.assertFalse(searchResponse.found());

        }
    }

    @Test
    public void testGet_TlqDocumentsIndex_1337_42() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337_42", "password")) {

            // user has 1337 and 42, document has 1337
            GetResponse<Map> searchResponse = client.get("tlqdocuments", "1");
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.found());
            assertAccessCodesMatch(searchResponse.source(), "access_codes", new Integer[] { 1337, 42 });

            // user has 1337 and 42, document has 42
            searchResponse = client.get("tlqdocuments", "2");
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.found());
            assertAccessCodesMatch(searchResponse.source(), "access_codes", new Integer[] { 1337, 42 });

            // user has 1337 and 42, document has 42 and 1337
            searchResponse = client.get("tlqdocuments", "3");
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.found());
            assertAccessCodesMatch(searchResponse.source(), "access_codes", new Integer[] { 1337, 42 });

            // user has 1337 and 42, document has no access codes, not visible
            searchResponse = client.get("tlqdocuments", "16");
            Assert.assertFalse(searchResponse.found());

            // user has 1337 and 42, document has 12345, not visible
            searchResponse = client.get("tlqdocuments", "17");
            Assert.assertFalse(searchResponse.found());

            // user has 1337 and 42, document has 12345 and 6789, not visible
            searchResponse = client.get("tlqdocuments", "18");
            Assert.assertFalse(searchResponse.found());

        }
    }

    @Test
    public void testGet_TlqDummyIndex_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            // no restrictions on this index
            GetResponse searchResponse = client.get("tlqdummy", "101");
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.found());

            searchResponse = client.get("tlqdummy", "102");
            Assert.assertTrue(searchResponse != null);
            Assert.assertTrue(searchResponse.found());

        }
    }

    @Test
    public void testGet_UserAccessCodesIndex_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {
            // no access to user_codes index, must throw exception
            client.get("user_access_codes","tlq_1337");
            Assert.fail();
        } catch (ElasticsearchException e) {
            Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN.getStatus(), e.status());
        }
    }

    @Test
    public void testMGet_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            MgetRequest request = new MgetRequest.Builder()
                    .docs(new MultiGetOperation.Builder().index("tlqdocuments").id("1").build(),
                    new MultiGetOperation.Builder().index("tlqdocuments").id("2").build(),
                    new MultiGetOperation.Builder().index("tlqdocuments").id("3").build(),
                    new MultiGetOperation.Builder().index("tlqdocuments").id("16").build(),
                    new MultiGetOperation.Builder().index("tlqdocuments").id("17").build(),
                    new MultiGetOperation.Builder().index("tlqdocuments").id("18").build(),
                    new MultiGetOperation.Builder().index("tlqdummy").id("101").build()
                    ).build();

            MgetResponse<Map> searchResponse = client.getJavaClient().mget(request, Map.class);

            for (MultiGetResponseItem<Map> response : searchResponse.docs()) {
                // no response from index "user_access_codes"
                Assert.assertFalse(response.result().index().equals("user_access_codes"));
                switch (response.result().index()) {
                case "tlqdocuments":
                    Assert.assertTrue(response.result().id(), response.result().id().equals("1") | response.result().id().equals("3"));
                    break;
                case "tlqdummy":
                    Assert.assertTrue(response.result().id(), response.result().id().equals("101"));
                    break;
                default:
                    Assert.fail("Index " + response.result().index() + " present in mget response, but should not");
                }
            }

            request = new MgetRequest.Builder()
                    .docs(new MultiGetOperation.Builder().index("tlqdocuments").id("1").build(),
                            new MultiGetOperation.Builder().index("tlqdocuments").id("2").build(),
                            new MultiGetOperation.Builder().index("tlqdocuments").id("3").build(),
                            new MultiGetOperation.Builder().index("tlqdocuments").id("16").build(),
                            new MultiGetOperation.Builder().index("tlqdocuments").id("17").build(),
                            new MultiGetOperation.Builder().index("tlqdocuments").id("18").build(),
                            new MultiGetOperation.Builder().index("tlqdummy").id("101").build(),
                            new MultiGetOperation.Builder().index("user_access_codes").id("tlq_1337").build()
                    ).build();

            try {
                searchResponse = client.getJavaClient().mget(request, Map.class);
                Assert.fail(searchResponse.toString());
            } catch (ElasticsearchException e) {
                Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN.getStatus(), e.status());
            }
        }
    }

    // ------------------------
    // Test aggregations
    // ------------------------

    @Test
    public void testSimpleAggregation_tlqdocuments_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index("tlqdocuments")
                    .aggregations("buaggregation", a->a.terms(ta->ta.field("bu"))).build();

            SearchResponse<Map> searchResponse = client.getJavaClient().search(searchRequest, Map.class);

            Aggregate aggs = searchResponse.aggregations().get("buaggregation");
            Assert.assertNotNull(searchResponse.toString(), aggs);
            StringTermsAggregate agg = aggs.sterms();
            Assert.assertTrue("Expected aggregation with name 'buaggregation'", agg != null);
            // expect AAA - EEE (FFF does not match) with 2 docs each
            for (String bucketName : new String[] { "AAA", "BBB", "CCC", "DDD", "EEE" }) {
                Optional<StringTermsBucket> bucket = DlsTermsLookupAsserts.getBucket(agg, bucketName);
                Assert.assertTrue(bucket.isPresent());
                Assert.assertNotNull("Expected bucket " + bucketName + " to be present in agregations", bucket.get());
                Assert.assertTrue("Expected doc count in bucket " + bucketName + " to be 2", bucket.get().docCount() == 2);
            }
            // expect FFF to be absent
            Assert.assertFalse("Expected bucket FFF to be absent", DlsTermsLookupAsserts.getBucket(agg, "FFF").isPresent());
        }
    }

    // ------------------------
    // Test scroll
    // ------------------------

    @Test
    public void testSimpleSearch_Scroll_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("tlq_1337", "password")) {

            SearchResponse<Map> searchResponse = client.getJavaClient().search(s->s
                    .index("tlqdocuments")
                    .size(1)
                    .scroll(new Time.Builder().time("1m").build()), Map.class);

            String scrollId = searchResponse.scrollId();
            List<Hit<Map>> searchHits = searchResponse.hits().hits();
            int totalHits = 0;

            // we scroll one by one
            for(;;) {
                // for counting the total documents
                totalHits += searchHits.size();

                // only docs with access codes 1337 must be returned
                Assert.assertEquals(1, searchHits.size());
                assertAccessCodesMatch(searchHits, new Integer[] { 1337 });

                if(scrollId == null) {
                    break;
                }
                // fetch next
                final String finalScrollId = scrollId;
                final ScrollResponse scrollResponse = client.getJavaClient().scroll(s->s.scrollId(finalScrollId)
                        .scroll(new Time.Builder().time("1m").build()), Map.class);

                scrollId = scrollResponse.scrollId();
                searchHits = scrollResponse.hits().hits();

                if(searchHits.isEmpty()) {
                    break;
                }
            }

            // assume total of 10 documents
            Assert.assertEquals(10, totalHits);
        }
    }

}