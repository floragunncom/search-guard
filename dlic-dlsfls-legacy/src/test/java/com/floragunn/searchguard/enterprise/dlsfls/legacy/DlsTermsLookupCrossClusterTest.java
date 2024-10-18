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
import static com.floragunn.searchguard.enterprise.dlsfls.legacy.DlsTermsLookupAsserts.assertAllHitsComeFromCluster;
import static com.floragunn.searchguard.enterprise.dlsfls.legacy.DlsTermsLookupAsserts.assertAllHitsComeFromLocalCluster;
import static com.floragunn.searchguard.enterprise.dlsfls.legacy.DlsTermsLookupAsserts.assertBuMatches;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Tests TLQ DLS with CCS
 * @author jkressin
 *
 */
public class DlsTermsLookupCrossClusterTest {

    private final static String CLUSTER_ALIAS = "my_remote";

    private final static TestCertificates testCertificates = TestCertificates.builder()
            .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com;OU=SearchGuard;O=SearchGuard")
            .build();

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup(); 
    
    @ClassRule
    public static LocalCluster.Embedded remote = new LocalCluster.Builder().singleNode().resources("dlsfls_legacy").sslEnabled(testCertificates)//
            .nodeSettings("searchguard.logging.context.extended", true)//
            .clusterName("remote")//
            .enterpriseModulesEnabled()//
            .roles(new Role("sg_dls_tlq_lookup").clusterPermissions("*").indexPermissions("*").on("tlqdummy").indexPermissions("*")

                    .dls("{ \"terms\": { \"access_codes\": { \"index\": \"user_access_codes\", \"id\": \"${user.name}\", \"path\": \"access_codes\" } } }")
                    .on("tlqdocuments")

            )//
            .user("tlq_1337", "password", "sg_dls_tlq_lookup").user("tlq_42", "password", "sg_dls_tlq_lookup")//
            .user("tlq_1337_42", "password", "sg_dls_tlq_lookup").user("tlq_999", "password", "sg_dls_tlq_lookup")//
            .user("tlq_empty_access_codes", "password", "sg_dls_tlq_lookup").user("tlq_no_codes", "password", "sg_dls_tlq_lookup")//
            .user("tlq_no_entry_in_user_index", "password", "sg_dls_tlq_lookup").user("admin", "password", "sg_admin")//
            .embedded().build();

    @ClassRule
    public static LocalCluster.Embedded coordinating = new LocalCluster.Builder().singleNode().resources("dlsfls_legacy").sslEnabled(testCertificates).remote(CLUSTER_ALIAS, remote)//
            .nodeSettings("searchguard.logging.context.extended", true)//
            .clusterName("coordinating")//
            .enterpriseModulesEnabled()//
            .roles(new Role("sg_dls_tlq_lookup").clusterPermissions("*").indexPermissions("*")
                    .dls("{ \"bool\": { \"must\": { \"match\": { \"bu\": \"GGG\"  }}}}").on("tlqdummy").indexPermissions("*")
                    .dls("{ \"bool\": { \"must\": { \"match\": { \"bu\": \"AAA\"  }}}}"
                    // THIS FAILS: "{ \"terms\": { \"access_codes\": { \"index\": \"user_access_codes\", \"id\": \"${user.name}\", \"path\": \"access_codes\" } } }"
                    ).on("tlqdocuments"))//
            .user("tlq_1337", "password", "sg_dls_tlq_lookup").user("tlq_42", "password", "sg_dls_tlq_lookup")//
            .user("tlq_1337_42", "password", "sg_dls_tlq_lookup").user("tlq_999", "password", "sg_dls_tlq_lookup")//
            .user("tlq_empty_access_codes", "password", "sg_dls_tlq_lookup").user("tlq_no_codes", "password", "sg_dls_tlq_lookup")//
            .user("tlq_no_entry_in_user_index", "password", "sg_dls_tlq_lookup").user("admin", "password", "sg_admin")//
            .embedded().build();

    @BeforeClass
    public static void setupTestData() {
        // we use the same data in both clusters but different DLS definitions on remote and coordinating
        for (LocalCluster.Embedded cluster : new LocalCluster.Embedded[] { remote, coordinating }) {
            try (Client client = cluster.getInternalNodeClient()) {

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
                client.admin().indices().putMapping(new PutMappingRequest("tlqdocuments").type("_doc").source("bu", "type=keyword")).actionGet();

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
                client.admin().indices().putMapping(new PutMappingRequest("tlqdummy").type("_doc").source("bu", "type=keyword")).actionGet();

                // tlqdummy, not protected by TLQ
                client.index(new IndexRequest("tlqdummy").id("101").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .source("{ \"mykey\": \"101\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
                client.index(new IndexRequest("tlqdummy").id("102").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .source("{ \"mykey\": \"102\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
                client.index(new IndexRequest("tlqdummy").id("103").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .source("{ \"mykey\": \"103\", \"bu\": \"GGG\" }", XContentType.JSON)).actionGet();
                client.index(new IndexRequest("tlqdummy").id("104").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .source("{ \"mykey\": \"104\", \"bu\": \"HHH\" }", XContentType.JSON)).actionGet();
                client.index(new IndexRequest("tlqdummy").id("105").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .source("{ \"mykey\": \"105\", \"bu\": \"HHH\" }", XContentType.JSON)).actionGet();

            }
        }
    }

    // ------------------------------------------------
    // Test single index, remote,coordinating and CCS
    // ------------------------------------------------

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("my_remote:tlqdocuments"), RequestOptions.DEFAULT);
            // 10 docs, all need to have access code 1337    
            Assert.assertEquals(searchResponse.toString(), 10, searchResponse.getHits().getTotalHits().value);
            // fields need to have 1337 access code
            assertAccessCodesMatch(searchResponse.getHits().getHits(), new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(searchResponse.getHits().getHits(), CLUSTER_ALIAS);
        }
    }

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_tlqdummy_no_restrictions() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("my_remote:tlqdummy"), RequestOptions.DEFAULT);
            // 5 docs, role on remote has no restrictions on tlqdummy 
            Assert.assertEquals(searchResponse.toString(), 5, searchResponse.getHits().getTotalHits().value);
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(searchResponse.getHits().getHits(), CLUSTER_ALIAS);
        }
    }

    @Test
    public void testSimpleSearch_Coordinating_AccessCode_1337() throws Exception {
        // Role on coordinating node has different DLS so we expect only documents where bu == AAA
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("tlqdocuments"), RequestOptions.DEFAULT);
            // 3 docs, all need to have bu code AAA  
            // NOTE: IF ISSUE WITH TLQ DLS ON BOTH CLUSTERS IS FIXED, THIS TEST NEEDS TO BE ADAPTED!
            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.getHits().getTotalHits().value);
            assertBuMatches(searchResponse.getHits().getHits(), "AAA");
            assertAllHitsComeFromLocalCluster(searchResponse.getHits().getHits());
        }
    }

    @Test
    public void testSimpleSearch_Remote_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = remote.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse searchResponse = client.search(new SearchRequest("tlqdocuments"), RequestOptions.DEFAULT);
            // 10 docs, all need to have access code 1337    
            Assert.assertEquals(searchResponse.toString(), 10, searchResponse.getHits().getTotalHits().value);
            // fields need to have 1337 access code
            assertAccessCodesMatch(searchResponse.getHits().getHits(), new Integer[] { 1337 });
            assertAllHitsComeFromLocalCluster(searchResponse.getHits().getHits());
        }
    }

    // --------------------------------------------------
    // Test multiple indices, CCS, wildcards and _all queries
    // --------------------------------------------------

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_Multiple_Indices_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchRequest request = new SearchRequest("my_remote:tlqdummy,my_remote:tlqdocuments");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only 10 docs with access code 1337
            // - tlqdummy, must contain all 5 docs, no restrictions on remote
            // no access to user_access_codes must be granted 

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<SearchHit> userAccessCodesHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_Wildcard_Indices_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchRequest request = new SearchRequest("my_remote:tlq*");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            request.source(searchSourceBuilder);

            System.out.println("========================================");

            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain all 5 docs, no restrictions on remote

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);
        }
    }

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_All_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchRequest request = new SearchRequest("my_remote:_all");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain all 5 docs, no restrictions on remote

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<SearchHit> userAccessCodesHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_Coordinating_Wildcard_Only_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchRequest request = new SearchRequest("my_remote:*");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain all 5 docs, no restrictions on remote

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<SearchHit> userAccessCodesHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_Mixed_Coordinating_Remote_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchRequest request = new SearchRequest("my_remote:tlqdocuments,tlqdummy");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain 3 docs, DLS restriction on coordinating apply

            // check all 3 tlqdummy entries present
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 3, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromLocalCluster(tlqdummyHits);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<SearchHit> userAccessCodesHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    // Same as above, but use the minimize_rondtrips flag
    public void testSimpleSearch_Mixed_Coordinating_Remote_Minimize_Roundtrips_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchRequest request = new SearchRequest("my_remote:tlqdocuments,tlqdummy");
            request.setCcsMinimizeRoundtrips(true);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(0).size(100);
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain 3 docs, DLS restriction on coordinating apply

            // check all 3 tlqdummy entries present
            Set<SearchHit> tlqdummyHits = Arrays.asList(searchResponse.getHits().getHits()).stream().filter((h) -> h.getIndex().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 3, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromLocalCluster(tlqdummyHits);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<SearchHit> tlqdocumentHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<SearchHit> userAccessCodesHits = Arrays.asList(searchResponse.getHits().getHits()).stream()
                    .filter((h) -> h.getIndex().equals("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    // ------------
    // Test msearch
    // ------------

    @Test
    public void testMSearch_Coordinating_To_Remote_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            MultiSearchRequest request = new MultiSearchRequest();
            request.add(new SearchRequest("my_remote:tlqdummy"));
            request.add(new SearchRequest("my_remote:tlqdocuments"));
            request.add(new SearchRequest("my_remote:user_access_codes"));
            MultiSearchResponse searchResponse = client.msearch(request, RequestOptions.DEFAULT);

            Item[] responseItems = searchResponse.getResponses();

            // as per API order in response is the same as in the msearch request

            // check all 5 tlqdummy entries present
            List<SearchHit> tlqdummyHits = Arrays.asList(responseItems[0].getResponse().getHits().getHits());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            List<SearchHit> tlqdocumentHits = Arrays.asList(responseItems[1].getResponse().getHits().getHits());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index, just two indices in the response
            Assert.assertTrue(responseItems[2].getResponse() == null);
            Assert.assertTrue(responseItems[2].getFailure() != null);

        }
    }

    @Test
    public void testMSearch_Mixed_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            MultiSearchRequest request = new MultiSearchRequest();
            request.add(new SearchRequest("tlqdummy"));
            request.add(new SearchRequest("my_remote:tlqdocuments"));
            request.add(new SearchRequest("user_access_codes"));
            MultiSearchResponse searchResponse = client.msearch(request, RequestOptions.DEFAULT);

            Item[] responseItems = searchResponse.getResponses();

            // as per API order in response is the same as in the msearch request

            // check all 3 tlqdummy entries present, we have a DLS query on coordinating cluster for this index
            List<SearchHit> tlqdummyHits = Arrays.asList(responseItems[0].getResponse().getHits().getHits());
            Assert.assertEquals(searchResponse.toString(), 3, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromLocalCluster(tlqdummyHits);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            List<SearchHit> tlqdocumentHits = Arrays.asList(responseItems[1].getResponse().getHits().getHits());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index, just two indices in the response
            Assert.assertTrue(responseItems[2].getResponse() == null);
            Assert.assertTrue(responseItems[2].getFailure() != null);

        }
    }

    // ------------------------
    // Test aggregations CCS
    // ------------------------

    @Test
    public void testSimpleAggregation_tlqdocuments_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(AggregationBuilders.terms("buaggregation").field("bu"));
            SearchRequest request = new SearchRequest("my_remote:tlqdocuments").source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            Aggregations aggs = searchResponse.getAggregations();
            Assert.assertNotNull(searchResponse.toString(), aggs);
            Terms agg = aggs.get("buaggregation");
            Assert.assertTrue("Expected aggregation with name 'buaggregation'", agg != null);
            // expect AAA - EEE (FFF does not match) with 2 docs each
            for (String bucketName : new String[] { "AAA", "BBB", "CCC", "DDD", "EEE" }) {
                Bucket bucket = agg.getBucketByKey(bucketName);
                Assert.assertNotNull("Expected bucket " + bucketName + " to be present in agregations", bucket);
                Assert.assertTrue("Expected doc count in bucket " + bucketName + " to be 2", bucket.getDocCount() == 2);
            }
            // expect FFF to be absent
            Assert.assertNull("Expected bucket FFF to be absent", agg.getBucketByKey("FFF"));
        }
    }

    // ------------------------
    // Test scroll with CCS
    // ------------------------

    @Test
    public void testFailureSimpleSearch_Scroll_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest("my_remote:tlqdocuments");
            searchRequest.setCcsMinimizeRoundtrips(false);
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(1);
            searchRequest.source(searchSourceBuilder);

            // Right now we don't support filter level DLS with CCS and scrolling. We need to ensure that this fails to avoid data leakage.

            try {
                client.search(searchRequest, RequestOptions.DEFAULT);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("Filter-level DLS via cross cluster search is not available for scrolling"));
            }
        }
    }

    @Ignore
    @Test
    public void testSimpleSearch_Scroll_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest("my_remote:tlqdocuments");
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(1);
            searchRequest.source(searchSourceBuilder);

            System.out.println("-------------------------------------------------------------");
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("-------------------------------------------------------------");

            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            int totalHits = 0;

            // we scroll one by one
            while (searchHits != null && searchHits.length > 0) {
                System.out.println(Strings.toString(searchResponse.getHits()));
                // for counting the total documents
                totalHits += searchHits.length;
                // only docs with access codes 1337 must be returned
                assertAccessCodesMatch(searchResponse.getHits().getHits(), new Integer[] { 1337 });
                // fetch next
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);

                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            // assume total of 10 documents
            Assert.assertTrue("" + totalHits, totalHits == 10);
        }
    }

}