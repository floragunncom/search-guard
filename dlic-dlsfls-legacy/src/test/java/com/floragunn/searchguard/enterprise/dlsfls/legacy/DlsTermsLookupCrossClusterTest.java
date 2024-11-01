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

import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.MsearchResponse;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.msearch.MultiSearchResponseItem;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.floragunn.searchguard.client.RestHighLevelClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
            .sgConfigSettings("sg_config.dynamic.do_not_fail_on_forbidden", true)//
            .clusterName("remote")//
            .enterpriseModulesEnabled()//
            .roles(new TestSgConfig.Role("sg_dls_tlq_lookup").clusterPermissions("*").indexPermissions("*").on("tlqdummy").indexPermissions("*")

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
            .sgConfigSettings("sg_config.dynamic.do_not_fail_on_forbidden", true)//
            .clusterName("coordinating")//
            .enterpriseModulesEnabled()//
            .roles(new TestSgConfig.Role("sg_dls_tlq_lookup").clusterPermissions("*").indexPermissions("*")
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
                    .source("{ \"mykey\": \"104\", \"bu\": \"HHH\" }", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("tlqdummy").id("105").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{ \"mykey\": \"105\", \"bu\": \"HHH\" }", XContentType.JSON)).actionGet();

        }
    }

    // ------------------------------------------------
    // Test single index, remote,coordinating and CCS
    // ------------------------------------------------

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("my_remote:tlqdocuments");
            // 10 docs, all need to have access code 1337    
            Assert.assertEquals(searchResponse.toString(), 10, searchResponse.hits().total().value());
            // fields need to have 1337 access code
            assertAccessCodesMatch(searchResponse.hits().hits(), new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(searchResponse.hits().hits(), CLUSTER_ALIAS);
        }
    }

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_tlqdummy_no_restrictions() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("my_remote:tlqdummy");
            // 5 docs, role on remote has no restrictions on tlqdummy 
            Assert.assertEquals(searchResponse.toString(), 5, searchResponse.hits().total().value());
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(searchResponse.hits().hits(), CLUSTER_ALIAS);
        }
    }

    @Test
    public void testSimpleSearch_Coordinating_AccessCode_1337() throws Exception {
        // Role on coordinating node has different DLS so we expect only documents where bu == AAA
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("tlqdocuments");
            // 3 docs, all need to have bu code AAA  
            // NOTE: IF ISSUE WITH TLQ DLS ON BOTH CLUSTERS IS FIXED, THIS TEST NEEDS TO BE ADAPTED!
            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.hits().total().value());
            assertBuMatches(searchResponse.hits().hits(), "AAA");
            assertAllHitsComeFromLocalCluster(searchResponse.hits().hits());
        }
    }

    @Test
    public void testSimpleSearch_Remote_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = remote.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("tlqdocuments");
            // 10 docs, all need to have access code 1337    
            Assert.assertEquals(searchResponse.toString(), 10, searchResponse.hits().total().value());
            // fields need to have 1337 access code
            assertAccessCodesMatch(searchResponse.hits().hits(), new Integer[] { 1337 });
            assertAllHitsComeFromLocalCluster(searchResponse.hits().hits());
        }
    }

    // --------------------------------------------------
    // Test multiple indices, CCS, wildcards and _all queries
    // --------------------------------------------------

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_Multiple_Indices_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("my_remote:tlqdummy,my_remote:tlqdocuments",0,100);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only 10 docs with access code 1337
            // - tlqdummy, must contain all 5 docs, no restrictions on remote
            // no access to user_access_codes must be granted 

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<Hit<Map>> tlqdummyHits = searchResponse.hits().hits().stream().filter((h) -> h.index().equals("my_remote:tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<Hit<Map>> tlqdocumentHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().equals("my_remote:tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<Hit<Map>> userAccessCodesHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().equals("my_remote:user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_Wildcard_Indices_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("my_remote:tlq*",0,100);


            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain all 5 docs, no restrictions on remote

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<Hit<Map>> tlqdummyHits = searchResponse.hits().hits().stream().filter((h) -> h.index().equals("my_remote:tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<Hit<Map>> tlqdocumentHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().equals("my_remote:tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);
        }
    }

    @Test
    public void testSimpleSearch_Coordinating_To_Remote_All_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("my_remote:_all",0,100);


            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain all 5 docs, no restrictions on remote

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<Hit<Map>> tlqdummyHits = searchResponse.hits().hits().stream().filter((h) -> h.index().equals("my_remote:tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<Hit<Map>> tlqdocumentHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().equals("my_remote:tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<Hit<Map>> userAccessCodesHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().equals("my_remote:user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_Coordinating_Wildcard_Only_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("my_remote:*",0,100);


            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain all 5 docs, no restrictions on remote

            // check all 5 tlqdummy entries present, index is not protected by DLS
            Set<Hit<Map>> tlqdummyHits = searchResponse.hits().hits().stream().filter((h) -> h.index().equals("my_remote:tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<Hit<Map>> tlqdocumentHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().equals("my_remote:tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<Hit<Map>> userAccessCodesHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().equals("my_remote:user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    public void testSimpleSearch_Mixed_Coordinating_Remote_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("my_remote:tlqdocuments,tlqdummy",0,100);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain 3 docs, DLS restriction on coordinating apply

            // check all 3 tlqdummy entries present
            Set<Hit<Map>> tlqdummyHits = searchResponse.hits().hits().stream().filter((h) -> h.index().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 3, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromLocalCluster(tlqdummyHits);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<Hit<Map>> tlqdocumentHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().equals("my_remote:tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<Hit<Map>> userAccessCodesHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().contains("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    @Test
    // Same as above, but use the minimize_rondtrips flag
    public void testSimpleSearch_Mixed_Coordinating_Remote_Minimize_Roundtrips_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {
            SearchResponse<Map> searchResponse = client.search("my_remote:tlqdocuments,tlqdummy",0,100);

            // assume hits from 2 indices:
            // - tlqdocuments, must contain only docs with access code 1337
            // - tlqdummy, must contain 3 docs, DLS restriction on coordinating apply

            // check all 3 tlqdummy entries present
            Set<Hit<Map>> tlqdummyHits = searchResponse.hits().hits().stream().filter((h) -> h.index().equals("tlqdummy"))
                    .collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 3, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromLocalCluster(tlqdummyHits);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered            
            Set<Hit<Map>> tlqdocumentHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().equals("my_remote:tlqdocuments")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index
            Set<Hit<Map>> userAccessCodesHits = searchResponse.hits().hits().stream()
                    .filter((h) -> h.index().contains("user_access_codes")).collect(Collectors.toSet());
            Assert.assertEquals(searchResponse.toString(), 0, userAccessCodesHits.size());

        }
    }

    // ------------
    // Test msearch
    // ------------

    @Test
    public void testMSearch_Coordinating_To_Remote_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            MsearchRequest.Builder request = new MsearchRequest.Builder();
            request.searches(new RequestItem.Builder().body(b->b).header(h->h.index("my_remote:tlqdummy")).build());
            request.searches(new RequestItem.Builder().body(b->b).header(h->h.index("my_remote:tlqdocuments")).build());
            request.searches(new RequestItem.Builder().body(b->b).header(h->h.index("my_remote:user_access_codes")).build());
            MsearchResponse<Map> searchResponse = client.getJavaClient().msearch(request.build(), Map.class);

            List<MultiSearchResponseItem<Map>> responseItems = searchResponse.responses();

            // as per API order in response is the same as in the msearch request

            // check all 5 tlqdummy entries present
            List<Hit<Map>> tlqdummyHits = responseItems.get(0).result().hits().hits();
            Assert.assertEquals(searchResponse.toString(), 5, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdummyHits, CLUSTER_ALIAS);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            List<Hit<Map>> tlqdocumentHits = responseItems.get(1).result().hits().hits();
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index, just two indices in the response
            Assert.assertTrue(responseItems.get(2).failure() != null);
            Assert.assertTrue(responseItems.get(2).isFailure());

        }
    }

    @Test
    public void testMSearch_Mixed_AccessCodes_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            MsearchRequest.Builder request = new MsearchRequest.Builder();
            request.searches(new RequestItem.Builder().body(b->b).header(h->h.index("tlqdummy")).build());
            request.searches(new RequestItem.Builder().body(b->b).header(h->h.index("my_remote:tlqdocuments")).build());
            request.searches(new RequestItem.Builder().body(b->b).header(h->h.index("user_access_codes")).build());
            MsearchResponse<Map> searchResponse = client.getJavaClient().msearch(request.build(), Map.class);

            List<MultiSearchResponseItem<Map>> responseItems = searchResponse.responses();

            // as per API order in response is the same as in the msearch request

            // check all 3 tlqdummy entries present, we have a DLS query on coordinating cluster for this index
            List<Hit<Map>> tlqdummyHits = responseItems.get(0).result().hits().hits();

            Assert.assertEquals(searchResponse.toString(), 3, tlqdummyHits.size());
            // all hits come from remote cluster
            assertAllHitsComeFromLocalCluster(tlqdummyHits);

            // check 10 hits with code 1337 from tlqdocuments index. All other documents must be filtered
            List<Hit<Map>> tlqdocumentHits = responseItems.get(1).result().hits().hits();
            Assert.assertEquals(searchResponse.toString(), 10, tlqdocumentHits.size());
            assertAccessCodesMatch(tlqdocumentHits, new Integer[] { 1337 });
            // all hits come from remote cluster
            assertAllHitsComeFromCluster(tlqdocumentHits, CLUSTER_ALIAS);

            // check no access to user_access_codes index, just two indices in the response
            Assert.assertTrue(responseItems.get(2).failure() != null);
            Assert.assertTrue(responseItems.get(2).isFailure());

        }
    }

    // ------------------------
    // Test aggregations CCS
    // ------------------------

    @Test
    public void testSimpleAggregation_tlqdocuments_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            co.elastic.clients.elasticsearch.core.SearchRequest searchRequest = new co.elastic.clients.elasticsearch.core.SearchRequest.Builder()
                    .index("my_remote:tlqdocuments")
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
                Assert.assertTrue("Expected doc count in bucket " + bucketName + " to be 2", bucket.get().docCount()== 2);
            }
            // expect FFF to be absent
            Assert.assertFalse("Expected bucket FFF to be absent", DlsTermsLookupAsserts.getBucket(agg, "FFF").isPresent());
        }
    }

    // ------------------------
    // Test scroll with CCS
    // ------------------------

    @Test
    public void testFailureSimpleSearch_Scroll_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            // Right now we don't support filter level DLS with CCS and scrolling. We need to ensure that this fails to avoid data leakage.

            try {
                client.getJavaClient().search(s->s
                        .index("my_remote:tlqdocuments")
                        .size(1).ccsMinimizeRoundtrips(false)
                        .scroll(new Time.Builder().time("1m").build()), Map.class);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("Filter-level DLS via cross cluster search is not available for scrolling"));
            }
        }
    }

    @Test
    @Ignore("TODO why is this ignored?")
    public void testSimpleSearch_Scroll_AccessCode_1337() throws Exception {
        try (RestHighLevelClient client = coordinating.getRestHighLevelClient("tlq_1337", "password")) {

            //System.out.println("-------------------------------------------------------------");
            SearchResponse<Map> searchResponse = client.getJavaClient().search(s->s
                    .index("my_remote:tlqdocuments")
                    .size(1)
                    .scroll(new Time.Builder().time("1m").build()), Map.class);
            //System.out.println("-------------------------------------------------------------");

            String scrollId = searchResponse.scrollId();
            List<Hit<Map>> searchHits = searchResponse.hits().hits();
            int totalHits = 0;

            // we scroll one by one
            while (searchHits != null && searchHits.size() > 0) {
                ////System.out.println(Strings.toString(searchResponse.getHits()));
                // for counting the total documents
                totalHits += searchHits.size();
                // only docs with access codes 1337 must be returned
                assertAccessCodesMatch(searchResponse.hits().hits(), new Integer[] { 1337 });
                // fetch next
                final String finalScrollId = scrollId;
                final ScrollResponse scrollResponse = client.getJavaClient().scroll(s->s.scrollId(finalScrollId), Map.class);

                scrollId = scrollResponse.scrollId();
                searchHits = scrollResponse.hits().hits();
            }

            // assume total of 10 documents
            Assert.assertTrue("" + totalHits, totalHits == 10);
        }
    }

}