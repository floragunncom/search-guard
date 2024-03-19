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

import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;


public class DlsTermsLookupTestTlqDisabled {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().resources("dlsfls_legacy").nodeSettings("searchguard.dls.mode", "lucene_level")
            .enterpriseModulesEnabled().build();

    @BeforeClass
    public static void setupTestData() {
        try (Client client = cluster.getInternalNodeClient()) {

            client.index(new IndexRequest("deals").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"amount\": 10, \"acodes\": [6,7]}",
                    XContentType.JSON)).actionGet();
            client.index(new IndexRequest("deals").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"amount\": 1500, \"acodes\": [1]}",
                    XContentType.JSON)).actionGet();

            client.index(new IndexRequest("users").id("sg_dls_lookup_user1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"acode\": [1,2,4]}", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("users").id("sg_dls_lookup_user2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"acode\": [2,3]}", XContentType.JSON)).actionGet();
        }
    }

    @Test
    public void testDlsWithTermsLookupGetTLQDisabled() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("sg_dls_lookup_user1", "password")) {

            GenericRestClient.HttpResponse res = client.get("/deals/_doc/0?pretty");

            Assert.assertEquals(res.getBody(), HttpStatus.SC_BAD_REQUEST, res.getStatusCode());

            res = client.get("/deals/_doc/1?pretty");

            Assert.assertEquals(res.getBody(), HttpStatus.SC_BAD_REQUEST, res.getStatusCode());

            Assert.assertTrue(res.getBody(), res.getBody().contains("async actions are left after rewrite"));
        }
    }
}