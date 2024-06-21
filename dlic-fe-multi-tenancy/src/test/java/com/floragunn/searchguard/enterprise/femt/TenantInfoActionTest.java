/*
 * Copyright 2017-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class TenantInfoActionTest {

    private final static TestSgConfig.User KIBANA_SERVER = new TestSgConfig.User("kibanaserver").roles("SGS_KIBANA_SERVER");

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().resources("multitenancy").user(KIBANA_SERVER)
            .enterpriseModulesEnabled().users(KIBANA_SERVER).embedded().build();

    @Test
    public void testTenantInfo() throws Exception {

        /*
         
            [admin_1, praxisrw, abcdef_2_2, kltentro, praxisro, kltentrw]
            admin_1==.kibana_-1139640511_admin1
            praxisrw==.kibana_-1386441176_praxisrw
            abcdef_2_2==.kibana_-634608247_abcdef22
            kltentro==.kibana_-2014056171_kltentro
            praxisro==.kibana_-1386441184_praxisro
            kltentrw==.kibana_-2014056163_kltentrw
         
         */

        try (Client tc = cluster.getInternalNodeClient()) {

            tc.index(new IndexRequest(".kibana-6").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":2}", XContentType.JSON))
                    .actionGet();
            tc.index(new IndexRequest(".kibana_-1139640511_admin1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-1386441176_praxisrw").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-634608247_abcdef22").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-12345_123456").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON))
                    .actionGet();
            tc.index(
                    new IndexRequest(".kibana2_-12345_123456").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON))
                    .actionGet();
            tc.index(new IndexRequest(".kibana_9876_xxx_ccc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON))
                    .actionGet();
            tc.index(new IndexRequest(".kibana_fff_eee").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON))
                    .actionGet();

            tc.index(new IndexRequest("esb-prod-5").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":5}", XContentType.JSON))
                    .actionGet();

            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices(".kibana-6").alias(".kibana")))
                    .actionGet();
            tc.admin().indices().aliases(
                    new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("esb-prod-5").alias(".kibana_-2014056163_kltentrw")))
                    .actionGet();
            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("esb-prod-5").alias("esb-alias-5")))
                    .actionGet();

        }

        try (GenericRestClient client = cluster.getRestClient(KIBANA_SERVER)) {
            GenericRestClient.HttpResponse res = client.get("/_searchguard/tenantinfo?pretty");
            System.out.println(res.getBody());
            Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
            Assert.assertTrue(res.getBody().contains("\".kibana_-1139640511_admin1\" : \"admin_1\""));
            Assert.assertTrue(res.getBody().contains("\".kibana_-1386441176_praxisrw\" : \"praxisrw\""));
            Assert.assertTrue(res.getBody().contains(".kibana_-2014056163_kltentrw\" : \"kltentrw\""));
            Assert.assertTrue(res.getBody().contains("\".kibana_-634608247_abcdef22\" : \"abcdef_2_2\""));
            Assert.assertTrue(res.getBody().contains("\".kibana_-12345_123456\" : \"__private__\""));
            Assert.assertFalse(res.getBody().contains(".kibana-6"));
            Assert.assertFalse(res.getBody().contains("esb-"));
            Assert.assertFalse(res.getBody().contains("xxx"));
            Assert.assertFalse(res.getBody().contains(".kibana2"));
        }

    }
}
