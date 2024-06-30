/*
 * Copyright 2015-2017 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.legacy;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;

public class AggregationTests extends SingleClusterTest {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @Test
    public void testBasicAggregations() throws Exception {
        final Settings settings = Settings.builder().build();

        setup(settings);
        final RestHelper rh = nonSslRestHelper();

        Client tc = getNodeClient();
        tc.admin().indices().create(new CreateIndexRequest("copysf")).actionGet();
        tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                .actionGet();
        tc.index(new IndexRequest("starfleet").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                .actionGet();
        tc.index(new IndexRequest("starfleet_academy").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                .actionGet();
        tc.index(new IndexRequest("starfleet_library").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                .actionGet();
        tc.index(new IndexRequest("klingonempire").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                .actionGet();
        tc.index(new IndexRequest("public").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.index(new IndexRequest("spock").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("kirk").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("role01_role02").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                .actionGet();

        tc.index(new IndexRequest("xyz").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.admin().indices()
                .aliases(new IndicesAliasesRequest()
                        .addAliasAction(AliasActions.add().indices("starfleet", "starfleet_academy", "starfleet_library").alias("sf")))
                .actionGet();
        tc.admin().indices()
                .aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("klingonempire", "vulcangov").alias("nonsf")))
                .actionGet();
        tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("public").alias("unrestricted")))
                .actionGet();
        tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("xyz").alias("alias1"))).actionGet();


        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK,
                (res = rh.executePostRequest("_search?pretty", "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":40}}}}",
                        encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());

        assertNotContains(res, "*xception*");
        assertNotContains(res, "*erial*");
        assertNotContains(res, "*mpty*");
        assertNotContains(res, "*earchguard*");
        assertContains(res, "*vulcangov*");
        assertContains(res, "*starfleet*");
        assertContains(res, "*klingonempire*");
        assertContains(res, "*xyz*");
        assertContains(res, "*role01_role02*");
        assertContains(res, "*\"failed\" : 0*");

        Assert.assertEquals(HttpStatus.SC_OK,
                (res = rh.executePostRequest("*/_search?pretty", "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":40}}}}",
                        encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());

        assertNotContains(res, "*xception*");
        assertNotContains(res, "*erial*");
        assertNotContains(res, "*mpty*");
        assertNotContains(res, "*earchguard*");
        assertContains(res, "*vulcangov*");
        assertContains(res, "*starfleet*");
        assertContains(res, "*klingonempire*");
        assertContains(res, "*xyz*");
        assertContains(res, "*role01_role02*");
        assertContains(res, "*\"failed\" : 0*");

    }

}
