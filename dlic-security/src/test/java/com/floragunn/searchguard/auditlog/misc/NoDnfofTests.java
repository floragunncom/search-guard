/*
 * Copyright 2017-2021 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.auditlog.misc;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class NoDnfofTests extends SingleClusterTest {

    // Moved over from MultitenancyTests because this really has nothing to do with multi tenancy
    
    @Override
    protected String getResourceFolder() {
        return "multitenancy";
    }
    
    @Test
    public void testNoDnfof() throws Exception {

        final Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_ROLES_MAPPING_RESOLUTION, "BOTH").build();

        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_nodnfof.yml"), settings);
        final RestHelper rh = nonSslRestHelper();

        try (Client tc = getInternalTransportClient()) {
            tc.admin().indices().create(new CreateIndexRequest("copysf")).actionGet();

            tc.index(new IndexRequest("indexa").type("doc").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":\"indexa\"}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("indexb").type("doc").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":\"indexb\"}",
                    XContentType.JSON)).actionGet();

            tc.index(new IndexRequest("vulcangov").type("kolinahr").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("starfleet").type("ships").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("starfleet_academy").type("students").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("starfleet_library").type("public").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("klingonempire").type("ships").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();
            tc.index(
                    new IndexRequest("public").type("legends").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                    .actionGet();

            tc.index(new IndexRequest("spock").type("type01").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                    .actionGet();
            tc.index(new IndexRequest("kirk").type("type01").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON))
                    .actionGet();
            tc.index(new IndexRequest("role01_role02").type("type01").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();

            tc.admin().indices()
                    .aliases(new IndicesAliasesRequest()
                            .addAliasAction(AliasActions.add().indices("starfleet", "starfleet_academy", "starfleet_library").alias("sf")))
                    .actionGet();
            tc.admin().indices()
                    .aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("klingonempire", "vulcangov").alias("nonsf")))
                    .actionGet();
            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("public").alias("unrestricted")))
                    .actionGet();

        }

        HttpResponse resc;
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("indexa,indexb/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("indexa,indexb/_search?pretty", encodeBasicHeader("user_b", "user_b"))).getStatusCode());
        System.out.println(resc.getBody());

        String msearchBody = "{\"index\":\"indexa\", \"type\":\"doc\", \"ignore_unavailable\": true}" + System.lineSeparator()
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator()
                + "{\"index\":\"indexb\", \"type\":\"doc\", \"ignore_unavailable\": true}" + System.lineSeparator()
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator();
        System.out.println("#### msearch a");
        resc = rh.executePostRequest("_msearch?pretty", msearchBody, encodeBasicHeader("user_a", "user_a"));
        Assert.assertEquals(200, resc.getStatusCode());
        System.out.println(resc.getBody());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("indexa"));
        Assert.assertFalse(resc.getBody(), resc.getBody().contains("indexb"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("permission"));

        System.out.println("#### msearch b");
        resc = rh.executePostRequest("_msearch?pretty", msearchBody, encodeBasicHeader("user_b", "user_b"));
        Assert.assertEquals(200, resc.getStatusCode());
        System.out.println(resc.getBody());
        Assert.assertFalse(resc.getBody(), resc.getBody().contains("indexa"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("indexb"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("permission"));

        msearchBody = "{\"index\":\"indexc\", \"type\":\"doc\", \"ignore_unavailable\": true}" + System.lineSeparator()
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator()
                + "{\"index\":\"indexd\", \"type\":\"doc\", \"ignore_unavailable\": true}" + System.lineSeparator()
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator();

        System.out.println("#### msearch b2");
        resc = rh.executePostRequest("_msearch?pretty", msearchBody, encodeBasicHeader("user_b", "user_b"));
        System.out.println(resc.getBody());
        Assert.assertEquals(200, resc.getStatusCode());
        Assert.assertFalse(resc.getBody(), resc.getBody().contains("indexc"));
        Assert.assertFalse(resc.getBody(), resc.getBody().contains("indexd"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("permission"));
        int count = resc.getBody().split("\"status\" : 403").length;
        Assert.assertEquals(3, count);

        String mgetBody = "{" + "\"docs\" : [" + "{" + "\"_index\" : \"indexa\"," + "\"_type\" : \"doc\"," + "\"_id\" : \"0\"" + " }," + " {"
                + "\"_index\" : \"indexb\"," + " \"_type\" : \"doc\"," + " \"_id\" : \"0\"" + "}" + "]" + "}";

        resc = rh.executePostRequest("_mget?pretty", mgetBody, encodeBasicHeader("user_b", "user_b"));
        Assert.assertEquals(200, resc.getStatusCode());
        Assert.assertFalse(resc.getBody(), resc.getBody().contains("\"content\" : \"indexa\""));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("indexb"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("permission"));

        mgetBody = "{" + "\"docs\" : [" + "{" + "\"_index\" : \"indexx\"," + "\"_type\" : \"doc\"," + "\"_id\" : \"0\"" + " }," + " {"
                + "\"_index\" : \"indexy\"," + " \"_type\" : \"doc\"," + " \"_id\" : \"0\"" + "}" + "]" + "}";

        resc = rh.executePostRequest("_mget?pretty", mgetBody, encodeBasicHeader("user_b", "user_b"));
        Assert.assertEquals(200, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("exception"));
        count = resc.getBody().split("root_cause").length;
        Assert.assertEquals(3, count);

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("index*/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());

        Assert.assertEquals(HttpStatus.SC_OK,
                (resc = rh.executeGetRequest("indexa/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("indexb/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("*/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("_all/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("notexists/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_NOT_FOUND,
                (resc = rh.executeGetRequest("indexanbh,indexabb*/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                (resc = rh.executeGetRequest("starfleet/_search?pretty", encodeBasicHeader("user_a", "user_a"))).getStatusCode());
        System.out.println(resc.getBody());

        Assert.assertEquals(HttpStatus.SC_OK,
                (resc = rh.executeGetRequest("starfleet/_search?pretty", encodeBasicHeader("worf", "worf"))).getStatusCode());
        System.out.println(resc.getBody());

    }
}
