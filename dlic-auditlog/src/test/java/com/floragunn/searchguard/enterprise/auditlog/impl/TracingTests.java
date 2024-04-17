/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.impl;

import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;

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
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchsupport.junit.AsyncAssert;

public class TracingTests extends SingleClusterTest {

    @Override
    protected String getResourceFolder() {
        return "auditlog";
    }

    @Test
    public void testHTTPTrace() throws Exception {

        final Settings settings = Settings.builder()
                .put(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, "debug")
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS, "*")
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, "*")
                .put("searchguard.audit.resolve_bulk_requests", true)
                .put("searchguard.audit.config.log4j.logger_name", "sg_action_trace")
                .put("searchguard.audit.config.log4j.level", "TRACE")
                .build();

        setup(Settings.EMPTY, new DynamicSgConfig(), settings, true, ClusterConfiguration.DEFAULT);

        try (Client tc = getPrivilegedInternalNodeClient()) {

            for(int i=0; i<50;i++) {
                tc.index(new IndexRequest("a").id(i+"").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":"+i+"}", XContentType.JSON)).actionGet();
                tc.index(new IndexRequest("c").id(i+"").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":"+i+"}", XContentType.JSON)).actionGet();
            }
        }




        RestHelper rh = nonSslRestHelper();
        //System.out.println("############ check shards");
        //System.out.println(rh.executeGetRequest("_cat/shards?v", encodeBasicHeader("admin", "admin")));

        //System.out.println("############ check shards");
        //System.out.println(rh.executeGetRequest("_searchguard/authinfo",encodeBasicHeader("admin", "admin")));

        //System.out.println("############ _bulk");
        String bulkBody =
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value1\" }" +System.lineSeparator()+
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }"+System.lineSeparator()+
                "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
                "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }"+System.lineSeparator();

        //System.out.println(rh.executePostRequest("_bulk?refresh=true", bulkBody, encodeBasicHeader("admin", "admin")));

        //System.out.println("############ _bulk");
        bulkBody =
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value1\" }" +System.lineSeparator()+
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }"+System.lineSeparator()+
                "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
                "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }"+System.lineSeparator();

        //System.out.println(rh.executePostRequest("_bulk?refresh=true", bulkBody, encodeBasicHeader("admin", "admin")));


        //System.out.println("############ cat indices");
        //cluster:monitor/state
        //cluster:monitor/health
        //indices:monitor/stats
        //System.out.println(rh.executeGetRequest("_cat/indices", encodeBasicHeader("admin", "admin")));


        //System.out.println("############ _search");
        //indices:data/read/search
        //System.out.println(rh.executeGetRequest("_search", encodeBasicHeader("admin", "admin")));

        //System.out.println("############ get 1");
        //indices:data/read/get
        //System.out.println(rh.executeGetRequest("a/b/1", encodeBasicHeader("admin", "admin")));
        //System.out.println("############ get 5");
        //System.out.println(rh.executeGetRequest("a/b/5", encodeBasicHeader("admin", "admin")));
        //System.out.println("############ get 17");
        //System.out.println(rh.executeGetRequest("a/b/17", encodeBasicHeader("admin", "admin")));

        //System.out.println("############ index (+create index)");
        //indices:data/write/index
        //indices:data/write/bulk
        //indices:admin/create
        //indices:data/write/bulk[s]
        //System.out.println(rh.executePostRequest("u/b/1?refresh=true", "{}",encodeBasicHeader("admin", "admin")));

        //System.out.println("############ index only");
        //indices:data/write/index
        //indices:data/write/bulk
        //indices:admin/create
        //indices:data/write/bulk[s]
        //System.out.println(rh.executePostRequest("u/b/2?refresh=true", "{}",encodeBasicHeader("admin", "admin")));

        //System.out.println("############ index updates");
        //indices:data/write/index
        //indices:data/write/bulk
        //indices:admin/create
        //indices:data/write/bulk[s]
        //System.out.println(rh.executePostRequest("u/b/2?refresh=true", "{\"n\":1, \"m\":1}",encodeBasicHeader("admin", "admin")));
        //System.out.println(rh.executePostRequest("u/b/2?refresh=true", "{\"n\":2, \"m\":1, \"z\":1}",encodeBasicHeader("admin", "admin")));
        //System.out.println(rh.executePostRequest("u/b/2?refresh=true", "{\"n\":2, \"z\":4}",encodeBasicHeader("admin", "admin")));
        //System.out.println(rh.executePostRequest("u/b/2?refresh=true", "{\"n\":5, \"z\":5}",encodeBasicHeader("admin", "admin")));
        //System.out.println(rh.executePostRequest("u/b/2?refresh=true", "{\"n\":5}",encodeBasicHeader("admin", "admin")));
        //System.out.println("############ update");
        //indices:data/write/index
        //indices:data/write/bulk
        //indices:admin/create
        //indices:data/write/bulk[s]
        //System.out.println(rh.executePostRequest("u/b/2/_update?refresh=true", "{\"doc\" : {\"a\":1}}",encodeBasicHeader("admin", "admin")));

        //System.out.println("############ delete");
        //indices:data/write/index
        //indices:data/write/bulk
        //indices:admin/create
        //indices:data/write/bulk[s]
        //System.out.println(rh.executeDeleteRequest("u/b/2?refresh=true",encodeBasicHeader("admin", "admin")));

        //System.out.println("############ reindex");
        String reindex =
        "{"+
        "  \"source\": {"+
        "    \"index\": \"a\""+
        "  },"+
        "  \"dest\": {"+
        "    \"index\": \"new_a\""+
        "  }"+
        "}";

        //System.out.println(rh.executePostRequest("_reindex", reindex, encodeBasicHeader("admin", "admin")));


        //System.out.println("############ msearch");
        String msearchBody =
                "{\"index\":\"a\", \"type\":\"b\", \"ignore_unavailable\": true}"+System.lineSeparator()+
                "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}"+System.lineSeparator()+
                "{\"index\":\"a\", \"type\":\"b\", \"ignore_unavailable\": true}"+System.lineSeparator()+
                "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}"+System.lineSeparator()+
                "{\"index\":\"public\", \"ignore_unavailable\": true}"+System.lineSeparator()+
                "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}"+System.lineSeparator();


        //System.out.println(rh.executePostRequest("_msearch", msearchBody, encodeBasicHeader("admin", "admin")));

        //System.out.println("############ mget");
        String mgetBody = "{"+
                "\"docs\" : ["+
                    "{"+
                         "\"_index\" : \"a\","+
                        "\"_type\" : \"b\","+
                        "\"_id\" : \"1\""+
                   " },"+
                   " {"+
                       "\"_index\" : \"a\","+
                       " \"_type\" : \"b\","+
                       " \"_id\" : \"12\""+
                    "},"+
                    " {"+
                    "\"_index\" : \"a\","+
                    " \"_type\" : \"b\","+
                    " \"_id\" : \"13\""+
                 "},"+" {"+
                 "\"_index\" : \"a\","+
                 " \"_type\" : \"b\","+
                 " \"_id\" : \"14\""+
              "}"+
                "]"+
            "}";

        //System.out.println(rh.executePostRequest("_mget?refresh=true", mgetBody, encodeBasicHeader("admin", "admin")));

        //System.out.println("############ delete by query");
        String dbqBody = "{"+
        ""+
        "  \"query\": { "+
        "    \"match\": {"+
        "      \"content\": 12"+
        "    }"+
        "  }"+
        "}";

        //System.out.println(rh.executePostRequest("a/b/_delete_by_query", dbqBody, encodeBasicHeader("admin", "admin")));
    }

    @Test
    public void testHTTPSingle() throws Exception {

        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace();

            }
        });

    final Settings settings = Settings.builder()
            .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+".worf", "knuddel","nonexists")
            .build();
    setup(settings);
    final RestHelper rh = nonSslRestHelper();

        try (Client tc = getPrivilegedInternalNodeClient()) {
            tc.admin().indices().create(new CreateIndexRequest("copysf")).actionGet();
            tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("starfleet").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("starfleet_academy").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("starfleet_library").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("klingonempire").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("public").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

            tc.index(new IndexRequest("spock").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("kirk").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("role01_role02").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("starfleet","starfleet_academy","starfleet_library").alias("sf"))).actionGet();
            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("klingonempire","vulcangov").alias("nonsf"))).actionGet();
            tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("public").alias("unrestricted"))).actionGet();

        }
        
        AsyncAssert.awaitAssert("_search is OK", () -> 
            rh.executeGetRequest("_search", encodeBasicHeader("admin", "admin")).getStatusCode() == 200, Duration.ofSeconds(10));

        //System.out.println("############ _bulk");
        String bulkBody =
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value1\" }" +System.lineSeparator()+
                "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }"+System.lineSeparator()+
                "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
                "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }"+System.lineSeparator()+
                "{ \"index\" : { \"_index\" : \"myindex\", \"_type\" : \"myindex\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value1\" }" +System.lineSeparator()+
                "{ \"index\" : { \"_index\" : \"myindex\", \"_type\" : \"myindex\", \"_id\" : \"1\" } }"+System.lineSeparator()+
                "{ \"field1\" : \"value1\" }" +System.lineSeparator();

        //System.out.println(rh.executePostRequest("_bulk?refresh=true", bulkBody, encodeBasicHeader("admin", "admin")).getBody());
        //System.out.println("############ _end");
    }

    @Test
    public void testSearchScroll() throws Exception {

        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace();

            }
        });

    final Settings settings = Settings.builder()
            .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+".worf", "knuddel","nonexists")
            .build();
    setup(settings);
    final RestHelper rh = nonSslRestHelper();

        try (Client tc = getPrivilegedInternalNodeClient()) {
            for(int i=0; i<3; i++)
            tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        }


        //System.out.println("########search");
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executeGetRequest("vulcangov/_search?scroll=1m&pretty=true", encodeBasicHeader("admin", "admin"))).getStatusCode());

        //System.out.println(res.getBody());
        int start = res.getBody().indexOf("_scroll_id") + 15;
        String scrollid = res.getBody().substring(start, res.getBody().indexOf("\"", start+1));
        //System.out.println(scrollid);
        //System.out.println("########search scroll");
        Assert.assertEquals(HttpStatus.SC_OK, (res=rh.executePostRequest("/_search/scroll?pretty=true", "{\"scroll_id\" : \""+scrollid+"\"}", encodeBasicHeader("admin", "admin"))).getStatusCode());


        //System.out.println("########search done");


    }

    @Test
    public void testAdvancedMapping() throws Exception {
        Settings settings = Settings.builder()
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS, "*")
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, "*")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, "debug").build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings, true, ClusterConfiguration.DEFAULT);

        try (Client tc = getPrivilegedInternalNodeClient()) {
            tc.admin().indices().create(new CreateIndexRequest("myindex1")
            .mapping(FileHelper.loadFile("mapping1.json"))).actionGet();
            tc.admin().indices().create(new CreateIndexRequest("myindex2")
            .mapping(FileHelper.loadFile("mapping2.json"))).actionGet();
            tc.admin().indices().create(new CreateIndexRequest("myindex3")
            .mapping(FileHelper.loadFile("mapping3.json"))).actionGet();
            tc.admin().indices().create(new CreateIndexRequest("myindex4")
            .mapping(FileHelper.loadFile("mapping4.json"))).actionGet();
        }

        RestHelper rh = nonSslRestHelper();
        //System.out.println("############ write into mapping 1");
        String data1 = FileHelper.loadFile("auditlog/data1.json");
        String data2 = FileHelper.loadFile("auditlog/data1mod.json");
        //System.out.println(rh.executePutRequest("myindex1/mytype1/1?refresh", data1, encodeBasicHeader("admin", "admin")));
        //System.out.println(rh.executePutRequest("myindex1/mytype1/1?refresh", data1, encodeBasicHeader("admin", "admin")));
        //System.out.println("############ write into mapping diffing");
        //System.out.println(rh.executePutRequest("myindex1/mytype1/1?refresh", data2, encodeBasicHeader("admin", "admin")));

        //System.out.println("############ write into mapping 2");
        //System.out.println(rh.executePutRequest("myindex2/mytype2/2?refresh", data1, encodeBasicHeader("admin", "admin")));
        //System.out.println(rh.executePutRequest("myindex2/mytype2/2?refresh", data2, encodeBasicHeader("admin", "admin")));

        //System.out.println("############ write into mapping 3");
        String parent = FileHelper.loadFile("auditlog/data2.json");
        String child = FileHelper.loadFile("auditlog/data3.json");
        //System.out.println(rh.executePutRequest("myindex3/mytype3/1?refresh", parent, encodeBasicHeader("admin", "admin")));
        //System.out.println(rh.executePutRequest("myindex3/mytype3/2?routing=1&refresh", child, encodeBasicHeader("admin", "admin")));

        //System.out.println("############ write into mapping 4");
        //System.out.println(rh.executePutRequest("myindex4/mytype4/1?refresh", parent, encodeBasicHeader("admin", "admin")));
        //System.out.println(rh.executePutRequest("myindex4/mytype4/2?routing=1&refresh", child, encodeBasicHeader("admin", "admin")));

        //System.out.println("############ get");
        //System.out.println(rh.executeGetRequest("myindex1/mytype1/1?pretty=true&_source=true&_source_include=*.id&_source_exclude=entities&stored_fields=tags,counter", encodeBasicHeader("admin", "admin")).getBody());

        //System.out.println("############ search");
        //System.out.println(rh.executeGetRequest("myindex1/_search", encodeBasicHeader("admin", "admin")).getStatusCode());

    }
    
    @Test
    public void testImmutableIndex() throws Exception {
        Settings settings = Settings.builder()
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_IMMUTABLE_INDICES, "myindex1")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, "debug").build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings, true, ClusterConfiguration.DEFAULT);

        try (Client tc = getPrivilegedInternalNodeClient()) {
            tc.admin().indices().create(new CreateIndexRequest("myindex1")
            .mapping(FileHelper.loadFile("mapping1.json"))).actionGet();
            tc.admin().indices().create(new CreateIndexRequest("myindex2")
            .mapping(FileHelper.loadFile("mapping1.json"))).actionGet();
        }

        RestHelper rh = nonSslRestHelper();
        //System.out.println("############ immutable 1");
        String data1 = FileHelper.loadFile("auditlog/data1.json");
        String data2 = FileHelper.loadFile("auditlog/data1mod.json");
        HttpResponse res = rh.executePutRequest("myindex1/_doc/1?refresh", data1, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(201, res.getStatusCode());
        res = rh.executePutRequest("myindex1/_doc/1?refresh", data2, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(403, res.getStatusCode());
        res = rh.executeDeleteRequest("myindex1/_doc/1?refresh", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(403, res.getStatusCode());
        res = rh.executeGetRequest("myindex1/_doc/1", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(200, res.getStatusCode());
        Assert.assertFalse(res.getBody().contains("city"));
        Assert.assertTrue(res.getBody().contains("\"found\":true,"));
        
        //System.out.println("############ immutable 2");
        res = rh.executePutRequest("myindex2/_doc/1?refresh", data1, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(201, res.getStatusCode());
        res = rh.executePutRequest("myindex2/_doc/1?refresh", data2, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(200, res.getStatusCode());
        res = rh.executeGetRequest("myindex2/_doc/1", encodeBasicHeader("admin", "admin"));
        Assert.assertTrue(res.getBody().contains("city"));
        res = rh.executeDeleteRequest("myindex2/_doc/1?refresh", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(200, res.getStatusCode());
        res = rh.executeGetRequest("myindex2/_doc/1", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(404, res.getStatusCode());
    }

}
