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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;

public class IndexIntegrationTests extends SingleClusterTest {

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @Test
    public void testComposite() throws Exception {
    
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_composite_config.yml").setSgRoles("sg_roles_composite.yml"), Settings.EMPTY, true);
        final RestHelper rh = nonSslRestHelper();
    
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("starfleet").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("klingonempire").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("public").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        String msearchBody = 
                "{\"index\":\"starfleet\", \"ignore_unavailable\": true}"+System.lineSeparator()+
                "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}"+System.lineSeparator()+
                "{\"index\":\"klingonempire\", \"ignore_unavailable\": true}"+System.lineSeparator()+
                "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}"+System.lineSeparator()+
                "{\"index\":\"public\", \"ignore_unavailable\": true}"+System.lineSeparator()+
                "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}"+System.lineSeparator();
                         
            
        HttpResponse resc = rh.executePostRequest("_msearch", msearchBody, encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(200, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("\"_index\":\"klingonempire\""));
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("hits"));        
    }
    
    @Test
    public void testBulkShards() throws Exception {
    
        setup(Settings.EMPTY, new DynamicSgConfig().setSgRoles("sg_roles_bs.yml"), Settings.EMPTY, true);
        final RestHelper rh = nonSslRestHelper();
        
        Client tc = getPrivilegedInternalNodeClient();
        //create indices and mapping upfront
        tc.index(new IndexRequest("test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"field2\":\"init\"}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("lorem").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"field2\":\"init\"}", XContentType.JSON)).actionGet();

        String bulkBody = 
        "{ \"index\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value1\" }" +System.lineSeparator()+
        "{ \"index\" : { \"_index\" : \"test\",  \"_id\" : \"2\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
        "{ \"index\" : { \"_index\" : \"test\", \"_id\" : \"3\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
        "{ \"index\" : { \"_index\" : \"test\",  \"_id\" : \"4\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
        "{ \"index\" : { \"_index\" : \"test\", \"_id\" : \"5\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
        "{ \"index\" : { \"_index\" : \"lorem\",  \"_id\" : \"1\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
        "{ \"index\" : { \"_index\" : \"lorem\",  \"_id\" : \"2\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
        "{ \"index\" : { \"_index\" : \"lorem\",  \"_id\" : \"3\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
        "{ \"index\" : { \"_index\" : \"lorem\", \"_id\" : \"4\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
        "{ \"index\" : { \"_index\" : \"lorem\", \"_id\" : \"5\" } }"+System.lineSeparator()+
        "{ \"field2\" : \"value2\" }"+System.lineSeparator()+
        "{ \"delete\" : { \"_index\" : \"lorem\", \"_id\" : \"5\" } }"+System.lineSeparator();
       
        //System.out.println("############ _bulk");
        HttpResponse res = rh.executePostRequest("_bulk?refresh=true&pretty=true", bulkBody, encodeBasicHeader("worf", "worf"));
        //System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());  
        Assert.assertTrue(res.getBody().contains("\"errors\" : true"));
        Assert.assertTrue(res.getBody().contains("\"status\" : 201"));
        Assert.assertTrue(res.getBody().contains("Insufficient permissions"));
        
        //System.out.println("############ check shards");
        //System.out.println(rh.executeGetRequest("_cat/shards?v", encodeBasicHeader("nagilum", "nagilum")));

        
    }

    @Test
    public void testCreateIndex() throws Exception {
    
        setup();
        RestHelper rh = nonSslRestHelper();
              
        HttpResponse res;
        Assert.assertEquals("Unable to create index 'nag'", HttpStatus.SC_OK, rh.executePutRequest("nag1", null, encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals("Unable to create index 'starfleet_library'", HttpStatus.SC_OK, rh.executePutRequest("starfleet_library", null, encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        
        clusterHelper.waitForCluster(ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(10), clusterInfo.numNodes);
        
        Assert.assertEquals("Unable to close index 'starfleet_library'", HttpStatus.SC_OK, rh.executePostRequest("starfleet_library/_close", null, encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        
        Assert.assertEquals("Unable to open index 'starfleet_library'", HttpStatus.SC_OK, (res = rh.executePostRequest("starfleet_library/_open", null, encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());
        Assert.assertTrue("open index 'starfleet_library' not acknowledged", res.getBody().contains("acknowledged"));
        Assert.assertFalse("open index 'starfleet_library' not acknowledged", res.getBody().contains("false"));
        
        clusterHelper.waitForCluster(ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(10), clusterInfo.numNodes);
        
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePutRequest("public", null, encodeBasicHeader("spock", "spock")).getStatusCode());
        
        
    }
    
    @Test
    public void testIndices() throws Exception {
    
        setup();
    
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("nopermindex").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.index(new IndexRequest("logstash-1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash-2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash-3").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash-4").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd", Locale.ENGLISH);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        String date = sdf.format(new Date());
        DocWriteResponse indexResponse = tc.index(new IndexRequest("logstash-"+date).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        //System.out.println("## " + new Date() + " " + date + " " + Strings.toString(indexResponse));

        RestHelper rh = nonSslRestHelper();
        
        HttpResponse res = null;
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/logstash-1/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
    
        //nonexistent index with permissions
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, (res = rh.executeGetRequest("/logstash-nonex/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
    
        //nonexistent and existent index with permissions
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, (res = rh.executeGetRequest("/logstash-nonex,logstash-1/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
        
        //existent index without permissions
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executeGetRequest("/nopermindex/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());

        //nonexistent index without permissions
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executeGetRequest("/does-not-exist-and-no-perm/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
    
        //existent index with permissions
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/logstash-1/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());

        //nonexistent index with failed login
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, (res = rh.executeGetRequest("/logstash-nonex/_search", encodeBasicHeader("nouser", "nosuer"))).getStatusCode());   
        
        //nonexistent index with no login
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, (res = rh.executeGetRequest("/logstash-nonex/_search")).getStatusCode());   
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/_all/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
    
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/*/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());        
    
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executeGetRequest("/nopermindex,logstash-1,nonexist/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
        
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executeGetRequest("/logstash-1,nonexist/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
        
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executeGetRequest("/nonexist/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());

        res = rh.executeGetRequest("/%3Clogstash-%7Bnow%2Fd%7D%3E/_search", encodeBasicHeader("logstash", "nagilum"));
        Assert.assertEquals(res.getBody(), HttpStatus.SC_OK, res.getStatusCode());
    
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executeGetRequest("/%3Cnonex-%7Bnow%2Fd%7D%3E/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/%3Clogstash-%7Bnow%2Fd%7D%3E,logstash-*/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/%3Clogstash-%7Bnow%2Fd%7D%3E,logstash-1/_search", encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
        
        Assert.assertEquals(HttpStatus.SC_CREATED, (res = rh.executePutRequest("/logstash-b/_doc/1", "{}",encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
    
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePutRequest("/%3Clogstash-cnew-%7Bnow%2Fd%7D%3E", "{}",encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
        
        Assert.assertEquals(HttpStatus.SC_CREATED, (res = rh.executePutRequest("/%3Clogstash-new-%7Bnow%2Fd%7D%3E/_doc/1", "{}",encodeBasicHeader("logstash", "nagilum"))).getStatusCode());
    
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/_cat/indices?v" ,encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());
    
        //System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("logstash-b"));
        Assert.assertTrue(res.getBody().contains("logstash-new-20"));
        Assert.assertTrue(res.getBody().contains("logstash-cnew-20"));
        Assert.assertFalse(res.getBody().contains("<"));
    }
    
    @Test
    public void testAliases() throws Exception {

        final Settings settings = Settings.builder()
                .put("searchguard.roles_mapping_resolution", "BOTH")
                .build();

        setup(settings);
    
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("nopermindex").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.index(new IndexRequest("logstash-1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash-2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash-3").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash-4").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash-5").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash-del").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("logstash-del-ok").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        String date = new SimpleDateFormat("YYYY.MM.dd").format(new Date());
        tc.index(new IndexRequest("logstash-"+date).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("nopermindex").alias("nopermalias"))).actionGet();
        tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("searchguard").alias("mysgi"))).actionGet();

        RestHelper rh = nonSslRestHelper();
        
        HttpResponse res = null;
        

        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executePostRequest("/mysgi/_doc/sg", "{}",encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executeGetRequest("/mysgi/_search?pretty", encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());

        //Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/mysgi/_search?pretty", encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());
        //assertContains(res, "*\"hits\" : {*\"value\" : 0,*\"hits\" : [ ]*");

        //System.out.println("#### add alias to allowed index");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executePutRequest("/logstash-1/_alias/alog1", "",encodeBasicHeader("aliasmngt", "nagilum"))).getStatusCode());

        //System.out.println("#### add alias to not existing (no perm)");
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executePutRequest("/nonexitent/_alias/alnp", "",encodeBasicHeader("aliasmngt", "nagilum"))).getStatusCode());
        
        //System.out.println("#### add alias to not existing (with perm)");
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, (res = rh.executePutRequest("/logstash-nonex/_alias/alnp", "",encodeBasicHeader("aliasmngt", "nagilum"))).getStatusCode());
        
        //System.out.println("#### add alias to not allowed index");
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executePutRequest("/nopermindex/_alias/alnp", "",encodeBasicHeader("aliasmngt", "nagilum"))).getStatusCode());

        String aliasRemoveIndex = "{"+
            "\"actions\" : ["+
               "{ \"add\":  { \"index\": \"logstash-del-ok\", \"alias\": \"logstash-del\" } },"+
               "{ \"remove_index\": { \"index\": \"logstash-del\" } }  "+
            "]"+
        "}";
        
        //System.out.println("#### remove_index");
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executePostRequest("/_aliases", aliasRemoveIndex,encodeBasicHeader("aliasmngt", "nagilum"))).getStatusCode());

        
        //System.out.println("#### get alias for permitted index");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/logstash-1/_alias/alog1", encodeBasicHeader("aliasmngt", "nagilum"))).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/_alias/alog1", encodeBasicHeader("aliasmngt", "nagilum"))).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, (res = rh.executeGetRequest("/_alias/nopermalias", encodeBasicHeader("aliasmngt", "nagilum"))).getStatusCode());
        
        String alias =
        "{"+
          "\"aliases\": {"+
            "\"alias1\": {}"+
          "}"+
        "}";
        
        
        //System.out.println("#### create alias along with index");
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executePutRequest("/beats-withalias", alias,encodeBasicHeader("aliasmngt", "nagilum"))).getStatusCode());        
    }

    @Test
    @Ignore("Cross-cluster calls are not supported in this context but remote indices were requested")
    public void testCCSIndexResolve() throws Exception {

        setup();
        final RestHelper rh = nonSslRestHelper();

        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest(".abc-6").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        //ccsresolv has perm for ?abc*
        HttpResponse res = rh.executeGetRequest("ggg:.abc-6,.abc-6/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
        
        res = rh.executeGetRequest("/*:.abc-6,.abc-6/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        //TODO: Change for 25.0 to be forbidden (possible bug in ES regarding ccs wildcard)
    }

    @Test
    @Ignore("TODO why is this ignored?")
    public void testCCSIndexResolve2() throws Exception {
        
        setup();
        final RestHelper rh = nonSslRestHelper();

        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest(".abc").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("xyz").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":2}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("noperm").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":3}", XContentType.JSON)).actionGet();
        
        HttpResponse res = rh.executeGetRequest("/*:.abc,.abc/_search", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody(),res.getBody().contains("\"content\":1"));
        
        res = rh.executeGetRequest("/ba*bcuzh/_search", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertTrue(res.getBody(),res.getBody().contains("\"content\":12"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        
        res = rh.executeGetRequest("/*:.abc/_search", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertTrue(res.getBody(),res.getBody().contains("\"content\":1"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        
        res = rh.executeGetRequest("/*:xyz,xyz/_search", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody(),res.getBody().contains("\"content\":2"));
        
        //res = rh.executeGetRequest("/*noexist/_search", encodeBasicHeader("nagilum", "nagilum"));
        //Assert.assertEquals(HttpStatus.SC_NOT_FOUND, res.getStatusCode()); 
        
        res = rh.executeGetRequest("/*:.abc/_search", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode()); 
        Assert.assertTrue(res.getBody(),res.getBody().contains("\"content\":1"));
        
        res = rh.executeGetRequest("/*:xyz/_search", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode()); 
        Assert.assertTrue(res.getBody(),res.getBody().contains("\"content\":2"));
   
        res = rh.executeGetRequest("/.abc/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());  
        res = rh.executeGetRequest("/xyz/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());  
        res = rh.executeGetRequest("/*:.abc,.abc/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());  
        res = rh.executeGetRequest("/*:xyz,xyz/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("/*:.abc/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode()); 
        res = rh.executeGetRequest("/*:xyz/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode()); 
        res = rh.executeGetRequest("/*:noperm/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("/*:noperm/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        //System.out.println(res.getBody());
        res = rh.executeGetRequest("/*:noexists/_search", encodeBasicHeader("ccsresolv", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        //System.out.println(res.getBody());
    }
    
    @Test
    //https://forum.search-guard.com/t/querying-missing-index-causes-security-exception/1531/11?u=hsaly
    public void testIndexResolveIndicesAlias() throws Exception {

        setup(Settings.EMPTY, new DynamicSgConfig(), Settings.EMPTY, true);
        final RestHelper rh = nonSslRestHelper();

        Client tc = getPrivilegedInternalNodeClient();
        //create indices and mapping upfront
        tc.index(new IndexRequest("foo-index").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"field2\":\"init\"}", XContentType.JSON)).actionGet();
        tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("foo-index").alias("foo-alias"))).actionGet();
        tc.admin().indices().delete(new DeleteIndexRequest("foo-index")).actionGet();

        HttpResponse resc = rh.executeGetRequest("/_cat/aliases", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertFalse(resc.getBody().contains("foo"));

        resc = rh.executeGetRequest("/foo-alias/_search", encodeBasicHeader("foo_index", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, resc.getStatusCode());
        
        resc = rh.executeGetRequest("/foo-index/_search", encodeBasicHeader("foo_index", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, resc.getStatusCode());
        
        resc = rh.executeGetRequest("/foo-alias/_search", encodeBasicHeader("foo_all", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, resc.getStatusCode());
        
    }
}
