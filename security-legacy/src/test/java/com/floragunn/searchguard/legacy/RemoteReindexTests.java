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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.AbstractSGUnitTest;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.ClusterHelper;
import com.floragunn.searchguard.test.helper.cluster.ClusterInfo;

public class RemoteReindexTests extends AbstractSGUnitTest{
    
    private final ClusterHelper cl1 = new ClusterHelper("crl1_n"+num.incrementAndGet()+"_f"+System.getProperty("forkno")+"_t"+System.nanoTime());
    private final ClusterHelper cl2 = new ClusterHelper("crl2_n"+num.incrementAndGet()+"_f"+System.getProperty("forkno")+"_t"+System.nanoTime());
    private ClusterInfo cl1Info;
    private ClusterInfo cl2Info;
    
    private void setupReindex() throws Exception {    
        
        System.setProperty("sg.display_lic_none","true");
        
        cl2Info = cl2.startCluster(minimumSearchGuardSettings(Settings.EMPTY), ClusterConfiguration.DEFAULT);
        initialize(PrivilegedConfigClient.adapt(cl2.nodeClient()));
        
        cl1Info = cl1.startCluster(minimumSearchGuardSettings(crossClusterNodeSettings(cl2Info)), ClusterConfiguration.DEFAULT);
        initialize(PrivilegedConfigClient.adapt(cl1.nodeClient()));
    }
    
    @After
    public void tearDown() throws Exception {
        cl1.stopCluster();
        cl2.stopCluster();
    }
    
    private Settings crossClusterNodeSettings(ClusterInfo remote) {
        Settings.Builder builder = Settings.builder()
                .putList("reindex.remote.whitelist", remote.httpHost+":"+remote.httpPort);
        return builder.build();
    }
    
    //TODO add ssl tests
    //https://github.com/elastic/elasticsearch/issues/27267
    
    @Test
    public void testNonSSLReindex() throws Exception {
        setupReindex();
        
        final String cl1BodyMain = new RestHelper(cl1Info, false, false, getResourceFolder()).executeGetRequest("", encodeBasicHeader("nagilum","nagilum")).getBody();
        Assert.assertTrue(cl1BodyMain.contains("crl1"));
        
        try (Client tc = cl1.nodeClient()) {
            tc.admin().indices().create(new CreateIndexRequest("twutter")).actionGet();
        }
        
        final String cl2BodyMain = new RestHelper(cl2Info, false, false, getResourceFolder()).executeGetRequest("", encodeBasicHeader("nagilum","nagilum")).getBody();
        Assert.assertTrue(cl2BodyMain.contains("crl2"));
        
        try (Client tc = cl2.nodeClient()) {
            tc.index(new IndexRequest("twitter").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                    .source("{\"cluster\": \""+cl1Info.clustername+"\"}", XContentType.JSON)).actionGet();
        }
        
        String reindex = "{"+
            "\"source\": {"+
                "\"remote\": {"+
                "\"host\": \"http://"+cl2Info.httpHost+":"+cl2Info.httpPort+"\","+
                "\"username\": \"nagilum\","+
                "\"password\": \"nagilum\""+
                  "},"+
                    "\"index\": \"twitter\","+
                    "\"size\": 10,"+
                    "\"query\": {"+
                    "\"match\": {"+
                    "\"_index\": \"twitter\""+
                    "}"+
                  "}"+
            "},"+
                "\"dest\": {"+
                "\"index\": \"twutter\""+
            "}"+
        "}";
        
        System.out.println(reindex);
        
        HttpResponse ccs = null;
        
        System.out.println("###################### reindex");
        ccs = new RestHelper(cl1Info, false, false, getResourceFolder()).executePostRequest("_reindex?pretty", reindex, encodeBasicHeader("nagilum","nagilum"));
        System.out.println(ccs.getBody());
        Assert.assertEquals(ccs.getBody(), HttpStatus.SC_OK, ccs.getStatusCode());
        Assert.assertTrue(ccs.getBody(), ccs.getBody().contains("created\" : 1"));
    }
}
