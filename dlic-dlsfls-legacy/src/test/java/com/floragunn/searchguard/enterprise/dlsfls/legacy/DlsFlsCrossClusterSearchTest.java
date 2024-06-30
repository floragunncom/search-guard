/*
 * Copyright 2018 by floragunn GmbH - All rights reserved
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.searchguard.legacy.test.AbstractSGUnitTest;
import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.ClusterHelper;
import com.floragunn.searchguard.test.helper.cluster.ClusterInfo;

@RunWith(Parameterized.class)
public class DlsFlsCrossClusterSearchTest extends AbstractSGUnitTest{
    
    private final ClusterHelper cl1 = new ClusterHelper("crl1_n", 0);
    private final ClusterHelper cl2 = new ClusterHelper("crl2_n", 1);
    private ClusterInfo cl1Info;
    private ClusterInfo cl2Info;
    
    //default is true
    @Parameter
    public boolean ccsMinimizeRoundtrips;
    
    @Parameters
    public static Object[] parameters() {
        return new Object[] { Boolean.FALSE, Boolean.TRUE };
    }
    
    @Override
    protected String getResourceFolder() {
        return "dlsfls_legacy";
    }
    
    private void setupCcs(String remoteRoles) throws Exception {    
        
        System.setProperty("sg.display_lic_none","true");
        
        cl2Info = cl2.startCluster(minimumSearchGuardSettings(Settings.EMPTY), ClusterConfiguration.DEFAULT);
        initialize(PrivilegedConfigClient.adapt(cl2.nodeClient()), new DynamicSgConfig().setSgRoles(remoteRoles));
        //System.out.println("### cl2 complete ###");
        
        //cl1 is coordinating
        cl1Info = cl1.startCluster(minimumSearchGuardSettings(crossClusterNodeSettings(cl2Info)), ClusterConfiguration.DEFAULT);
        //System.out.println("### cl1 start ###");
        initialize(PrivilegedConfigClient.adapt(cl1.nodeClient()), new DynamicSgConfig().setSgRoles("sg_roles_983.yml"));
        //System.out.println("### cl1 initialized ###");
    }
    
    @After
    public void tearDown() throws Exception {
        cl1.stopCluster();
        cl2.stopCluster();
    }
    
    private Settings crossClusterNodeSettings(ClusterInfo remote) {
        Settings.Builder builder = Settings.builder()
                .putList("cluster.remote.cross_cluster_two.seeds", remote.nodeHost+":"+remote.nodePort);
        return builder.build();
    }
    
    @Test
    public void testCcs() throws Exception {
        setupCcs("sg_roles_983.yml");
        
        Client tc1 = cl1.nodeClient();
        tc1.index(new IndexRequest("twitter").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl1Info.clustername+"\"}", XContentType.JSON)).actionGet();

        Client tc2 = cl2.nodeClient();
        tc2.index(new IndexRequest("twutter").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl2Info.clustername+"\"}", XContentType.JSON)).actionGet();
        tc2.index(new IndexRequest("humanresources").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl2Info.clustername+"\","+
                          "\"Designation\": \"CEO\","+
                          "\"FirstName\": \"__fn__"+cl2Info.clustername+"\","+
                          "\"LastName\": \"lastname0\","+
                          "\"Salary\": \"salary0\","+
                          "\"SecretFiled\": \"secret0\","+
                          "\"AnotherSecredField\": \"anothersecret0\","+
                          "\"XXX\": \"xxx0\""
                        + "}", XContentType.JSON)).actionGet();

        tc2.index(new IndexRequest("humanresources").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1")
                .source("{\"cluster\": \""+cl2Info.clustername+"\","+
                          "\"Designation\": \"someoneelse\","+
                          "\"FirstName\": \"__fn__"+cl2Info.clustername+"\","+
                          "\"LastName\": \"lastname1\","+
                          "\"Salary\": \"salary1\","+
                          "\"SecretFiled\": \"secret1\","+
                          "\"AnotherSecredField\": \"anothersecret1\","+
                          "\"XXX\": \"xxx1\""
                        + "}", XContentType.JSON)).actionGet();
            

        HttpResponse ccs = null;
        
        //System.out.println("###################### query 1");
        //on coordinating cluster
        ccs = new RestHelper(cl1Info, false, false, getResourceFolder()).executeGetRequest("cross_cluster_two:humanresources/_search?pretty&ccs_minimize_roundtrips="+ccsMinimizeRoundtrips, encodeBasicHeader("human_resources_trainee", "password"));
        //System.out.println(ccs.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, ccs.getStatusCode());
        Assert.assertFalse(ccs.getBody(), ccs.getBody().contains("crl1"));
        Assert.assertTrue(ccs.getBody(), ccs.getBody().contains("crl2"));
        Assert.assertTrue(ccs.getBody(), ccs.getBody().contains("\"value\" : 1,\n      \"relation"));
        Assert.assertFalse(ccs.getBody(), ccs.getBody().contains("CEO"));
        Assert.assertFalse(ccs.getBody(), ccs.getBody().contains("salary0"));
        Assert.assertFalse(ccs.getBody(), ccs.getBody().contains("secret0"));
        Assert.assertTrue(ccs.getBody(), ccs.getBody().contains("someoneelse"));
        Assert.assertTrue(ccs.getBody(), ccs.getBody().contains("__fn__crl2"));
        Assert.assertTrue(ccs.getBody(), ccs.getBody().contains("salary1"));
        Assert.assertFalse(ccs.getBody(), ccs.getBody().contains("secret1"));
        Assert.assertFalse(ccs.getBody(), ccs.getBody().contains("AnotherSecredField"));
        Assert.assertFalse(ccs.getBody(), ccs.getBody().contains("xxx1"));
    }
    
    @Test
    public void testCcsDifferentConfig() throws Exception {
        setupCcs("sg_roles_ccs2.yml");
        
        Client tc1 = cl1.nodeClient();
        tc1.index(new IndexRequest("twitter").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl1Info.clustername+"\"}", XContentType.JSON)).actionGet();

        Client tc2 = cl2.nodeClient();
        tc2.index(new IndexRequest("twutter").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl2Info.clustername+"\"}", XContentType.JSON)).actionGet();
        tc2.index(new IndexRequest("humanresources").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl2Info.clustername+"\","+
                          "\"Designation\": \"CEO\","+
                          "\"FirstName\": \"__fn__"+cl2Info.clustername+"\","+
                          "\"LastName\": \"lastname0\","+
                          "\"Salary\": \"salary0\","+
                          "\"SecretFiled\": \"secret0\","+
                          "\"AnotherSecredField\": \"anothersecret0\","+
                          "\"XXX\": \"xxx0\""
                        + "}", XContentType.JSON)).actionGet();

        tc2.index(new IndexRequest("humanresources").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1")
                .source("{\"cluster\": \""+cl2Info.clustername+"\","+
                          "\"Designation\": \"someoneelse\","+
                          "\"FirstName\": \"__fn__"+cl2Info.clustername+"\","+
                          "\"LastName\": \"lastname1\","+
                          "\"Salary\": \"salary1\","+
                          "\"SecretFiled\": \"secret1\","+
                          "\"AnotherSecredField\": \"anothersecret1\","+
                          "\"XXX\": \"xxx1\""
                        + "}", XContentType.JSON)).actionGet();
            

        HttpResponse ccs = null;
        
        //System.out.println("###################### query 1");
        //on coordinating cluster
        ccs = new RestHelper(cl1Info, false, false, getResourceFolder()).executeGetRequest("cross_cluster_two:humanresources/_search?pretty&ccs_minimize_roundtrips="+ccsMinimizeRoundtrips, encodeBasicHeader("human_resources_trainee", "password"));
        //System.out.println(ccs.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, ccs.getStatusCode());
        Assert.assertFalse(ccs.getBody().contains("crl1"));
        Assert.assertTrue(ccs.getBody().contains("crl2"));
        Assert.assertTrue(ccs.getBody().contains("\"value\" : 1,\n      \"relation"));
        Assert.assertTrue(ccs.getBody().contains("XXX"));
        Assert.assertTrue(ccs.getBody().contains("xxx"));
        Assert.assertFalse(ccs.getBody().contains("Designation"));
        Assert.assertFalse(ccs.getBody().contains("salary1"));
        Assert.assertTrue(ccs.getBody().contains("salary0"));
        Assert.assertFalse(ccs.getBody().contains("secret0"));
        Assert.assertTrue(ccs.getBody().contains("__fn__crl2"));
        Assert.assertFalse(ccs.getBody().contains("secret1"));
        Assert.assertFalse(ccs.getBody().contains("AnotherSecredField"));
    }
    
    @Test
    public void testCcsDifferentConfigBoth() throws Exception {
        setupCcs("sg_roles_ccs2.yml");
        
        Client tc1 = cl1.nodeClient();
        tc1.index(new IndexRequest("twitter").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl1Info.clustername+"\"}", XContentType.JSON)).actionGet();

        tc1.index(new IndexRequest("humanresources").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl1Info.clustername+"\","+
                          "\"Designation\": \"CEO\","+
                          "\"FirstName\": \"__fn__"+cl1Info.clustername+"\","+
                          "\"LastName\": \"lastname0\","+
                          "\"Salary\": \"salary0\","+
                          "\"SecretFiled\": \"secret3\","+
                          "\"AnotherSecredField\": \"anothersecret3\","+
                          "\"XXX\": \"xxx0\""
                        + "}", XContentType.JSON)).actionGet();

        tc1.index(new IndexRequest("humanresources").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1")
                .source("{\"cluster\": \""+cl1Info.clustername+"\","+
                          "\"Designation\": \"someoneelse\","+
                          "\"FirstName\": \"__fn__"+cl1Info.clustername+"\","+
                          "\"LastName\": \"lastname1\","+
                          "\"Salary\": \"salary1\","+
                          "\"SecretFiled\": \"secret4\","+
                          "\"AnotherSecredField\": \"anothersecret4\","+
                          "\"XXX\": \"xxx1\""
                        + "}", XContentType.JSON)).actionGet();

        Client tc2 = cl2.nodeClient();
        tc2.index(new IndexRequest("twutter").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl2Info.clustername+"\"}", XContentType.JSON)).actionGet();
        tc2.index(new IndexRequest("humanresources").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("{\"cluster\": \""+cl2Info.clustername+"\","+
                          "\"Designation\": \"CEO\","+
                          "\"FirstName\": \"__fn__"+cl2Info.clustername+"\","+
                          "\"LastName\": \"lastname0\","+
                          "\"Salary\": \"salary0\","+
                          "\"SecretFiled\": \"secret0\","+
                          "\"AnotherSecredField\": \"anothersecret0\","+
                          "\"XXX\": \"xxx0\""
                        + "}", XContentType.JSON)).actionGet();

        tc2.index(new IndexRequest("humanresources").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1")
                .source("{\"cluster\": \""+cl2Info.clustername+"\","+
                          "\"Designation\": \"someoneelse\","+
                          "\"FirstName\": \"__fn__"+cl2Info.clustername+"\","+
                          "\"LastName\": \"lastname1\","+
                          "\"Salary\": \"salary1\","+
                          "\"SecretFiled\": \"secret1\","+
                          "\"AnotherSecredField\": \"anothersecret1\","+
                          "\"XXX\": \"xxx1\""
                        + "}", XContentType.JSON)).actionGet();
            

        HttpResponse ccs = null;
        
        //System.out.println("###################### query 1");
        //on coordinating cluster
        ccs = new RestHelper(cl1Info, false, false, getResourceFolder()).executeGetRequest("cross_cluster_two:humanresources,humanresources/_search?pretty&ccs_minimize_roundtrips="+ccsMinimizeRoundtrips, encodeBasicHeader("human_resources_trainee", "password"));
        //System.out.println(ccs.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, ccs.getStatusCode());
        Assert.assertTrue(ccs.getBody().contains("crl1"));
        Assert.assertTrue(ccs.getBody().contains("crl2"));
        Assert.assertTrue(ccs.getBody().contains("\"value\" : 2,\n      \"relation"));
        Assert.assertTrue(ccs.getBody().contains("XXX"));
        Assert.assertTrue(ccs.getBody().contains("xxx"));
        Assert.assertTrue(ccs.getBody().contains("Designation"));
        Assert.assertTrue(ccs.getBody().contains("salary1"));
        Assert.assertTrue(ccs.getBody().contains("salary0"));
        Assert.assertFalse(ccs.getBody().contains("secret0"));
        Assert.assertTrue(ccs.getBody().contains("__fn__crl2"));
        Assert.assertTrue(ccs.getBody().contains("__fn__crl1"));
        Assert.assertFalse(ccs.getBody().contains("secret1"));
        Assert.assertFalse(ccs.getBody().contains("AnotherSecredField"));
        Assert.assertTrue(ccs.getBody().contains("someoneelse"));
    }
}
