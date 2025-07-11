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

package com.floragunn.searchguard.legacy.test;

import java.net.InetAddress;

import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;

import com.floragunn.searchguard.ssl.util.config.GenericSSLConfig;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.ClusterHelper;
import com.floragunn.searchguard.test.helper.cluster.ClusterInfo;
import org.junit.Assert;

public abstract class SingleClusterTest extends AbstractSGUnitTest {
        
    protected ClusterHelper clusterHelper = new ClusterHelper("utest_n", 0);
    protected ClusterInfo clusterInfo;
    
    protected void setup(Settings nodeOverride) throws Exception {    
        setup(Settings.EMPTY, new DynamicSgConfig(), nodeOverride, true);
    }
    
    protected void setup(Settings nodeOverride, ClusterConfiguration clusterConfiguration) throws Exception {    
        setup(Settings.EMPTY, new DynamicSgConfig(), nodeOverride, true, clusterConfiguration);
    }
    
    protected void setup() throws Exception {    
        setup(Settings.EMPTY, new DynamicSgConfig(), Settings.EMPTY, true);
    }
    
    protected void setup(Settings initTransportClientSettings, DynamicSgConfig dynamicSgSettings, Settings nodeOverride) throws Exception {    
        setup(initTransportClientSettings, dynamicSgSettings, nodeOverride, true);
    }
    
    protected void setup(Settings initTransportClientSettings, DynamicSgConfig dynamicSgSettings, Settings nodeOverride, boolean initSearchGuardIndex) throws Exception {    
        setup(initTransportClientSettings, dynamicSgSettings, nodeOverride, initSearchGuardIndex, ClusterConfiguration.DEFAULT);
    }

    protected void setupWithHttps(DynamicSgConfig dynamicSgSettings, Settings nodeOverride, boolean initSearchGuardIndex) throws Exception {

        final String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

        final Settings settings = Settings.builder()
                .put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath(prefix +"node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath(prefix +"truststore.jks"))
                .put(nodeOverride)
                .build();

        setup(Settings.EMPTY, dynamicSgSettings, settings, initSearchGuardIndex, ClusterConfiguration.DEFAULT);
    }




    
    ClusterHelper remoteClusterHelper = null;
    private Settings ccs(Settings nodeOverride) throws Exception {
        if(withRemoteCluster) {
        remoteClusterHelper = new ClusterHelper("crl2_n", 1);
        ClusterInfo cl2Info = remoteClusterHelper.startCluster(minimumSearchGuardSettings(Settings.EMPTY), ClusterConfiguration.SINGLENODE);
        Settings.Builder builder = Settings.builder()
                .put(nodeOverride)
                .putList("cluster.remote.cross_cluster_two.seeds", cl2Info.nodeHost+":"+cl2Info.nodePort);
        return builder.build();
        } else {
            return nodeOverride;
        }
    }
    
    
    protected void setup(Settings initTransportClientSettings, DynamicSgConfig dynamicSgSettings, Settings nodeOverride, boolean initSearchGuardIndex, ClusterConfiguration clusterConfiguration) throws Exception {
        Assert.assertEquals(Settings.EMPTY, initTransportClientSettings);
        clusterInfo = clusterHelper.startCluster(minimumSearchGuardSettings(ccs(nodeOverride)), clusterConfiguration);
        if(initSearchGuardIndex && dynamicSgSettings != null) {
            initialize(getPrivilegedInternalNodeClient(), dynamicSgSettings);
        }
    }
    
    protected void setup(Settings initTransportClientSettings, DynamicSgConfig dynamicSgSettings, Settings nodeOverride
            , boolean initSearchGuardIndex, ClusterConfiguration clusterConfiguration, int timeout, Integer nodes) throws Exception {
        Assert.assertEquals(Settings.EMPTY, initTransportClientSettings);
        clusterInfo = clusterHelper.startCluster(minimumSearchGuardSettings(ccs(nodeOverride)), clusterConfiguration, timeout, nodes);
        if(initSearchGuardIndex) {
            initialize(getPrivilegedInternalNodeClient(), dynamicSgSettings);
        }
    }
    
    protected void setupSslOnlyMode(Settings nodeOverride) throws Exception {    
        clusterInfo = clusterHelper.startCluster(minimumSearchGuardSettingsSslOnly(nodeOverride), ClusterConfiguration.DEFAULT);
    }
    
    protected void setupSslOnlyMode(Settings nodeOverride, ClusterConfiguration clusterConfiguration) throws Exception {    
        clusterInfo = clusterHelper.startCluster(minimumSearchGuardSettingsSslOnly(nodeOverride), clusterConfiguration);
    }
    
    protected RestHelper restHelper() {
        return new RestHelper(clusterInfo, getResourceFolder());
    }

    protected RestHelper restHelper(InetAddress bindAddress) {
        RestHelper result = new RestHelper(clusterInfo, getResourceFolder());
        result.setLocalAddress(bindAddress);
        return result;
    }
    
    protected RestHelper restHelper(GenericSSLConfig sslConfig) {
        RestHelper result = new RestHelper(clusterInfo, getResourceFolder());
        result.setSslConfig(sslConfig);
        return result;
    }

    protected RestHelper restHelper(int nodeIndex) {
        RestHelper result = new RestHelper(clusterInfo, getResourceFolder());
        result.setNodeIndex(nodeIndex);
        return result;
    }

    protected RestHelper restHelper(int nodeIndex, GenericSSLConfig sslConfig) {
        RestHelper result = new RestHelper(clusterInfo, getResourceFolder());
        result.setNodeIndex(nodeIndex);
        result.setSslConfig(sslConfig);
        return result;
    }

    protected RestHelper nonSslRestHelper() {
        return new RestHelper(clusterInfo, false, false, getResourceFolder());
    }
    
    protected RestHelper nonSslRestHelper(InetAddress bindAddress) {
        RestHelper result = new RestHelper(clusterInfo, false, false, getResourceFolder());
        result.setLocalAddress(bindAddress);
        return result;
    }
    
    protected Client getNodeClient() {
        return clusterHelper.nodeClient();
    }
    

    public Client getPrivilegedInternalNodeClient() {
        return PrivilegedConfigClient.adapt(getNodeClient());
    }       
    @After
    public void tearDown() throws Exception {
        
        if(remoteClusterHelper != null) {
            remoteClusterHelper.stopCluster();
        }
        
        if(clusterInfo != null) {
            clusterHelper.stopCluster();
        }
        
    }
}
