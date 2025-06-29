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

package com.floragunn.searchguard.ssl.test;

import java.net.InetAddress;

import org.elasticsearch.common.settings.Settings;
import org.junit.After;

import com.floragunn.searchguard.ssl.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.ssl.test.helper.cluster.ClusterHelper;
import com.floragunn.searchguard.ssl.test.helper.cluster.ClusterInfo;
import com.floragunn.searchguard.ssl.test.helper.rest.RestHelper;
import com.floragunn.searchguard.ssl.util.config.GenericSSLConfig;

public abstract class SingleClusterTest extends AbstractSGUnitTest {

    protected ClusterHelper clusterHelper = new ClusterHelper(
            "utest_n", 0);
    protected ClusterInfo clusterInfo;

    protected void setup(Settings nodeOverride) throws Exception {
        setup(Settings.EMPTY, nodeOverride);
    }

    protected void setup(Settings nodeOverride, ClusterConfiguration clusterConfiguration) throws Exception {
        setup(Settings.EMPTY, nodeOverride, clusterConfiguration);
    }

    protected void setup() throws Exception {
        setup(Settings.EMPTY, Settings.EMPTY);
    }

    protected void setup(Settings initTransportClientSettings, Settings nodeOverride) throws Exception {
        setup(initTransportClientSettings, nodeOverride, ClusterConfiguration.DEFAULT);
    }

    ClusterHelper remoteClusterHelper = null;

    protected void setup(Settings initTransportClientSettings, Settings nodeOverride, ClusterConfiguration clusterConfiguration) throws Exception {
        clusterInfo = clusterHelper.startCluster(minimumSearchGuardSettings(nodeOverride), clusterConfiguration);

    }

    protected void setup(Settings initTransportClientSettings, Settings nodeOverride, boolean initSearchGuardIndex,
            ClusterConfiguration clusterConfiguration, int timeout, Integer nodes) throws Exception {
        clusterInfo = clusterHelper.startCluster(minimumSearchGuardSettings(nodeOverride), clusterConfiguration, null, timeout, nodes);

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

    @After
    public void tearDown() throws Exception {

        if (remoteClusterHelper != null) {
            remoteClusterHelper.stopCluster();
        }

        if (clusterInfo != null) {
            clusterHelper.stopCluster();
        }

    }
}
