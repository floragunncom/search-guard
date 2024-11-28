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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.PluginAwareNode;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchsupport.junit.AsyncAssert;

public class SlowIntegrationTests extends SingleClusterTest {

    public static final TimeValue MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(40);

    @Ignore("TODO why is this ignored?")
    @Test
    public void testCustomInterclusterRequestEvaluator() throws Exception {
        
        final Settings settings = Settings.builder()
                .put(ConfigConstants.SG_INTERCLUSTER_REQUEST_EVALUATOR_CLASS, "com.floragunn.searchguard.AlwaysFalseInterClusterRequestEvaluator")
                .put("discovery.initial_state_timeout","8s")
                .build();
        setup(Settings.EMPTY, null, settings, false,ClusterConfiguration.DEFAULT ,5,1);
        Assert.assertEquals(1, clusterHelper.nodeClient().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet().getNumberOfNodes());
        Assert.assertEquals(ClusterHealthStatus.GREEN, clusterHelper.nodeClient().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet().getStatus());
    }

    @SuppressWarnings("resource")
    @Test
    public void testNodeClientAllowedWithServerCertificate() throws Exception {
        setup();
        Assert.assertEquals(clusterInfo.numNodes, clusterHelper.nodeClient().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet().getNumberOfNodes());
        Assert.assertEquals(ClusterHealthStatus.GREEN, clusterHelper.nodeClient().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet().getStatus());

        Path tmp = Files.createTempDirectory("sgunit");
        tmp.toFile().deleteOnExit();

        final Settings tcSettings = Settings.builder()
                .put(minimumSearchGuardSettings(Settings.EMPTY).get(0))
                .put("cluster.name", clusterInfo.clustername)
                .put("node.roles", "")
                .put("path.home", tmp)
                .put("node.name", "transportclient")
                .put("discovery.initial_state_timeout","18s")
                .putList("cluster.initial_master_nodes", clusterInfo.tcpMasterPortsOnly)
                .putList("discovery.seed_hosts",         clusterInfo.tcpMasterPortsOnly)
                .build();
    
        log.debug("Start node client");
        
        try (Node node = new PluginAwareNode(false, tcSettings).start()) {
            Assert.assertFalse(node.client().admin().cluster().health(new ClusterHealthRequest().waitForNodes(String.valueOf(clusterInfo.numNodes+1))).actionGet().isTimedOut());
            Assert.assertEquals(clusterInfo.numNodes+1, node.client().admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet().getNodes().size());    
        }
    }
    
    @SuppressWarnings("resource")
    @Test
    public void testNodeClientDisallowedWithNonServerCertificate() throws Exception {
        setup();
        Assert.assertEquals(clusterInfo.numNodes, clusterHelper.nodeClient().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet().getNumberOfNodes());
        Assert.assertEquals(ClusterHealthStatus.GREEN, clusterHelper.nodeClient().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet().getStatus());

        Path tmp = Files.createTempDirectory("sgunit");
        tmp.toFile().deleteOnExit();
        
        final Settings tcSettings = Settings.builder()
                .put(minimumSearchGuardSettings(Settings.EMPTY).get(0))
                .put("cluster.name", clusterInfo.clustername)
                .put("node.roles", "")
                .put("path.home", tmp)
                .put("node.name", "transportclient")
                .put("discovery.initial_state_timeout","8s")
                .putList("cluster.initial_master_nodes", clusterInfo.tcpMasterPortsOnly)
                .putList("discovery.seed_hosts",         clusterInfo.tcpMasterPortsOnly)
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("kirk-keystore.jks"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS,"kirk")
                .build();
    
        log.debug("Start node client");

        try (Node node = new PluginAwareNode(false, tcSettings).start()) {
            AsyncAssert.awaitAssert("Node has started",
                    () -> node.client().admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet().getNodes().size() == 1,
                    Duration.ofSeconds(10));
        }
    }
    
    @SuppressWarnings("resource")
    @Test
    public void testNodeClientDisallowedWithNonServerCertificate2() throws Exception {
        setup();
        Assert.assertEquals(clusterInfo.numNodes, clusterHelper.nodeClient().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet().getNumberOfNodes());
        Assert.assertEquals(ClusterHealthStatus.GREEN, clusterHelper.nodeClient().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet().getStatus());

        Path tmp = Files.createTempDirectory("sgunit");
        tmp.toFile().deleteOnExit();

        final Settings tcSettings = Settings.builder()
                .put(minimumSearchGuardSettings(Settings.EMPTY).get(0))
                .put("cluster.name", clusterInfo.clustername)
                .put("node.roles", "")
                .put("path.home", tmp)
                .put("node.name", "transportclient")
                .put("discovery.initial_state_timeout","8s")
                .putList("cluster.initial_master_nodes", clusterInfo.tcpMasterPortsOnly)
                .putList("discovery.seed_hosts",         clusterInfo.tcpMasterPortsOnly)
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("spock-keystore.jks"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS,"spock")
                .build();
    
        log.debug("Start node client");
        
        try (Node node = new PluginAwareNode(false, tcSettings).start()) {
            AsyncAssert.awaitAssert("Node has started",
                    () -> node.client().admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet().getNodes().size() == 1,
                    Duration.ofSeconds(10));
        } 
    }

}
