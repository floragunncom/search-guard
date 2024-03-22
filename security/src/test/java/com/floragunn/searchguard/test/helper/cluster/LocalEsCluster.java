/*
 * Copyright 2015-2024 floragunn GmbH
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

package com.floragunn.searchguard.test.helper.cluster;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration.NodeSettings;
import com.floragunn.searchguard.test.helper.network.PortAllocator;

public abstract class LocalEsCluster {
    private static final Logger log = LogManager.getLogger(LocalEsCluster.class);

    protected final String clusterName;
    protected final ClusterConfiguration clusterConfiguration;
    protected final NodeSettingsSupplier nodeSettingsSupplier;
    protected final TestCertificates testCertificates;

    protected final File clusterHomeDir;
    protected final Random random = new Random();

    protected List<String> seedHosts;
    protected List<String> initialMasterHosts;
    protected int retry = 0;
    protected boolean started;
    protected boolean portCollisionDetected = false;

    public LocalEsCluster(String clusterName, ClusterConfiguration clusterConfiguration, NodeSettingsSupplier nodeSettingsSupplier,
            TestCertificates testCertificates) {
        this.clusterName = clusterName;
        this.clusterConfiguration = clusterConfiguration;
        this.nodeSettingsSupplier = nodeSettingsSupplier;
        this.clusterHomeDir = FileHelper.createTempDirectory("sg_local_cluster_" + clusterName);
        this.clusterHomeDir.deleteOnExit();
        this.testCertificates = testCertificates;
    }

    public abstract boolean isStarted();

    public abstract void destroy();

    public abstract List<? extends Node> getAllNodes();

    public abstract List<? extends Node> clientNodes();

    public abstract List<? extends Node> dataNodes();

    public abstract List<? extends Node> masterNodes();

    public abstract void waitForGreenCluster() throws Exception;

    protected abstract CompletableFuture<? extends Node> startNode(NodeSettings nodeSettings, int httpPort, int transportPort);

    protected abstract void destroyNodes();

    public Node getNodeByName(String name) {
        return getAllNodes().stream().filter(node -> node.getNodeName().equals(name)).findAny().orElseThrow(() -> new RuntimeException(
                "No such node with name: " + name + "; available: " + getAllNodes().stream().map(Node::getNodeName).collect(Collectors.toList())));
    }

    public void start() throws Exception {
        log.info("Starting {}", clusterName);

        portCollisionDetected = false;

        int forkNumber = getUnitTestForkNumber();
        int masterNodeCount = clusterConfiguration.getMasterNodes();
        int nonMasterNodeCount = clusterConfiguration.getDataNodes() + clusterConfiguration.getClientNodes();

        SortedSet<Integer> masterNodeTransportPorts = PortAllocator.TCP.allocate(clusterName, Math.max(masterNodeCount, 4),
                5000 + forkNumber * 1000 + 300);
        SortedSet<Integer> masterNodeHttpPorts = PortAllocator.TCP.allocate(clusterName, masterNodeCount, 5000 + forkNumber * 1000 + 200);

        this.seedHosts = toHostList(masterNodeTransportPorts);
        this.initialMasterHosts = toHostList(masterNodeTransportPorts.stream().limit(masterNodeCount).collect(Collectors.toSet()));

        started = true;

        CompletableFuture<Void> masterNodeFuture = startNodes(clusterConfiguration.getMasterNodeSettings(), masterNodeTransportPorts,
                masterNodeHttpPorts);

        SortedSet<Integer> nonMasterNodeTransportPorts = PortAllocator.TCP.allocate(clusterName, nonMasterNodeCount, 5000 + forkNumber * 1000 + 310);
        SortedSet<Integer> nonMasterNodeHttpPorts = PortAllocator.TCP.allocate(clusterName, nonMasterNodeCount, 5000 + forkNumber * 1000 + 210);

        CompletableFuture<Void> nonMasterNodeFuture = startNodes(clusterConfiguration.getNonMasterNodeSettings(), nonMasterNodeTransportPorts,
                nonMasterNodeHttpPorts);

        try {
            CompletableFuture.allOf(masterNodeFuture, nonMasterNodeFuture).get();
        } catch (ExecutionException e) {
            if (portCollisionDetected) {
                log.info("Detected port collision. Retrying.");

                retry();
                return;
            } else {
                throw new Exception("Error while starting cluster " + this, e.getCause());
            }
        }

        log.info("Startup finished. Waiting for GREEN");

        waitForGreenCluster();

        log.info("Started: {}", this);

    }

    protected CompletableFuture<Void> startNodes(List<NodeSettings> nodeSettingList, SortedSet<Integer> transportPorts,
            SortedSet<Integer> httpPorts) {

        Iterator<Integer> transportPortIterator = transportPorts.iterator();
        Iterator<Integer> httpPortIterator = httpPorts.iterator();
        List<CompletableFuture<? extends Node>> futures = new ArrayList<>();

        for (NodeSettings nodeSettings : nodeSettingList) {
            futures.add(startNode(nodeSettings, httpPortIterator.next(), transportPortIterator.next()));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    public void stop() {

        log.info("Stopping " + this);
        
        for (Node node : clientNodes()) {
            node.stop();
        }

        for (Node node : dataNodes()) {
            node.stop();
        }

        for (Node node : masterNodes()) {
            node.stop();
        }
        
        destroyNodes();
    }

    public Node clientNode() {
        return findRunningNode(clientNodes(), dataNodes(), masterNodes());
    }

    public Node randomClientNode() {
        return randomRunningNode(clientNodes(), dataNodes(), masterNodes());
    }

    public Node masterNode() {
        return findRunningNode(masterNodes());
    }

    @SafeVarargs
    private final Node findRunningNode(List<? extends Node> nodes, List<? extends Node>... moreNodes) {
        for (Node node : nodes) {
            if (node.isRunning()) {
                return node;
            }
        }

        if (moreNodes != null && moreNodes.length > 0) {
            for (List<? extends Node> nodesList : moreNodes) {
                for (Node node : nodesList) {
                    if (node.isRunning()) {
                        return node;
                    }
                }
            }
        }

        return null;
    }

    @SafeVarargs
    private final Node randomRunningNode(List<? extends Node> nodes, List<? extends Node>... moreNodes) {
        ArrayList<Node> runningNodes = new ArrayList<>();

        for (Node node : nodes) {
            if (node.isRunning()) {
                runningNodes.add(node);
            }
        }

        if (moreNodes != null && moreNodes.length > 0) {
            for (List<? extends Node> nodesList : moreNodes) {
                for (Node node : nodesList) {
                    if (node.isRunning()) {
                        runningNodes.add(node);
                    }
                }
            }
        }

        if (runningNodes.size() == 0) {
            return null;
        }

        int index = this.random.nextInt(runningNodes.size());

        return runningNodes.get(index);
    }

    protected Settings getMinimalEsSettings() {
        return Settings.builder()//
                .put("cluster.name", clusterName)//
                .putList("cluster.initial_master_nodes", initialMasterHosts)//
                .put("discovery.initial_state_timeout", "8s")//
                .putList("discovery.seed_hosts", seedHosts)//
                .put("cluster.routing.allocation.disk.threshold_enabled", false)//
                .put("discovery.probe.connect_timeout", "10s")//
                .put("discovery.probe.handshake_timeout", "10s")//
                .put("http.cors.enabled", true).build();
    }

    protected static Settings joinedSettings(Settings... settings) {
        Settings.Builder result = Settings.builder();

        for (Settings s : settings) {
            result.put(s);
        }

        return result.build();
    }

    protected void retry() throws Exception {
        retry++;

        if (retry > 10) {
            throw new RuntimeException("Detected port collisions for master node. Giving up.");
        }

        stop();

        this.seedHosts = null;
        this.initialMasterHosts = null;

        FileUtils.cleanDirectory(this.clusterHomeDir);

        start();
    }

    private static List<String> toHostList(Collection<Integer> ports) {
        return ports.stream().map(port -> "127.0.0.1:" + port).collect(Collectors.toList());
    }

    private static int getUnitTestForkNumber() {
        String forkno = System.getProperty("forkno");

        if (forkno != null && forkno.length() > 0) {
            return Integer.parseInt(forkno.split("_")[1]);
        } else {
            return 42;
        }
    }

    public interface Node extends EsClientProvider {
        InetSocketAddress getTransportAddress();

        InetSocketAddress getHttpAddress();

        String getNodeName();

        boolean isRunning();

        void stop();
    }
}
