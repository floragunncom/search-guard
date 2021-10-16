/*
 * Copyright 2015-2021 floragunn GmbH
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
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import org.apache.commons.io.FileUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.TransportInfo;

import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration.NodeSettings;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.network.PortAllocator;
import com.google.common.net.InetAddresses;

/**
 * This is the SG-agnostic and ES-specific part of LocalCluster
 */
public class LocalEsCluster {

    static {
        System.setProperty("es.enforce.bootstrap.checks", "true");
    }

    private static final Logger log = LogManager.getLogger(LocalEsCluster.class);

    private final String clusterName;
    private final ClusterConfiguration clusterConfiguration;
    private final NodeSettingsSupplier nodeSettingsSupplier;
    private final List<Class<? extends Plugin>> additionalPlugins;
    private File clusterHomeDir;
    private final List<Node> allNodes = new ArrayList<>();
    private final List<Node> masterNodes = new ArrayList<>();
    private final List<Node> dataNodes = new ArrayList<>();
    private final List<Node> clientNodes = new ArrayList<>();

    // TODO replace by proper TLS config
    private String resourcesFolder;

    private List<String> seedHosts;
    private List<String> initialMasterHosts;
    private TimeValue timeout = TimeValue.timeValueSeconds(10);
    private int retry = 0;
    private boolean started;

    public LocalEsCluster(String clustername, ClusterConfiguration clusterConfiguration, NodeSettingsSupplier nodeSettingsSupplier,
            String resourcesFolder, List<Class<? extends Plugin>> additionalPlugins) {
        super();
        this.clusterName = clustername;
        this.clusterConfiguration = clusterConfiguration;
        this.nodeSettingsSupplier = nodeSettingsSupplier;
        this.additionalPlugins = additionalPlugins;
        this.resourcesFolder = resourcesFolder;
        try {
            this.clusterHomeDir = Files.createTempDirectory("sg_local_cluster_" + clustername).toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() throws Exception {

        if (log.isDebugEnabled()) {
            log.debug("Starting " + clusterName);
        }

        int forkNumber = getUnitTestForkNumber();
        int masterNodeCount = clusterConfiguration.getMasterNodes();
        int nonMasterNodeCount = clusterConfiguration.getDataNodes() + clusterConfiguration.getClientNodes();

        // TODO expiry hier festlegel
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

        CompletableFuture<Void> otherNodeFuture = startNodes(clusterConfiguration.getNonMasterNodeSettings(), nonMasterNodeTransportPorts,
                nonMasterNodeHttpPorts);

        CompletableFuture.allOf(masterNodeFuture, otherNodeFuture).join();

        if (isNodeFailedWithPortCollision()) {
            log.info("Detected port collision for master node. Retrying.");

            retry();
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Startup finished. Waiting for GREEN");
        }

        waitForCluster(ClusterHealthStatus.GREEN, timeout, allNodes.size());
        putDefaultTemplate();

        if (log.isInfoEnabled()) {
            log.info("Started: " + this);
        }

    }

    private void putDefaultTemplate() {
        String defaultTemplate = "{\n" //
                + "          \"index_patterns\": [\"*\"],\n" //
                + "          \"order\": -1,\n" + "          \"settings\": {\n" //
                + "            \"number_of_shards\": \"5\",\n" //
                + "            \"number_of_replicas\": \"1\"\n"//
                + "          }\n" //
                + "        }";

        AcknowledgedResponse templateAck = clientNode().getInternalNodeClient().admin().indices()
                .putTemplate(new PutIndexTemplateRequest("default").source(defaultTemplate, XContentType.JSON)).actionGet();

        if (!templateAck.isAcknowledged()) {
            throw new RuntimeException("Default template could not be created");
        }
    }

    private CompletableFuture<Void> startNodes(List<NodeSettings> nodeSettingList, SortedSet<Integer> transportPorts, SortedSet<Integer> httpPorts) {
        Iterator<Integer> transportPortIterator = transportPorts.iterator();
        Iterator<Integer> httpPortIterator = httpPorts.iterator();
        List<CompletableFuture<?>> futures = new ArrayList<>();

        for (NodeSettings nodeSettings : nodeSettingList) {
            Node node = new Node(nodeSettings, transportPortIterator.next(), httpPortIterator.next());
            futures.add(node.start());
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private int getUnitTestForkNumber() {

        String forkno = System.getProperty("forkno");

        if (forkno != null && forkno.length() > 0) {
            return Integer.parseInt(forkno.split("_")[1]);
        } else {
            return 42;
        }
    }

    public void stop() {

        for (Node node : clientNodes) {
            node.stop();
        }

        for (Node node : dataNodes) {
            node.stop();
        }

        for (Node node : masterNodes) {
            node.stop();
        }
    }

    public void destroy() {
        stop();
        clientNodes.clear();
        dataNodes.clear();
        masterNodes.clear();

        try {
            FileUtils.deleteDirectory(clusterHomeDir);
        } catch (IOException e) {
            log.warn("Error while deleting " + clusterHomeDir, e);
        }
    }

    public Node clientNode() {
        return findRunningNode(clientNodes, dataNodes, masterNodes);
    }

    public Node masterNode() {
        return findRunningNode(masterNodes);
    }

    private boolean isNodeFailedWithPortCollision() {
        for (Node node : allNodes) {
            if (node.isPortCollision()) {
                return true;
            }
        }

        return false;
    }

    private void retry() throws Exception {
        retry++;

        if (retry > 10) {
            throw new RuntimeException("Detected port collisions for master node. Giving up.");
        }

        stop();

        this.allNodes.clear();
        this.masterNodes.clear();
        this.dataNodes.clear();
        this.clientNodes.clear();
        this.seedHosts = null;
        this.initialMasterHosts = null;
        this.clusterHomeDir = Files.createTempDirectory("sg_local_cluster_" + clusterName + "_retry_" + retry).toFile();

        start();
    }

    @SafeVarargs
    private static final Node findRunningNode(List<Node> nodes, List<Node>... moreNodes) {
        for (Node node : nodes) {
            if (node.isRunning()) {
                return node;
            }
        }

        if (moreNodes != null && moreNodes.length > 0) {
            for (List<Node> nodesList : moreNodes) {
                for (Node node : nodesList) {
                    if (node.isRunning()) {
                        return node;
                    }
                }
            }
        }

        return null;
    }

    public List<Node> allNodes() {
        return Collections.unmodifiableList(allNodes);
    }

    public Node getNodeByName(String name) {
        for (Node node : allNodes) {
            if (node.getNodeName().equals(name)) {
                return node;
            }
        }

        throw new RuntimeException(
                "No such node name: " + name + "; available: " + allNodes.stream().map((n) -> n.getNodeName()).collect(Collectors.toList()));
    }

    public ClusterInfo waitForCluster(ClusterHealthStatus status, TimeValue timeout, int expectedNodeCount) throws IOException {

        ClusterInfo clusterInfo = new ClusterInfo();
        Client client = clientNode().getInternalNodeClient();

        try {
            log.debug("waiting for cluster state {} and {} nodes", status.name(), expectedNodeCount);
            final ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForStatus(status).setTimeout(timeout)
                    .setMasterNodeTimeout(timeout).setWaitForNodes("" + expectedNodeCount).execute().actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Current ClusterState:\n" + Strings.toString(healthResponse));
            }

            if (healthResponse.isTimedOut()) {
                throw new IOException(
                        "cluster state is " + healthResponse.getStatus().name() + " with " + healthResponse.getNumberOfNodes() + " nodes");
            } else {
                log.debug("... cluster state ok " + healthResponse.getStatus().name() + " with " + healthResponse.getNumberOfNodes() + " nodes");
            }

            org.junit.Assert.assertEquals(expectedNodeCount, healthResponse.getNumberOfNodes());

            final NodesInfoResponse res = client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();

            final List<NodeInfo> nodes = res.getNodes();

            final List<NodeInfo> masterNodes = nodes.stream().filter(n -> n.getNode().getRoles().contains(DiscoveryNodeRole.MASTER_ROLE))
                    .collect(Collectors.toList());
            final List<NodeInfo> dataNodes = nodes.stream().filter(n -> n.getNode().getRoles().contains(DiscoveryNodeRole.DATA_ROLE)
                    && !n.getNode().getRoles().contains(DiscoveryNodeRole.MASTER_ROLE)).collect(Collectors.toList());
            final List<NodeInfo> clientNodes = nodes.stream().filter(n -> !n.getNode().getRoles().contains(DiscoveryNodeRole.MASTER_ROLE)
                    && !n.getNode().getRoles().contains(DiscoveryNodeRole.DATA_ROLE)).collect(Collectors.toList());

            for (NodeInfo nodeInfo : masterNodes) {
                final TransportAddress is = nodeInfo.getInfo(TransportInfo.class).getAddress().publishAddress();
                clusterInfo.nodePort = is.getPort();
                clusterInfo.nodeHost = is.getAddress();
            }

            if (!clientNodes.isEmpty()) {
                NodeInfo nodeInfo = clientNodes.get(0);
                if (nodeInfo.getInfo(HttpInfo.class) != null && nodeInfo.getInfo(HttpInfo.class).address() != null) {
                    final TransportAddress his = nodeInfo.getInfo(HttpInfo.class).address().publishAddress();
                    clusterInfo.httpPort = his.getPort();
                    clusterInfo.httpHost = his.getAddress();
                } else {
                    throw new RuntimeException("no http host/port for client node");
                }
            } else if (!dataNodes.isEmpty()) {

                for (NodeInfo nodeInfo : dataNodes) {
                    if (nodeInfo.getInfo(HttpInfo.class) != null && nodeInfo.getInfo(HttpInfo.class).address() != null) {
                        final TransportAddress his = nodeInfo.getInfo(HttpInfo.class).address().publishAddress();
                        clusterInfo.httpPort = his.getPort();
                        clusterInfo.httpHost = his.getAddress();
                        break;
                    }
                }
            } else {

                for (NodeInfo nodeInfo : nodes) {
                    if (nodeInfo.getInfo(HttpInfo.class) != null && nodeInfo.getInfo(HttpInfo.class).address() != null) {
                        final TransportAddress his = nodeInfo.getInfo(HttpInfo.class).address().publishAddress();
                        clusterInfo.httpPort = his.getPort();
                        clusterInfo.httpHost = his.getAddress();
                        break;
                    }
                }
            }

            for (NodeInfo nodeInfo : nodes) {
                clusterInfo.httpAdresses.add(nodeInfo.getInfo(HttpInfo.class).address().publishAddress());
            }
        } catch (final ElasticsearchTimeoutException e) {
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
        }
        return clusterInfo;
    }

    @Override
    public String toString() {
        return "\nES Cluster " + clusterName + "\nmaster nodes: " + masterNodes + "\n  data nodes: " + dataNodes + "\nclient nodes: " + clientNodes
                + "\n";
    }


    //todo fix also here!
    private SSLContext getSSLContext() {
        try {
            String truststoreType = "JKS";
            String truststorePassword = "changeit";
            String prefix = resourcesFolder == null ? "" : resourcesFolder + "/";

            KeyStore trustStore = KeyStore.getInstance(truststoreType);
            try (InputStream in = Files.newInputStream(FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"))) {
                trustStore.load(in, (truststorePassword == null || truststorePassword.length() == 0) ? null : truststorePassword.toCharArray());
            }

            SSLContextBuilder sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
            return sslContextBuilder.build();

            //return new OverlyTrustfulSSLContextBuilder().build();
        } catch (Exception e) {
            throw new RuntimeException("Error while building SSLContext", e);
        }
    }

    public SSLIOSessionStrategy getSSLIOSessionStrategy() {
        return new SSLIOSessionStrategy(getSSLContext(), null, null, NoopHostnameVerifier.INSTANCE);
    }

    private static List<String> toHostList(Collection<Integer> ports) {
        return ports.stream().map(s -> "127.0.0.1:" + s).collect(Collectors.toList());
    }

    public class Node implements EsClientProvider {
        private final String nodeName;
        private final NodeSettings nodeSettings;
        private final File nodeHomeDir;
        private final File dataDir;
        private final File logsDir;
        private final int transportPort;
        private final int httpPort;
        private final InetAddress hostAddress;
        private final InetSocketAddress httpAddress;
        private final InetSocketAddress transportAddress;
        private PluginAwareNode node;
        private boolean running = false;
        private boolean portCollision = false;

        Node(NodeSettings nodeSettings, int transportPort, int httpPort) {
            this.nodeName = createNextNodeName(nodeSettings);
            this.nodeSettings = nodeSettings;
            this.nodeHomeDir = new File(clusterHomeDir, nodeName);
            this.dataDir = new File(this.nodeHomeDir, "data");
            this.logsDir = new File(this.nodeHomeDir, "logs");
            this.transportPort = transportPort;
            this.httpPort = httpPort;
            this.hostAddress = InetAddresses.forString("127.0.0.1");
            this.httpAddress = new InetSocketAddress(this.hostAddress, httpPort);
            this.transportAddress = new InetSocketAddress(this.hostAddress, transportPort);

            if (nodeSettings.masterNode) {
                masterNodes.add(this);
            } else if (nodeSettings.dataNode) {
                dataNodes.add(this);
            } else {
                clientNodes.add(this);
            }

            allNodes.add(this);

        }

        CompletableFuture<String> start() {
            CompletableFuture<String> completableFuture = new CompletableFuture<>();

            this.node = new PluginAwareNode(nodeSettings.masterNode, getEsSettings(), nodeSettings.getPlugins(additionalPlugins));

            new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        node.start();
                        running = true;
                        completableFuture.complete("initialized");
                    } catch (BindTransportException | BindHttpException e) {
                        log.warn("Port collision detected for " + this, e);
                        portCollision = true;
                        try {
                            node.close();
                        } catch (IOException e2) {
                            e2.printStackTrace();
                        }

                        node = null;
                        PortAllocator.TCP.blacklist(transportPort, httpPort);

                        completableFuture.complete("retry");

                    } catch (Throwable e) {
                        log.error("Unable to start " + this, e);
                        node = null;
                        completableFuture.completeExceptionally(e);
                    }
                }
            }).start();

            return completableFuture;
        }

        Settings getEsSettings() {
            Settings settings = getMinimalEsSettings();

            if (nodeSettingsSupplier != null) {
                // TODO node number
                settings = Settings.builder().put(settings).put(nodeSettingsSupplier.get(0)).build();
            }

            return settings;
        }

        Settings getMinimalEsSettings() {

            return Settings.builder().put("node.name", nodeName)//
                    .put("node.data", nodeSettings.dataNode)//
                    .put("node.master", nodeSettings.masterNode)//
                    .put("cluster.name", clusterName)//
                    .put("path.home", nodeHomeDir.toPath())//
                    .put("path.data", dataDir.toPath())//
                    .put("path.logs", logsDir.toPath())//
                    .putList("cluster.initial_master_nodes", initialMasterHosts)//
                    .put("discovery.initial_state_timeout", "8s")//
                    .putList("discovery.seed_hosts", seedHosts)//
                    .put("transport.tcp.port", transportPort)//
                    .put("http.port", httpPort)//
                    .put("cluster.routing.allocation.disk.threshold_enabled", false)//
                    .put("discovery.probe.connect_timeout", "10s")
                    .put("discovery.probe.handshake_timeout", "10s")
                    .put("http.cors.enabled", true).build();
        }

        @Override
        public Client getInternalNodeClient() {
            return node.client();
        }

        public PluginAwareNode esNode() {
            return node;
        }

        public boolean isRunning() {
            return running;
        }

        public <X> X getInjectable(Class<X> clazz) {
            return node.injector().getInstance(clazz);
        }

        public void stop() {
            try {
                log.info("Stopping " + this);

                running = false;

                if (node != null) {
                    node.close();
                    node = null;
                    Thread.sleep(10);
                }

            } catch (Throwable e) {
                log.warn("Error while stopping " + this, e);
            }
        }

        @Override
        public String toString() {
            String state = running ? "RUNNING" : node != null ? "INITIALIZING" : "STOPPED";

            return nodeName + " " + state + " [" + transportPort + ", " + httpPort + "]";
        }

        public boolean isPortCollision() {
            return portCollision;
        }

        public String getNodeName() {
            return nodeName;
        }

        public int getTransportPort() {
            return transportPort;
        }

        public int getHttpPort() {
            return httpPort;
        }

        public String getHost() {
            return "127.0.0.1";
        }

        @Override
        public InetSocketAddress getHttpAddress() {
            return httpAddress;
        }

        @Override
        public InetSocketAddress getTransportAddress() {
            return transportAddress;
        }

        @Override
        public String getResourceFolder() {
            return resourcesFolder;
        }

        @Override
        public SSLIOSessionStrategy getSSLIOSessionStrategy() {
            return LocalEsCluster.this.getSSLIOSessionStrategy();
        }

        @Override
        public String getClusterName() {
            return LocalEsCluster.this.clusterName;
        }

    }

    private String createNextNodeName(NodeSettings nodeSettings) {
        List<Node> nodes;
        String nodeType;

        if (nodeSettings.masterNode) {
            nodes = this.masterNodes;
            nodeType = "master";
        } else if (nodeSettings.dataNode) {
            nodes = this.dataNodes;
            nodeType = "data";
        } else {
            nodes = this.clientNodes;
            nodeType = "client";
        }

        return nodeType + "_" + nodes.size();
    }

    public String getClusterName() {
        return clusterName;
    }

    public boolean isStarted() {
        return started;
    }

}
