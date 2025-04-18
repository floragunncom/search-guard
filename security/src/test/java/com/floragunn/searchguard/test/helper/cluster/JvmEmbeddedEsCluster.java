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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.floragunn.searchguard.client.RestHighLevelClient;
import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.BindTransportException;

import com.floragunn.searchguard.test.GenericRestClient.RequestInfo;
import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration.NodeSettings;
import com.floragunn.searchguard.test.helper.network.PortAllocator;
import com.google.common.net.InetAddresses;

/**
 * This is the SG-agnostic and ES-specific part of LocalCluster
 */
public class JvmEmbeddedEsCluster extends LocalEsCluster {

    static {
        System.setProperty("es.enforce.bootstrap.checks", "true");
    }

    private static final Logger log = LogManager.getLogger(JvmEmbeddedEsCluster.class);

    private final List<Class<? extends Plugin>> additionalPlugins;
    private final List<Node> allNodes = new ArrayList<>();
    private final List<Node> masterNodes = new ArrayList<>();
    private final List<Node> dataNodes = new ArrayList<>();
    private final List<Node> clientNodes = new ArrayList<>();
    private final LeakDetector leakDetector;

    public JvmEmbeddedEsCluster(String clusterName, ClusterConfiguration clusterConfiguration, NodeSettingsSupplier nodeSettingsSupplier,
            List<Class<? extends Plugin>> additionalPlugins, TestCertificates testCertificates) {
        super(clusterName, clusterConfiguration, nodeSettingsSupplier, testCertificates);
        this.additionalPlugins = additionalPlugins;
        this.leakDetector = new LeakDetector();
    }

    public boolean isStarted() {
        return started;
    }

    @Override
    public void start() throws Exception {
        super.start();
        leakDetector.start();
    }

    public void destroy() {
        try {
            stop();
            clientNodes.clear();
            dataNodes.clear();
            masterNodes.clear();
        } finally {
            leakDetector.stop();
        }

        try {
            FileUtils.deleteDirectory(clusterHomeDir);
        } catch (IOException e) {
            log.warn("Error while deleting " + clusterHomeDir, e);
        }
    }

    @Override
    public List<JvmEmbeddedEsCluster.Node> getAllNodes() {
        return Collections.unmodifiableList(allNodes);
    }

    @Override
    protected CompletableFuture<Node> startNode(NodeSettings nodeSettings, int httpPort, int transportPort) {
        return new Node(nodeSettings, transportPort, httpPort).start();
    }

    public void waitForGreenCluster() throws Exception {
        ClusterHealthStatus status = ClusterHealthStatus.GREEN;
        TimeValue timeout = TimeValue.timeValueSeconds(10);
        int expectedNodeCount = allNodes.size();
        Client client = clientNode().getInternalNodeClient();

        try {
            AdminClient adminClient = client.admin();

            TimeValue masterNoeTimeout = TimeValue.timeValueSeconds(40);
            final ClusterHealthResponse healthResponse = adminClient.cluster().prepareHealth(masterNoeTimeout).setWaitForStatus(status).setTimeout(timeout)
                    .setMasterNodeTimeout(timeout).setWaitForNodes("" + expectedNodeCount).execute().actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Current ClusterState:\n{}", Strings.toString(healthResponse));
            }

            if (healthResponse.isTimedOut()) {
                throw new Exception(
                        "cluster state is " + healthResponse.getStatus().name() + " with " + healthResponse.getNumberOfNodes() + " nodes");
            } else {
                log.debug("... cluster state ok {} with {} nodes", healthResponse.getStatus().name(), healthResponse.getNumberOfNodes());
            }

            assertEquals(expectedNodeCount, healthResponse.getNumberOfNodes());

        } catch (ElasticsearchTimeoutException e) {
            throw new Exception("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
        }
    }

    public <X> X getInjectable(Class<X> clazz) {
        return ((Node) masterNode()).getInjectable(clazz);
    }

    @Override
    public String toString() {
        return "\nES Cluster " + clusterName + "\nmaster nodes: " + masterNodes + "\n  data nodes: " + dataNodes + "\nclient nodes: " + clientNodes
                + "\n";
    }

    public class Node implements EsClientProvider, LocalEsCluster.Node {
        private final String nodeName;
        private final NodeSettings nodeSettings;
        private final File nodeHomeDir;
        private final File dataDir;
        private final File logsDir;
        private final int transportPort;
        private final int httpPort;
        private final InetSocketAddress httpAddress;
        private final InetSocketAddress transportAddress;
        private PluginAwareNode node;
        private boolean running = false;

        Node(NodeSettings nodeSettings, int transportPort, int httpPort) {
            this.nodeName = nodeSettings.name;
            this.nodeSettings = nodeSettings;
            this.nodeHomeDir = new File(clusterHomeDir, nodeName);
            this.dataDir = new File(this.nodeHomeDir, "data");
            this.logsDir = new File(this.nodeHomeDir, "logs");
            this.transportPort = transportPort;
            this.httpPort = httpPort;
            InetAddress hostAddress = InetAddresses.forString("127.0.0.1");
            this.httpAddress = new InetSocketAddress(hostAddress, httpPort);
            this.transportAddress = new InetSocketAddress(hostAddress, transportPort);

            if (nodeSettings.masterNode) {
                masterNodes.add(this);
            } else if (nodeSettings.dataNode) {
                dataNodes.add(this);
            } else {
                clientNodes.add(this);
            }
            allNodes.add(this);
        }

        CompletableFuture<Node> start() {
            CompletableFuture<Node> completableFuture = new CompletableFuture<>();

            this.node = new PluginAwareNode(nodeSettings.masterNode, getEsSettings(), additionalPlugins);

            new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        node.start();
                        running = true;
                        completableFuture.complete(Node.this);
                    } catch (BindTransportException | BindHttpException e) {
                        log.warn("Port collision detected for {}", this, e);
                        
                        portCollisionDetected = true;
                        
                        try {
                            node.close();
                        } catch (IOException e1) {
                            log.error(e1);
                        }

                        node = null;
                        PortAllocator.TCP.blacklist(transportPort, httpPort);

                        completableFuture.completeExceptionally(e);

                    } catch (Throwable e) {
                        log.error("Unable to start {}", this, e);
                        node = null;
                        completableFuture.completeExceptionally(e);
                    }
                }
            }).start();

            return completableFuture;
        }

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
                log.info("Stopping {}", this);

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

        public String getNodeName() {
            return nodeName;
        }

        @Override
        public InetSocketAddress getHttpAddress() {
            return httpAddress;
        }

        @Override
        public InetSocketAddress getTransportAddress() {
            return transportAddress;
        }

        public RestHighLevelClient getRestHighLevelClient(BasicHeader basicHeader) {
            SSLContextProvider sslContextProvider = new TestCertificateBasedSSLContextProvider(testCertificates.getCaCertificate(),
                    testCertificates.getAnyClientCertificate());

            RestClientBuilder builder = RestClient.builder(new HttpHost(getHttpAddress().getHostString(), getHttpAddress().getPort(), "https"))
                    .setDefaultHeaders(new Header[] { basicHeader })
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(
                            new SSLIOSessionStrategy(sslContextProvider.getSslContext(false), null, null, NoopHostnameVerifier.INSTANCE)));

            return new RestHighLevelClient(builder);
        }

        private Settings getEsSettings() {
            Settings.Builder settings = Settings.builder().put(getMinimalNodeSpecificSettings()).put(getMinimalEsSettings());

            if (nodeSettingsSupplier != null) {
                settings.put(nodeSettingsSupplier.get(0));
            }

            if (testCertificates != null) {
                settings.put(testCertificates.getSgSettings());
            }

            return settings.build();
        }

        private Settings getMinimalNodeSpecificSettings() {
            List<String> nodeRoles = new ArrayList<>();

            if (nodeSettings.dataNode) {
                nodeRoles.add("data");
            }

            if (nodeSettings.masterNode) {
                nodeRoles.add("master");
            }

            nodeRoles.add("remote_cluster_client");

            return Settings.builder().put("node.name", nodeName).putList("node.roles", nodeRoles).put("path.home", nodeHomeDir.toPath())
                    .put("path.data", dataDir.toPath()).put("path.logs", logsDir.toPath()).put("transport.port", transportPort)
                    .put("http.port", httpPort).build();
        }

        @Override
        public String getClusterName() {
            return clusterName;
        }

        @Override
        public TestCertificates getTestCertificates() {
            return testCertificates;
        }

        @Override
        public Consumer<RequestInfo> getRequestInfoConsumer() {
            return null;
        }
    }

    @Override
    protected void destroyNodes() {
        this.allNodes.clear();
        this.masterNodes.clear();
        this.dataNodes.clear();
        this.clientNodes.clear();
    }

    @Override
    public List<Node> clientNodes() {
        return clientNodes;
    }

    @Override
    public List<Node> dataNodes() {
        return dataNodes;
    }

    @Override
    public List<Node> masterNodes() {
        return masterNodes;
    }

    @Override
    public Node clientNode() {
        return (Node) super.clientNode();
    }

}
