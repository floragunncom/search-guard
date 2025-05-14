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

package com.floragunn.searchguard.ssl.test.helper.cluster;

import com.floragunn.searchguard.ssl.test.NodeSettingsSupplier;
import com.floragunn.searchguard.ssl.test.helper.cluster.ClusterConfiguration.NodeSettings;
import com.floragunn.searchguard.ssl.test.helper.network.SocketUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xcontent.XContentType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.floragunn.searchsupport.Constants.DEFAULT_MASTER_TIMEOUT;

public final class ClusterHelper {

    static {
        System.setProperty("es.enforce.bootstrap.checks", "true");
        System.setProperty("sg.default_init.dir", new File("./sgconfig").getAbsolutePath());
    }

    private static final AtomicLong num = new AtomicLong();

    protected final Logger log = LogManager.getLogger(ClusterHelper.class);

    protected final List<PluginAwareNode> esNodes = new LinkedList<>();

    private final String clusternamePrefix;

    public ClusterHelper(String prefix, int clusterNumber) {
        super();
        //"crl1_n"+num.incrementAndGet()+"_f"+System.getProperty("forkno")+"_t"+System.nanoTime()
        this.clusternamePrefix = prefix+num.incrementAndGet()+"_f"+System.getProperty("forkno")+"_c"+clusterNumber+"_t";
    }

    /**
     * Start n Elasticsearch nodes with the provided settings
     *
     * @return
     * @throws Exception
     */

    public final ClusterInfo startCluster(final NodeSettingsSupplier nodeSettingsSupplier, ClusterConfiguration clusterConfiguration)
            throws Exception {
        return startCluster(nodeSettingsSupplier, clusterConfiguration, null, 10, null);
    }

    public final synchronized ClusterInfo startCluster(final NodeSettingsSupplier nodeSettingsSupplier, ClusterConfiguration clusterConfiguration,
                                                       List<Class<? extends Plugin>> additionalPlugins, int timeout, Integer nodes) throws Exception {

        if (!esNodes.isEmpty()) {
            throw new RuntimeException("There are still " + esNodes.size() + " nodes instantiated, close them first.");
        }

        final String clustername = clusternamePrefix+System.nanoTime();

        File homeDir = null;
        try {
            homeDir = Files.createTempDirectory(clustername).toFile();
            homeDir.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<NodeSettings> internalNodeSettings = clusterConfiguration.getNodeSettings();

        final String forkno = System.getProperty("forkno");
        int forkNumber = 1;

        if (forkno != null && forkno.length() > 0) {
            forkNumber = Integer.parseInt(forkno.split("_")[1]);
        }

        final int min = SocketUtils.PORT_RANGE_MIN + (forkNumber * 5000);
        final int max = SocketUtils.PORT_RANGE_MIN + ((forkNumber + 1) * 5000) - 1;

        final SortedSet<Integer> freePorts = SocketUtils.findAvailableTcpPorts(internalNodeSettings.size() * 2, min, max);
        assert freePorts.size() == internalNodeSettings.size() * 2;
        final SortedSet<Integer> tcpMasterPortsOnly = new TreeSet<Integer>();
        final SortedSet<Integer> tcpAllPorts = new TreeSet<Integer>();
        freePorts.stream().limit(clusterConfiguration.getMasterNodes()).forEach(el -> tcpMasterPortsOnly.add(el));
        freePorts.stream().limit(internalNodeSettings.size()).forEach(el -> tcpAllPorts.add(el));

        final Iterator<Integer> tcpPortsAllIt = tcpAllPorts.iterator();

        final SortedSet<Integer> httpPorts = new TreeSet<Integer>();
        freePorts.stream().skip(internalNodeSettings.size()).limit(internalNodeSettings.size()).forEach(el -> httpPorts.add(el));
        final Iterator<Integer> httpPortsIt = httpPorts.iterator();

        System.out.println("tcpMasterPorts: " + tcpMasterPortsOnly + "/tcpAllPorts: " + tcpAllPorts + "/httpPorts: " + httpPorts + " for (" + min
                + "-" + max + ") fork " + forkNumber);

        final CountDownLatch latch = new CountDownLatch(internalNodeSettings.size());

        final AtomicReference<Exception> err = new AtomicReference<Exception>();

        List<NodeSettings> internalMasterNodeSettings = clusterConfiguration.getMasterNodeSettings();
        List<NodeSettings> internalNonMasterNodeSettings = clusterConfiguration.getNonMasterNodeSettings();

        int nodeNumCounter = internalNodeSettings.size();

        for (int i = 0; i < internalMasterNodeSettings.size(); i++) {
            NodeSettings setting = internalMasterNodeSettings.get(i);
            int nodeNum = nodeNumCounter--;
            PluginAwareNode node = new PluginAwareNode(setting.masterNode,
                    getMinimumNonSgNodeSettingsBuilder(nodeNum, setting.masterNode, setting.dataNode, tcpMasterPortsOnly,
                            tcpPortsAllIt.next(), httpPortsIt.next(), clustername, homeDir)
                            .put(nodeSettingsSupplier == null ? Settings.builder().build() : nodeSettingsSupplier.get(nodeNum)).build());

            new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        node.start();
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Unable to start node: " + e);
                        err.set(e);
                        latch.countDown();
                    }
                }
            }).start();
            esNodes.add(node);
        }

        for (int i = 0; i < internalNonMasterNodeSettings.size(); i++) {
            NodeSettings setting = internalNonMasterNodeSettings.get(i);
            int nodeNum = nodeNumCounter--;
            PluginAwareNode node = new PluginAwareNode(setting.masterNode,
                    getMinimumNonSgNodeSettingsBuilder(nodeNum, setting.masterNode, setting.dataNode, tcpMasterPortsOnly,
                            tcpPortsAllIt.next(), httpPortsIt.next(), clustername, homeDir)
                            .put(nodeSettingsSupplier == null ? Settings.builder().build() : nodeSettingsSupplier.get(nodeNum)).build());

            new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        node.start();
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Unable to start node: " + e);
                        err.set(e);
                        latch.countDown();
                    }
                }
            }).start();
            esNodes.add(node);
        }

        assert nodeNumCounter == 0;

        latch.await();

        if (err.get() != null) {
            throw new RuntimeException("Could not start all nodes " + err.get(), err.get());
        }

        ClusterInfo cInfo = waitForCluster(ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(timeout),
                nodes == null ? esNodes.size() : nodes.intValue());
        cInfo.numNodes = internalNodeSettings.size();
        cInfo.clustername = clustername;
        cInfo.tcpMasterPortsOnly = tcpMasterPortsOnly.stream().map(s -> "127.0.0.1:" + s).collect(Collectors.toList());

        final String defaultTemplate = "{\n" + "          \"index_patterns\": [\"*\"],\n" + "          \"order\": -1,\n"
                + "          \"settings\": {\n" + "            \"number_of_shards\": \"5\",\n" + "            \"number_of_replicas\": \"1\"\n"
                + "          }\n" + "        }";

        final AcknowledgedResponse templateAck = nodeClient().admin().indices()
                .putTemplate(new PutIndexTemplateRequest("default").source(defaultTemplate, XContentType.JSON)).actionGet();

        if (!templateAck.isAcknowledged()) {
            throw new RuntimeException("Default template could not be created");
        }

        return cInfo;
    }

    public final void stopCluster() throws Exception {

        //close non master nodes
        esNodes.stream().filter(n -> !n.isMasterEligible()).forEach(node -> closeNode(node));

        //close master nodes
        esNodes.stream().filter(n -> n.isMasterEligible()).forEach(node -> closeNode(node));

        esNodes.clear();
    }

    private static void closeNode(Node node) {
        try {
            node.close();
            node.awaitClose(10, TimeUnit.SECONDS);
        } catch (Throwable e) {
            //ignore
        }
    }

    public Client nodeClient() {
        return esNodes.get(0).client();
    }

    public PluginAwareNode node() {
        if (esNodes.size() == 0) {
            for (int i = 0; i < 100; i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (esNodes.size() != 0) {
                    break;
                }
            }
        }

        if (esNodes.size() == 0) {
            throw new RuntimeException("Could not get intialized cluster");
        }

        return esNodes.get(0);
    }

    public List<PluginAwareNode> allNodes() {
        return Collections.unmodifiableList(esNodes);
    }

    public ClusterInfo waitForCluster(final ClusterHealthStatus status, final TimeValue timeout, final int expectedNodeCount) throws IOException {
        if (esNodes.isEmpty()) {
            throw new RuntimeException("List of nodes was empty.");
        }

        ClusterInfo clusterInfo = new ClusterInfo();

        Node node = esNodes.get(0);
        Client client = node.client();
        try {
            log.debug("waiting for cluster state {} and {} nodes", status.name(), expectedNodeCount);
            TimeValue masterNodeTimeout = new TimeValue(40, TimeUnit.SECONDS);
            final ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth(masterNodeTimeout).setWaitForStatus(status).setTimeout(timeout)
                    .setMasterNodeTimeout(timeout).setWaitForNodes("" + expectedNodeCount).execute().actionGet();
            if (healthResponse.isTimedOut()) {

                //System.out.println("-- Time out while waiting for test cluster --");
                log.error(Strings.toString(healthResponse));
                log.error(Strings.toString(client.execute(TransportPendingClusterTasksAction.TYPE, new PendingClusterTasksRequest(masterNodeTimeout)).actionGet()));
                log.error(Strings.toString(client.admin().indices().getIndex(new GetIndexRequest(DEFAULT_MASTER_TIMEOUT).includeDefaults(true).features(GetIndexRequest.Feature.MAPPINGS)).actionGet()));
                log.error(Strings.toString(client.admin().indices().stats(new IndicesStatsRequest().all()).actionGet()));
                log.error(Strings.toString(client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet()));
                log.error(Strings.toString(client.admin().cluster().nodesStats(new NodesStatsRequest()).actionGet()));


                throw new IOException(
                        "cluster state is " + healthResponse.getStatus().name() + " with " + healthResponse.getNumberOfNodes() + " nodes: "+Strings.toString(healthResponse));
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

    // @formatter:off
    private Settings.Builder getMinimumNonSgNodeSettingsBuilder(final int nodenum, final boolean masterNode,
                                                                final boolean dataNode, SortedSet<Integer> masterTcpPorts, int tcpPort, int httpPort,
                                                                String clustername, File homeDir) {

        List<String> nodeRoles = new ArrayList<>();

        if (dataNode) {
            nodeRoles.add("data");
        }

        if (masterNode) {
            nodeRoles.add("master");
        }

        nodeRoles.add("remote_cluster_client");

        return Settings.builder()
                .put("node.name", "node_"+clustername+ "_num" + nodenum)
                .putList("node.roles", nodeRoles)
                .put("cluster.name", clustername)
                .put("path.home", homeDir.getAbsolutePath()+ "/" + nodenum)
                .put("path.data", homeDir.getAbsolutePath() + "/" + nodenum +"/data")
                .put("path.logs", homeDir.getAbsolutePath() + "/"  +nodenum +"/logs")
                .putList("cluster.initial_master_nodes", masterTcpPorts.stream().map(s->"127.0.0.1:"+s).collect(Collectors.toList()))
                .put("discovery.initial_state_timeout","8s")
                .putList("discovery.seed_hosts", masterTcpPorts.stream().map(s->"127.0.0.1:"+s).collect(Collectors.toList()))
                .put("transport.port", tcpPort)
                .put("http.port", httpPort)
                .put("cluster.routing.allocation.disk.threshold_enabled", false)
                .put("http.cors.enabled", true);
    }
    // @formatter:on
}