/*
 * Copyright 2024 floragunn GmbH
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.RequestInfo;
import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration.NodeSettings;
import com.floragunn.searchguard.test.helper.cluster.EsDownload.EsInstallationUnavailableException;

public class ExternalProcessEsCluster extends LocalEsCluster {
    private static final Logger log = LogManager.getLogger(ExternalProcessEsCluster.class);

    /**
     * This thread pool provides threads for consuming logs from ES nodes. One node needs two threads, thus a cluster with 3 nodes needs 6 threads. 
     * As we even could run several clusters in paralle, this executor service is unbounded.
     * 
     * Note: If there's a way to reduce the number of threads without performance impact that would be ofcourse cool. But it seems Java process interfaces 
     * do not really support async io.
     */
    private static final ExecutorService logConsumptionExecutorService = Executors.newCachedThreadPool();

    /**
     * This thread pool is for more or less quick (less than 10 seconds) async actions.
     */
    private static final ExecutorService quickActionExecutorService = new ThreadPoolExecutor(0, 10, 5, TimeUnit.MINUTES, new SynchronousQueue<>());

    private boolean started;
    protected final File esDir;
    private EsInstallation esInstallation;
    private final List<Node> allNodes = new ArrayList<>();
    private final List<Node> masterNodes = new ArrayList<>();
    private final List<Node> dataNodes = new ArrayList<>();
    private final List<Node> clientNodes = new ArrayList<>();
    private final TestSgConfig testSgConfig;
    private TestCertificates installedTestCertificates;

    public ExternalProcessEsCluster(String clusterName, ClusterConfiguration clusterConfiguration, NodeSettingsSupplier nodeSettingsSupplier,
            TestCertificates testCertificates, TestSgConfig testSgConfig) {
        super(clusterName, clusterConfiguration, nodeSettingsSupplier, testCertificates);
        this.esDir = new File(this.clusterHomeDir, "es");
        this.testSgConfig = testSgConfig;
    }

    public void start() throws Exception {
        String esVersion = this.getEsVersion();

        CompletableFuture<EsInstallation> installationFuture = EsDownload.get(esVersion).extractAsync(esDir);
        CompletableFuture<SgPluginPackage> pluginFuture = SgPluginPackage.get();

        failFastAllOf(installationFuture, pluginFuture).get();

        this.esInstallation = installationFuture.get();
        this.esInstallation.ensureKeystore();
        this.esInstallation.installPlugin(pluginFuture.get().getFile());

        this.esInstallation.appendConfig("jvm.options", "-Xms1g");
        this.esInstallation.appendConfig("jvm.options", "-Xmx1g");

        this.esInstallation.appendConfig("elasticsearch.yml", "" //
                + "cluster.routing.allocation.disk.threshold_enabled: false\n" //
                + "ingest.geoip.downloader.enabled: false\n" //
                + "xpack.security.enabled: false\n" //
                + "searchguard.background_init_if_sgindex_not_exist: false");

        this.installedTestCertificates = this.testCertificates.at(this.esInstallation.getConfigPath());

        this.started = true;

        super.start();
    }

    @Override
    protected CompletableFuture<Node> startNode(NodeSettings nodeSettings, int httpPort, int transportPort) {
        CompletableFuture<Node> result = new CompletableFuture<>();

        quickActionExecutorService.submit(() -> {
            try {
                File nodeHomeDir = new File(clusterHomeDir, nodeSettings.name);
                nodeHomeDir.mkdir();
                nodeHomeDir.deleteOnExit();

                Settings settings = joinedSettings(this.nodeSettingsSupplier.get(0), this.installedTestCertificates.getSgSettings(),
                        getMinimalEsSettings());

                Process process = this.esInstallation.startProcess(httpPort, transportPort, nodeHomeDir, settings, nodeSettings);

                try {
                    new Node(nodeSettings, process, httpPort, transportPort, result, this);
                } catch (Throwable t) {
                    process.destroyForcibly();
                    result.completeExceptionally(t);
                }
            } catch (EsInstallationUnavailableException e) {
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public void destroy() {
        stop();
        clientNodes.clear();
        dataNodes.clear();
        masterNodes.clear();
        allNodes.clear();

        try {
            FileUtils.deleteDirectory(clusterHomeDir);
        } catch (IOException e) {
            log.warn("Error while deleting " + clusterHomeDir, e);
        }
    }

    @Override
    public String toString() {
        return "external_process_cluster" + this.allNodes;
    }

    private String getEsVersion() {
        return org.elasticsearch.Version.CURRENT.toString();
    }

    public static class Node implements LocalEsCluster.Node {
        private final NodeSettings nodeSettings;
        private final Process process;
        private final CompletableFuture<Node> onReady;
        private boolean onReadyCompleted;
        private final Instant started = Instant.now();
        private final InetSocketAddress transportAddress;
        private final InetSocketAddress httpAddress;
        private final String name;
        private final ExternalProcessEsCluster cluster;

        private boolean ready = false;

        Node(NodeSettings nodeSettings, Process process, int httpPort, int transportPort, CompletableFuture<Node> onReady,
                ExternalProcessEsCluster cluster) throws UnknownHostException {
            this.nodeSettings = nodeSettings;
            this.name = nodeSettings.name;
            this.process = process;
            this.onReady = onReady;
            this.transportAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), transportPort);
            this.httpAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), httpPort);
            this.cluster = cluster;

            logConsumptionExecutorService.submit(() -> {
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String line;

                    while ((line = reader.readLine()) != null) {
                        System.out.println(line);
                        this.processLogLine(line);
                    }

                    if (!this.onReadyCompleted) {
                        this.completeFutureExceptionally(new Exception("Startup has failed"));
                    }

                } catch (IOException e) {
                    if ("Stream closed".equals(e.getMessage())) {
                        if (this.onReadyCompleted) {
                            // This is probably a normal shutdown
                            log.info("Output stream of " + this + " was closed");
                        } else {
                            log.error("Output stream of " + this + " was closed before startup was finished", e);
                            this.completeFutureExceptionally(e);
                        }
                    } else {
                        log.error("Error while monitoring output of " + this, e);
                        this.completeFutureExceptionally(e);
                    }
                } catch (Throwable t) {
                    log.error("Error while monitoring output of " + this, t);
                    this.completeFutureExceptionally(t);
                } finally {
                    this.stop();
                }
            });

            logConsumptionExecutorService.submit(() -> {
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                    String line;

                    while ((line = reader.readLine()) != null) {
                        System.out.println(line);
                    }

                    if (!this.onReadyCompleted) {
                        this.completeFutureExceptionally(new Exception("Startup has failed"));
                    }

                    this.stop();
                } catch (IOException e) {
                    if ("Stream closed".equals(e.getMessage())) {
                        if (this.onReadyCompleted) {
                            // This is probably a normal shutdown
                            log.info("Error stream of " + this + " was closed");
                        } else {
                            log.error("Error stream of " + this + " was closed before startup was finished", e);
                            this.completeFutureExceptionally(e);
                        }
                    } else {
                        log.error("Error while monitoring output of " + this, e);
                        this.completeFutureExceptionally(e);
                    }
                } catch (Throwable t) {
                    log.error("Error while monitoring output of " + this, t);
                    this.completeFutureExceptionally(t);
                } finally {
                    this.stop();
                }
            });

            if (nodeSettings.masterNode) {
                cluster.masterNodes.add(this);
            } else if (nodeSettings.dataNode) {
                cluster.dataNodes.add(this);
            } else {
                cluster.clientNodes.add(this);
            }
            cluster.allNodes.add(this);
        }

        @Override
        public InetSocketAddress getTransportAddress() {
            return transportAddress;
        }

        @Override
        public InetSocketAddress getHttpAddress() {
            return httpAddress;
        }

        @Override
        public String getNodeName() {
            return name;
        }

        @Override
        public boolean isRunning() {
            return ready;
        }

        @Override
        public synchronized void stop() {
            if (process.isAlive()) {
                log.info("Stopping " + this);
                process.destroyForcibly();
            }

            if (!this.onReadyCompleted) {
                this.completeFutureExceptionally(new Exception("stop() was called"));
            }
        }

        @Override
        public String getClusterName() {
            return cluster.clusterName;
        }

        @Override
        public TestCertificates getTestCertificates() {
            return cluster.testCertificates;
        }

        @Override
        public Consumer<RequestInfo> getRequestInfoConsumer() {
            // TODO Auto-generated method stub
            return (r) -> {
            };
        }

        private synchronized void completeFuture() {
            if (!this.onReadyCompleted) {
                this.onReady.complete(this);
                this.onReadyCompleted = true;
            }
        }

        private synchronized void completeFutureExceptionally(Throwable t) {
            if (!this.onReadyCompleted) {
                this.onReady.completeExceptionally(t);
                this.onReadyCompleted = true;
                if (process.isAlive()) {
                    log.info("Stopping " + this);
                    process.destroyForcibly();
                }
            }
        }

        private void processLogLine(String line) {
            // Note: This runs on the log consumption thread. Be careful on what you are executing synchronously. If it blocks, you might also block the ES execution, which can lead to deadlocks.

            if (!this.ready) {
                if (nodeSettings.masterNode && cluster.testSgConfig != null
                        && line.contains(".searchguard index does not exist yet, use sgctl to initialize the cluster.")) {
                    quickActionExecutorService.submit(() -> {
                        log.info("Wating for components");
                        LocalCluster.waitForComponents(ImmutableSet.of("config_var_storage"), this);
                        log.info("Setting initial Search Guard configuration");
                        try (GenericRestClient client = getAdminCertRestClient()) {
                            cluster.testSgConfig.initByConfigRestApi(client);
                            log.info("Configuration initialized");
                        } catch (Exception e) {
                            log.error("Error while initializing configuration", e);
                            this.completeFutureExceptionally(e);
                            this.stop();
                        }

                    });
                } else if (line.contains("Search Guard configuration has been successfully initialized")) {
                    quickActionExecutorService.submit(() -> {
                        for (int i = 0; i < 10000; i++) {
                            try {
                                Thread.sleep(10);

                                try (GenericRestClient client = getRestClient()) {
                                    GenericRestClient.HttpResponse response = client.get("/");

                                    if (response.getStatusCode() != 503) {
                                        this.ready = true;
                                        this.completeFuture();
                                        return;
                                    }
                                } catch (Exception e) {
                                    this.completeFutureExceptionally(e);
                                    this.stop();
                                    return;
                                }
                            } catch (InterruptedException e) {
                                this.completeFutureExceptionally(e);
                                this.stop();
                                return;
                            }
                        }

                        this.completeFutureExceptionally(new Exception("Node startup has timed out"));
                    });
                } else if (line.contains("BindHttpException")) {
                    cluster.portCollisionDetected = true;
                } else if (this.started.compareTo(Instant.now().minus(1, ChronoUnit.MINUTES)) < 0) {
                    this.completeFutureExceptionally(new Exception("Node startup has timed out"));
                }
            }
        }

        @Override
        public String toString() {
            return cluster.clusterName + "/" + name;
        }

    }

    @Override
    public List<? extends com.floragunn.searchguard.test.helper.cluster.LocalEsCluster.Node> getAllNodes() {
        return allNodes;
    }

    @Override
    public List<? extends com.floragunn.searchguard.test.helper.cluster.LocalEsCluster.Node> clientNodes() {
        return clientNodes;
    }

    @Override
    public List<? extends com.floragunn.searchguard.test.helper.cluster.LocalEsCluster.Node> dataNodes() {
        return dataNodes;
    }

    @Override
    public List<? extends com.floragunn.searchguard.test.helper.cluster.LocalEsCluster.Node> masterNodes() {
        return masterNodes;
    }

    @Override
    public void waitForGreenCluster() throws Exception {
        try (GenericRestClient client = masterNode().getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = client.get("/_cluster/health?wait_for_status=green&timeout=30s");

            if (response.getStatusCode() != 200) {
                throw new Exception("/_cluster/health request failed: " + response);
            }

            if (!"green".equals(response.getBodyAsDocNode().getAsString("status"))) {
                throw new Exception("Cluster is not green: " + response);
            }
        }
    }

    @Override
    protected void destroyNodes() {
        this.allNodes.clear();
        this.masterNodes.clear();
        this.dataNodes.clear();
        this.clientNodes.clear();
    }

    private static CompletableFuture<?> failFastAllOf(CompletableFuture<?>... futures) {
        CompletableFuture<?> failure = new CompletableFuture<>();
        for (CompletableFuture<?> f : futures) {
            f.exceptionally(ex -> {
                failure.completeExceptionally(ex);
                return null;
            });
        }
        failure.exceptionally(ex -> {
            for (CompletableFuture<?> future : futures) {
                future.cancel(true);
            }
            return null;
        });
        return CompletableFuture.anyOf(failure, CompletableFuture.allOf(futures));
    }

}
