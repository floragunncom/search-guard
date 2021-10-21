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

import com.floragunn.searchguard.modules.SearchGuardModule;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.rest.SSLContextProvider;
import com.floragunn.searchguard.test.helper.rest.TestCertificateBasedSSLContextProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.plugins.Plugin;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

public class LocalCluster extends ExternalResource implements AutoCloseable, EsClientProvider {

    private static final Logger log = LogManager.getLogger(LocalCluster.class);

    static {
        System.setProperty("sg.default_init.dir", new File("./sgconfig").getAbsolutePath());
    }

    protected static final AtomicLong num = new AtomicLong();

    protected final String resourceFolder;
    private final List<Class<? extends Plugin>> plugins;
    private final ClusterConfiguration clusterConfiguration;
    private final TestSgConfig testSgConfig;
    private final Settings nodeOverride;
    private final String clusterName;
    private final TestCertificates testCertificates;
    private final MinimumSearchGuardSettingsSupplierFactory minimumSearchGuardSettingsSupplierFactory;
    private LocalEsCluster localEsCluster;

    private LocalCluster(String clusterName, String resourceFolder, TestSgConfig testSgConfig, Settings nodeOverride,
                         ClusterConfiguration clusterConfiguration, List<Class<? extends Plugin>> plugins, TestCertificates testCertificates) {
        this.resourceFolder = resourceFolder;
        this.plugins = plugins;
        this.clusterConfiguration = clusterConfiguration;
        this.testSgConfig = testSgConfig;
        this.nodeOverride = nodeOverride;
        this.clusterName = clusterName;
        this.testCertificates = testCertificates;
        this.minimumSearchGuardSettingsSupplierFactory = new MinimumSearchGuardSettingsSupplierFactory(resourceFolder, testCertificates);

        start();
    }

    @Override
    protected void before() throws Throwable {
        if (localEsCluster == null) {
            start();
        }
    }

    @Override
    protected void after() {
        close();
    }

    @Override
    public void close() {
        if (localEsCluster != null && localEsCluster.isStarted()) {
            try {
                Thread.sleep(100);
                localEsCluster.destroy();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                localEsCluster = null;
            }
        }
    }

    public <X> X getInjectable(Class<X> clazz) {
        return this.localEsCluster.masterNode().getInjectable(clazz);
    }

    public PluginAwareNode getEsNode() {
        return this.localEsCluster.masterNode().esNode();
    }

    public List<LocalEsCluster.Node> nodes() {
        return this.localEsCluster.getAllNodes();
    }

    public LocalEsCluster.Node getNodeByName(String name) {
        return this.localEsCluster.getNodeByName(name);
    }

    public void updateSgConfig(CType configType, String key, Map<String, Object> value) {
        SgConfigUpdater.updateSgConfig(this::getAdminCertClient, configType, key, value);
    }

    private void start() {
        try {
            localEsCluster = new LocalEsCluster(clusterName, clusterConfiguration,
                    minimumSearchGuardSettingsSupplierFactory.minimumSearchGuardSettings(nodeOverride),
                    plugins,
                    Optional.ofNullable(testCertificates)
                            .map(certificates -> new TestCertificateBasedSSLContextProvider(certificates.getCaCertificate(), certificates.getAdminCertificate(), false, true)).orElse(null));
            localEsCluster.start();
        } catch (Exception e) {
            log.error("Local ES cluster start failed", e);
            throw new RuntimeException(e);
        }

        if (testSgConfig != null) {
            SearchGuardIndexInitializer.initSearchGuardIndex(this::getAdminCertClient, testSgConfig);
        }
    }

    //todo
    @Override
    public Client getAdminCertClient() {
        if (testCertificates == null) {
            throw new RuntimeException("Cannnot get admin cert client because missing certs");
        }
        return new LocalEsClusterTransportClient(localEsCluster.getClusterName(), localEsCluster.clientNode().getTransportAddress(), testCertificates.getAdminCertificate(),
                testCertificates.getCaCertFile().toPath());
    }

    @Override
    public Client getInternalNodeClient() {
        return localEsCluster.clientNode().getInternalNodeClient();
    }

    //todo can be removed (EsClientProvider
    @Override
    public InetSocketAddress getHttpAddress() {
        return localEsCluster.clientNode().getHttpAddress();
    }

    @Override
    public BiFunction<Boolean, Boolean, SSLContextProvider> getSSLContextProvider() {
        return (sendHttpClientCertificate, trustHttpServerCertificate) -> new TestCertificateBasedSSLContextProvider(testCertificates.getCaCertificate(), testCertificates.getAdminCertificate(), sendHttpClientCertificate, trustHttpServerCertificate);
    }

    public static class Builder {

        private final Settings.Builder nodeOverrideSettingsBuilder = Settings.builder();
        private final List<String> disabledModules = new ArrayList<>();
        private final List<Class<? extends Plugin>> plugins = new ArrayList<>();
        private boolean sslEnabled;
        private String resourceFolder;
        private ClusterConfiguration clusterConfiguration = ClusterConfiguration.DEFAULT;
        private TestSgConfig testSgConfig = new TestSgConfig().resources("/");
        private String clusterName = "local_cluster";
        private TestCertificates testCertificates;

        public Builder sslEnabled() {
            sslEnabled(TestCertificates.builder()
                    .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
                    .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard")
                    .addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
                    .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard")
                    .build());
            return this;
        }

        public Builder sslEnabled(TestCertificates certificatesContext) {
            this.testCertificates = certificatesContext;
            this.sslEnabled = true;
            return this;
        }

        public Builder dependsOn(Object object) {
            // We just want to make sure that the object is already done
            if (object == null) {
                throw new IllegalStateException("Dependency not fulfilled");
            }
            return this;
        }

        public Builder resources(String resourceFolder) {
            this.resourceFolder = resourceFolder;
            testSgConfig.resources(resourceFolder);
            return this;
        }

        public Builder clusterConfiguration(ClusterConfiguration clusterConfiguration) {
            this.clusterConfiguration = clusterConfiguration;
            return this;
        }

        public Builder singleNode() {
            this.clusterConfiguration = ClusterConfiguration.SINGLE_NODE;
            return this;
        }

        public Builder sgConfig(TestSgConfig testSgConfig) {
            this.testSgConfig = testSgConfig;
            return this;
        }

        public Builder setInSgConfig(String keyPath, Object value, Object... more) {
            testSgConfig.sgConfigSettings(keyPath, value, more);
            return this;
        }

        public Builder nodeSettings(Object... settings) {
            for (int i = 0; i < settings.length - 1; i += 2) {
                String key = String.valueOf(settings[i]);
                Object value = settings[i + 1];

                nodeOverrideSettingsBuilder.put(key, String.valueOf(value));
            }

            return this;
        }

        public Builder disableModule(Class<? extends SearchGuardModule<?>> moduleClass) {
            this.disabledModules.add(moduleClass.getName());

            return this;
        }

        public Builder plugin(Class<? extends Plugin> plugin) {
            this.plugins.add(plugin);

            return this;
        }

        public Builder remote(String name, LocalCluster anotherCluster) {
            InetSocketAddress transportAddress = anotherCluster.localEsCluster.masterNode().getTransportAddress();

            nodeOverrideSettingsBuilder.putList("cluster.remote." + name + ".seeds",
                    transportAddress.getHostString() + ":" + transportAddress.getPort());

            return this;
        }

        public Builder users(TestSgConfig.User... users) {
            for (TestSgConfig.User user : users) {
                testSgConfig.user(user);
            }
            return this;
        }

        public Builder user(TestSgConfig.User user) {
            testSgConfig.user(user);
            return this;
        }

        public Builder user(String name, String password, String... sgRoles) {
            testSgConfig.user(name, password, sgRoles);
            return this;
        }

        public Builder user(String name, String password, Role... sgRoles) {
            testSgConfig.user(name, password, sgRoles);
            return this;
        }

        public Builder roles(Role... roles) {
            testSgConfig.roles(roles);
            return this;
        }

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public LocalCluster build() {
            try {
                if (sslEnabled) {
                    nodeOverrideSettingsBuilder
                            .put("searchguard.ssl.http.enabled", true)
                            .put("searchguard.ssl.transport.pemtrustedcas_filepath", testCertificates.getCaCertFile().getPath())
                            .put("searchguard.ssl.http.pemtrustedcas_filepath", testCertificates.getCaCertFile().getPath());

                }
                if (this.disabledModules.size() > 0) {
                    nodeOverrideSettingsBuilder.putList(SearchGuardModulesRegistry.DISABLED_MODULES.getKey(), this.disabledModules);
                }

                clusterName += "_" + num.incrementAndGet();

                return new LocalCluster(clusterName, resourceFolder, testSgConfig, nodeOverrideSettingsBuilder.build(), clusterConfiguration, plugins, testCertificates);
            } catch (Exception e) {
                log.error("Failed to build LocalCluster", e);
                throw new RuntimeException(e);
            }
        }
    }

}
