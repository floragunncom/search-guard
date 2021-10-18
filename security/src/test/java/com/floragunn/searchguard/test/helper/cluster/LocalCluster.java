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

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.modules.SearchGuardModule;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.TestCertificateBasedSSLContextProvider;
import org.apache.http.Header;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.plugins.Plugin;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
    private LocalEsCluster localCluster;

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
        painlessWhitelistKludge();

        start();
    }

    @Override
    protected void before() throws Throwable {
        if (localCluster == null) {
            start();
        }
    }

    @Override
    protected void after() {
        if (localCluster != null && localCluster.isStarted()) {
            try {
                Thread.sleep(1234);
                localCluster.destroy();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                localCluster = null;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (localCluster != null && localCluster.isStarted()) {
            try {
                Thread.sleep(100);
                localCluster.destroy();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                localCluster = null;
            }
        }
    }

    public <X> X getInjectable(Class<X> clazz) {
        return this.localCluster.masterNode().getInjectable(clazz);
    }

    public PluginAwareNode node() {
        return this.localCluster.masterNode().esNode();
    }

    public List<LocalEsCluster.Node> nodes() {
        return this.localCluster.allNodes();
    }

    public LocalEsCluster.Node getNodeByName(String name) {
        return this.localCluster.getNodeByName(name);
    }

    public void updateSgConfig(CType configType, String key, Map<String, Object> value) {
        try (Client client = getAdminCertClient()) {
            log.info("Updating config " + configType + "." + key + ": " + value);

            GetResponse getResponse = client.get(new GetRequest("searchguard", configType.toLCString())).actionGet();
            String jsonDoc = new String(Base64.getDecoder().decode(String.valueOf(getResponse.getSource().get(configType.toLCString()))));
            NestedValueMap config = NestedValueMap.fromJsonString(jsonDoc);

            config.put(key, value);

            if (log.isTraceEnabled()) {
                log.trace("Updated config: " + config);
            }

            IndexResponse response = client
                    .index(new IndexRequest("searchguard").id(configType.toLCString()).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                            .source(configType.toLCString(), BytesReference.fromByteBuffer(ByteBuffer.wrap(config.toJsonString().getBytes("utf-8")))))
                    .actionGet();

            if (response.getResult() != DocWriteResponse.Result.UPDATED) {
                throw new RuntimeException("Updated failed " + response);
            }

            ConfigUpdateResponse configUpdateResponse = client
                    .execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]))).actionGet();

            if (configUpdateResponse.hasFailures()) {
                throw new RuntimeException("ConfigUpdateResponse produced failures: " + configUpdateResponse.failures());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void start() {
        try {
            if(testCertificates != null) {
                this.localCluster = new LocalEsCluster(clusterName, clusterConfiguration,
                        minimumSearchGuardSettingsSupplierFactory.minimumSearchGuardSettings(nodeOverride),
                        resourceFolder, plugins, new TestCertificateBasedSSLContextProvider(testCertificates.getCaCertificate(), testCertificates.getAdminCertificate(), false, true));
            } else {
                this.localCluster = new LocalEsCluster(clusterName, clusterConfiguration,
                        minimumSearchGuardSettingsSupplierFactory.minimumSearchGuardSettings(nodeOverride),
                        resourceFolder, plugins);
            }
            localCluster.start();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (testSgConfig != null) {
            initSearchGuardIndex(testSgConfig);
        }
    }

    private void painlessWhitelistKludge() {
        try {
            // TODO make this optional

            /*
             
            final ClassLoader classLoader = getClass().getClassLoader();
            
            try (PainlessPlugin p = new PainlessPlugin()) {
                p.loadExtensions(new ExtensionLoader() {
            
                    @SuppressWarnings("unchecked")
                    @Override
                    public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                        if (extensionPointType.equals(PainlessExtension.class)) {
                            List<?> result = StreamSupport.stream(ServiceLoader.load(PainlessExtension.class, classLoader).spliterator(), false)
                                    .collect(Collectors.toList());
            
                            return (List<T>) result;
                        } else {
                            return Collections.emptyList();
                        }
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
            */
        } catch (NoClassDefFoundError e) {

        }
    }

    private void initSearchGuardIndex(TestSgConfig testSgConfig) {
        log.info("Initializing Search Guard index");

        try (Client client = getAdminCertClient()) {

            testSgConfig.initIndex(client);

            Assert.assertTrue(client.get(new GetRequest("searchguard", "config")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "internalusers")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "roles")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "rolesmapping")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "actiongroups")).actionGet().isExists());
            Assert.assertFalse(client.get(new GetRequest("searchguard", "rolesmapping_xcvdnghtu165759i99465")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "config")).actionGet().isExists());
        }
    }

    @Override
    public Client getAdminCertClient() {
        if (testCertificates != null) {
            return new LocalEsClusterTransportClient(getClusterName(), getTransportAddress(), testCertificates.getAdminCertificate(),
                    testCertificates.getCaCertFile().toPath());
        }

        return EsClientProvider.super.getAdminCertClient();
    }

    @Override
    public String getResourceFolder() {
        return resourceFolder;
    }

    @Override
    public InetSocketAddress getHttpAddress() {
        return localCluster.clientNode().getHttpAddress();
    }

    @Override
    public InetSocketAddress getTransportAddress() {
        return localCluster.clientNode().getTransportAddress();
    }

    @Override
    public String getClusterName() {
        return localCluster.getClusterName();
    }

    @Override
    public SSLIOSessionStrategy getSSLIOSessionStrategy() {
        return localCluster.getSSLIOSessionStrategy();
    }

    @Override
    public Client getInternalNodeClient() {
        return localCluster.clientNode().getInternalNodeClient();
    }

    @Override
    public GenericRestClient createGenericClientRestClient(InetSocketAddress nodeHttpAddress, List<Header> headers) {
        if (testCertificates != null) {
            return new GenericRestClient(nodeHttpAddress, headers, new TestCertificateBasedSSLContextProvider(testCertificates.getCaCertificate(), testCertificates.getNodeCertificate(), false, true));

        }
        return EsClientProvider.super.createGenericClientRestClient(nodeHttpAddress, headers);
    }

    @Override
    public GenericRestClient createGenericAdminRestClient(InetSocketAddress nodeHttpAddress, List<Header> headers) {
        if (testCertificates != null) {
            return new GenericRestClient(nodeHttpAddress, headers, new TestCertificateBasedSSLContextProvider(testCertificates.getCaCertificate(), testCertificates.getAdminCertificate(), true, true));
        }
        return EsClientProvider.super.createGenericAdminRestClient(nodeHttpAddress, headers);
    }

    public static class Builder {
        private boolean sslEnabled;
        private String httpKeystoreFilepath = "node-0-keystore.jks";
        private String httpTruststoreFilepath = "truststore.jks";
        private String resourceFolder;
        private ClusterConfiguration clusterConfiguration = ClusterConfiguration.DEFAULT;
        private Settings.Builder nodeOverrideSettingsBuilder = Settings.builder();
        private List<String> disabledModules = new ArrayList<>();
        private List<Class<? extends Plugin>> plugins = new ArrayList<>();
        private TestSgConfig testSgConfig = new TestSgConfig().resources("/");
        private String clusterName = "local_cluster";
        private TestCertificates testCertificates;

        public Builder sslEnabled() {
            this.sslEnabled = true;
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
            this.clusterConfiguration = ClusterConfiguration.SINGLENODE;
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
            InetSocketAddress transportAddress = anotherCluster.localCluster.masterNode().getTransportAddress();

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
                if (testCertificates != null) {
                    File caCertFile = testCertificates.getCaCertFile();
                    nodeOverrideSettingsBuilder.put("searchguard.ssl.http.enabled", true)
                            .put("searchguard.ssl.transport.pemtrustedcas_filepath", caCertFile.getPath())
                            .put("searchguard.ssl.http.pemtrustedcas_filepath", caCertFile.getPath());

                } else if (sslEnabled) {
                    nodeOverrideSettingsBuilder.put("searchguard.ssl.http.enabled", true)
                            .put("searchguard.ssl.http.keystore_filepath",
                                    FileHelper.getAbsoluteFilePathFromClassPath(
                                            resourceFolder != null ? (resourceFolder + "/" + httpKeystoreFilepath) : httpKeystoreFilepath))
                            .put("searchguard.ssl.http.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(
                                    resourceFolder != null ? (resourceFolder + "/" + httpTruststoreFilepath) : httpTruststoreFilepath));
                }

                if (this.disabledModules.size() > 0) {
                    nodeOverrideSettingsBuilder.putList(SearchGuardModulesRegistry.DISABLED_MODULES.getKey(), this.disabledModules);
                }

                clusterName += "_" + num.incrementAndGet();

                return new LocalCluster(clusterName, resourceFolder, testSgConfig, nodeOverrideSettingsBuilder.build(), clusterConfiguration, plugins,
                        testCertificates);
            } catch (Exception e) {
                log.error("Failed to build LocalCluster", e);
                throw new RuntimeException(e);
            }

        }
    }

}
