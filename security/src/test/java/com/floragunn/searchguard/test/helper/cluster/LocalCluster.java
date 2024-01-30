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
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.floragunn.searchsupport.cstate.ComponentState;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.awaitility.Awaitility;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.ExtensiblePlugin.ExtensionLoader;
import org.elasticsearch.plugins.Plugin;
import org.hamcrest.Matchers;
import org.junit.rules.ExternalResource;

import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.RoleMapping;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;

import static org.hamcrest.MatcherAssert.assertThat;

public class LocalCluster extends ExternalResource implements AutoCloseable, EsClientProvider {

    private static final Logger log = LogManager.getLogger(LocalCluster.class);

    static {
        System.setProperty("sg.default_init.dir", new File("./sgconfig").getAbsolutePath());
    }

    private static final ImmutableSet<String> MODULES_DISABLED_BY_DEFAULT = ImmutableSet.of("com.floragunn.searchguard.authtoken.AuthTokenModule",
            "com.floragunn.signals.SignalsModule");

    protected static final AtomicLong num = new AtomicLong();

    protected final String resourceFolder;
    private final List<Class<? extends Plugin>> plugins;
    private final ClusterConfiguration clusterConfiguration;
    private final TestSgConfig testSgConfig;
    private Settings nodeOverride;
    private final String clusterName;
    private final MinimumSearchGuardSettingsSupplierFactory minimumSearchGuardSettingsSupplierFactory;
    private final TestCertificates testCertificates;
    private final List<LocalCluster> clusterDependencies;
    private final Map<String, LocalCluster> remotes;
    private final List<TestIndex> testIndices;
    private final List<TestAlias> testAliases;
    private volatile LocalEsCluster localEsCluster;

    private LocalCluster(String clusterName, String resourceFolder, TestSgConfig testSgConfig, Settings nodeOverride,
            ClusterConfiguration clusterConfiguration, List<Class<? extends Plugin>> plugins, TestCertificates testCertificates,
            List<LocalCluster> clusterDependencies, Map<String, LocalCluster> remotes, List<TestIndex> testIndices, List<TestAlias> testAliases) {
        this.resourceFolder = resourceFolder;
        this.plugins = plugins;
        this.clusterConfiguration = clusterConfiguration;
        this.testSgConfig = testSgConfig;
        this.nodeOverride = nodeOverride;
        this.clusterName = clusterName;
        this.minimumSearchGuardSettingsSupplierFactory = new MinimumSearchGuardSettingsSupplierFactory(resourceFolder, testCertificates);
        this.testCertificates = testCertificates;
        this.remotes = remotes;
        this.clusterDependencies = clusterDependencies;
        this.testIndices = testIndices;
        this.testAliases = testAliases;
        
        painlessWhitelistKludge();
    }

    @Override
    public void before() throws Throwable {
        if (localEsCluster == null) {
            for (LocalCluster dependency : clusterDependencies) {
                if (!dependency.isStarted()) {
                    dependency.before();
                }
            }

            for (Map.Entry<String, LocalCluster> entry : remotes.entrySet()) {
                @SuppressWarnings("resource")
                InetSocketAddress transportAddress = entry.getValue().localEsCluster.masterNode().getTransportAddress();
                nodeOverride = Settings.builder().put(nodeOverride)
                        .putList("cluster.remote." + entry.getKey() + ".seeds", transportAddress.getHostString() + ":" + transportAddress.getPort())
                        .build();
            }

            start();
        }
    }

    @Override
    protected void after() {
        if (localEsCluster != null && localEsCluster.isStarted()) {
            try {
                Thread.sleep(1234);
                localEsCluster.destroy();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                localEsCluster = null;
            }
        }
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

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public TestCertificates getTestCertificates() {
        return testCertificates;
    }

    @Override
    public InetSocketAddress getHttpAddress() {
        return localEsCluster.clientNode().getHttpAddress();
    }

    @Override
    public InetSocketAddress getTransportAddress() {
        return localEsCluster.clientNode().getTransportAddress();
    }

    public Client getInternalNodeClient() {
        return localEsCluster.clientNode().getInternalNodeClient();
    }

    public Client getPrivilegedInternalNodeClient() {
        return PrivilegedConfigClient.adapt(getInternalNodeClient());
    }

    public <X> X getInjectable(Class<X> clazz) {
        return this.localEsCluster.masterNode().getInjectable(clazz);
    }

    public PluginAwareNode node() {
        return this.localEsCluster.masterNode().esNode();
    }

    public List<LocalEsCluster.Node> nodes() {
        return this.localEsCluster.getAllNodes();
    }

    public LocalEsCluster.Node getNodeByName(String name) {
        return this.localEsCluster.getNodeByName(name);
    }

    public LocalEsCluster.Node getRandomClientNode() {
        return this.localEsCluster.randomClientNode();
    }

    public void updateSgConfig(CType<?> configType, String key, Map<String, Object> value) {

        try  {
            Client client = PrivilegedConfigClient.adapt(this.getInternalNodeClient());
            log.info("Updating config {}.{}:{}", configType, key, value);
            String searchGuardIndex = getConfigIndexName();

            String jsonDoc = loadConfig(configType, client, searchGuardIndex);
            NestedValueMap config = NestedValueMap.fromJsonString(jsonDoc);

            if(Strings.isNullOrEmpty(key)) {
                config.putAllFromAnyMap(value);
            } else {
                config.put(key, value);
            }

            if (log.isTraceEnabled()) {
                log.trace("Updated config: " + config);
            }

            writeConfigToIndexAndReload(client, configType, searchGuardIndex, config.toJsonString());

        } catch (IOException | DocumentParseException | UnexpectedDocumentStructureException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateSgConfig(CType<?> configType, Map<String, Object> value) {
        updateSgConfig(configType, null, value);
    }

    /**
     * Add provided roles to cluster configuration
     * @param roles roles which will be added to configuration, validation of roles is bypassed
     */
    public void updateRolesConfig(Role...roles) {
        NestedValueMap nestedValueMap = new NestedValueMap();
        Arrays.stream(roles).map(Role::toJsonMap).forEach(nestedValueMap::putAllFromAnyMap);
        updateSgConfig(CType.ROLES, nestedValueMap);
    }

    /**
     * Add provided roles mapping to cluster configuration
     * @param mappings role mappings which will be added to configuration, validation of mappings is bypassed
     */
    public void updateRolesMappingsConfig(RoleMapping...mappings) {
        NestedValueMap nestedValueMap = new NestedValueMap();
        Arrays.stream(mappings).map(RoleMapping::toJsonMap).forEach(nestedValueMap::putAllFromAnyMap);
        updateSgConfig(CType.ROLESMAPPING, nestedValueMap);
    }

    private static void writeConfigToIndexAndReload(Client client, CType<?> configType, String searchGuardIndex, String config) {
        DocWriteResponse response = client
                .index(new IndexRequest(searchGuardIndex).id(configType.toLCString()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(configType.toLCString(), BytesReference.fromByteBuffer(ByteBuffer.wrap(stringToUtfByteArray(config)))))
                .actionGet();

        if (response.getResult() != DocWriteResponse.Result.UPDATED) {
            throw new RuntimeException("Updated failed " + response);
        }

        ConfigUpdateResponse configUpdateResponse = client
                .execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]))).actionGet();

        if (configUpdateResponse.hasFailures()) {
            throw new RuntimeException("ConfigUpdateResponse produced failures: " + configUpdateResponse.failures());
        }
    }

    private static byte[] stringToUtfByteArray(String config) {
        try {
            return config.getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Cannot convert configuration string to byte array", e);
        }
    }

    private static String loadConfig(CType<?> configType, Client client, String searchGuardIndex) {
        GetResponse getResponse = client.get(new GetRequest(searchGuardIndex, configType.toLCString())).actionGet();
        return new String(Base64.getDecoder().decode(String.valueOf(getResponse.getSource().get(configType.toLCString()))));
    }

    private String getConfigIndexName() {
        ConfigurationRepository configRepository = getInjectable(ConfigurationRepository.class);
        return configRepository.getEffectiveSearchGuardIndex();
    }

    /**
     * Backup configuration of type <code>configTypeToRestore</code> before execution of {@link Callable}. Then
     * {@link Callable} from parameter <code>callable</code> is executed. After execution of {@link Callable} the
     * configuration is restored from backup created previously
     * @param configTypeToRestore type of configuration to back up and restore
     * @param callable action to be executed after configuration backup is created and before the backup is restored.
     */
    public <T> T callAndRestoreConfig(CType<?> configTypeToRestore, Callable<T> callable) throws Exception {
        Client client = PrivilegedConfigClient.adapt(this.getInternalNodeClient());
        String searchGuardIndex = getConfigIndexName();
        String configurationBackup = loadConfig(configTypeToRestore, client, searchGuardIndex);
        try {
            return callable.call();
        } finally {
            writeConfigToIndexAndReload(client, configTypeToRestore, searchGuardIndex, configurationBackup);
        }
    }

    public boolean isStarted() {
        return localEsCluster != null;
    }

    public Random getRandom() {
        return localEsCluster.getRandom();
    }

    private void start() {
        try {
            localEsCluster = new LocalEsCluster(clusterName, clusterConfiguration,
                    minimumSearchGuardSettingsSupplierFactory.minimumSearchGuardSettings(nodeOverride), plugins, testCertificates);
            localEsCluster.start();

            Client client = getInternalNodeClient();
            for (TestIndex index : this.testIndices) {
                index.create(client);
            }
            for (TestAlias alias : this.testAliases) {
                alias.create(client);
            }
            waitForSignalsInitialization();
        } catch (Exception e) {
            log.error("Local ES cluster start failed", e);
            throw new RuntimeException(e);
        }

        if (testSgConfig != null) {
            initSearchGuardIndex(testSgConfig);
        }
    }

    /**
     * Triggers loading of SPI extensions for the painless plugin. This is only effective when Painless is on the classpath. If Painless is not on the classpath, nothing will be done.
     */
    private void painlessWhitelistKludge() {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            Class<?> painlessExtensionClass = Class.forName("org.elasticsearch.painless.spi.PainlessExtension");
            ExtensiblePlugin painlessPlugin = (ExtensiblePlugin) Class.forName("org.elasticsearch.painless.PainlessPlugin").getConstructor()
                    .newInstance();

            painlessPlugin.loadExtensions(new ExtensionLoader() {

                @SuppressWarnings("unchecked")
                @Override
                public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                    if (extensionPointType.equals(painlessExtensionClass)) {
                        List<?> result = StreamSupport.stream(ServiceLoader.load(painlessExtensionClass, classLoader).spliterator(), false)
                                .collect(Collectors.toList());

                        return (List<T>) result;
                    } else {
                        return Collections.emptyList();
                    }
                }
            });
            
            ((Plugin) painlessPlugin).close();
        } catch (ClassNotFoundException e) {
            // Ignore this, as this is expected on projects without painless dependency
        } catch (Exception e) {
            log.error("Error while applying painlessWhitelistKludge", e);
        }
    }

    private void initSearchGuardIndex(TestSgConfig testSgConfig) {
        log.info("Initializing Search Guard index");

        Client client = PrivilegedConfigClient.adapt(this.getInternalNodeClient());
        testSgConfig.initIndex(client);
    }

    private void waitForSignalsInitialization() {
        if (!nodeOverride.getAsList(SearchGuardModulesRegistry.DISABLED_MODULES.getKey()).contains("com.floragunn.signals.SignalsModule")) {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(25))
                    .untilAsserted(() -> {
                        SearchGuardModulesRegistry modulesRegistry = getInjectable(SearchGuardModulesRegistry.class);
                        String signalsModuleName = "signals";
                        ComponentState state = modulesRegistry.getComponentState(signalsModuleName);
                        assertThat("Modules registry should contain signals module state", state, Matchers.notNullValue());
                        assertThat("Signals module should be initialized or disabled", state.getState(), Matchers.anyOf(
                                Matchers.equalTo(ComponentState.State.INITIALIZED), Matchers.equalTo(ComponentState.State.DISABLED)
                        ));
                    });
        }
    }

    public static class Builder {

        private final Settings.Builder nodeOverrideSettingsBuilder = Settings.builder();
        private final Set<String> disabledModules = new HashSet<>(MODULES_DISABLED_BY_DEFAULT);
        private final List<Class<? extends Plugin>> plugins = new ArrayList<>();
        private Map<String, LocalCluster> remoteClusters = new HashMap<>();
        private List<LocalCluster> clusterDependencies = new ArrayList<>();
        private List<TestIndex> testIndices = new ArrayList<>();
        private List<TestAlias> testAliases = new ArrayList<>();
        private boolean sslEnabled;
        private String resourceFolder;
        private ClusterConfiguration clusterConfiguration = ClusterConfiguration.DEFAULT;
        private TestSgConfig testSgConfig = new TestSgConfig().resources("/");
        private String clusterName = "local_cluster";
        private TestCertificates testCertificates;
        private boolean enterpriseModulesEnabled;

        public Builder sslEnabled() {
            sslEnabled(TestCertificates.builder().ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
                    .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard").addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
                    .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard").build());
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

        public Builder ignoreUnauthorizedIndices(boolean ignoreUnauthorizedIndices) {
            this.testSgConfig.ignoreUnauthorizedIndices(ignoreUnauthorizedIndices);
            return this;
        }

        public Builder sgConfigSettings(String keyPath, Object value, Object... more) {
            this.testSgConfig.sgConfigSettings(keyPath, value, more);
            return this;
        }

        public Builder nodeSettings(Object... settings) {
            for (int i = 0; i < settings.length - 1; i += 2) {
                String key = String.valueOf(settings[i]);
                Object value = settings[i + 1];

                if (value instanceof List) {
                    List<String> values = ((List<?>) value).stream().map(String::valueOf).collect(Collectors.toList());
                    nodeOverrideSettingsBuilder.putList(key, values);
                } else {
                    nodeOverrideSettingsBuilder.put(key, String.valueOf(value));
                }
            }

            return this;
        }

        public Builder enterpriseModulesEnabled() {
            this.enterpriseModulesEnabled = true;
            return this;
        }

        public Builder disableModule(Class<? extends SearchGuardModule> moduleClass) {
            this.disabledModules.add(moduleClass.getName());

            return this;
        }

        public Builder enableModule(Class<? extends SearchGuardModule> moduleClass) {
            this.disabledModules.remove(moduleClass.getName());

            return this;
        }

        public Builder plugin(Class<? extends Plugin> plugin) {
            this.plugins.add(plugin);

            return this;
        }

        public Builder remote(String name, LocalCluster anotherCluster) {
            remoteClusters.put(name, anotherCluster);

            clusterDependencies.add(anotherCluster);

            return this;
        }

        public Builder indices(TestIndex... indices) {
            this.testIndices.addAll(Arrays.asList(indices));
            return this;
        }

        public Builder aliases(TestAlias... aliases) {
            this.testAliases.addAll(Arrays.asList(aliases));
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

        public Builder roleMapping(RoleMapping... mappings) {
            testSgConfig.roleMapping(mappings);
            return this;
        }

        public Builder roleToRoleMapping(Role role, String... backendRoles) {
            testSgConfig.roleToRoleMapping(role, backendRoles);
            return this;
        }

        public Builder authc(TestSgConfig.Authc authc) {
            testSgConfig.authc(authc);
            return this;
        }

        public Builder frontendMultiTenancy(TestSgConfig.FrontendMultiTenancy frontendMultiTenancy) {
            testSgConfig.frontendMultiTenancy(frontendMultiTenancy);
            return this;
        }

        public Builder tenants(TestSgConfig.Tenant... tenants) {
            testSgConfig.tenants(tenants);
            return this;
        }

        public Builder dlsFls(TestSgConfig.DlsFls dlsfls) {
            testSgConfig.dlsFls(dlsfls);
            return this;
        }
        
        public Builder authTokenService(TestSgConfig.AuthTokenService authTokenService) {
            testSgConfig.authTokenService(authTokenService);
            return this;
        }

        public Builder var(String name, Supplier<Object> value) {
            testSgConfig.var(name, value);
            return this;
        }

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder configIndexName(String configIndexName) {
            testSgConfig.configIndexName(configIndexName);
            return this;
        }

        public LocalCluster build() {
            try {
                if (sslEnabled) {
                    nodeOverrideSettingsBuilder.put("searchguard.ssl.http.enabled", true)
                            .put("searchguard.ssl.transport.pemtrustedcas_filepath", testCertificates.getCaCertFile().getPath())
                            .put("searchguard.ssl.http.pemtrustedcas_filepath", testCertificates.getCaCertFile().getPath());

                }

                nodeOverrideSettingsBuilder.put("searchguard.enterprise_modules_enabled", enterpriseModulesEnabled);

                if (this.disabledModules.size() > 0) {
                    nodeOverrideSettingsBuilder.putList(SearchGuardModulesRegistry.DISABLED_MODULES.getKey(), new ArrayList<>(this.disabledModules));
                }

                clusterName += "_" + num.incrementAndGet();

                return new LocalCluster(clusterName, resourceFolder, testSgConfig, nodeOverrideSettingsBuilder.build(), clusterConfiguration, plugins,
                        testCertificates, clusterDependencies, remoteClusters, testIndices, testAliases);
            } catch (Exception e) {
                log.error("Failed to build LocalCluster", e);
                throw new RuntimeException(e);
            }
        }

        public LocalCluster start() {
            LocalCluster localCluster = build();

            localCluster.start();

            return localCluster;
        }
    }

}
