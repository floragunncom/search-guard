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
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.plugins.Plugin;
import org.junit.rules.ExternalResource;

import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.fluent.collections.CheckList;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.RequestInfo;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.AuthTokenService;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.DlsFls;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.RoleMapping;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.TestSgConfig.UserPassword;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.google.common.base.Strings;

/**
 * Test resource class for starting ES clusters for integration tests.
 * 
 * There are two kinds of local clusters:
 * 
 * 1. The "JVM embedded cluster":
 *  
 * This is a cluster which is pulled up in the same JVM the tests are running in. You can force using an embedded cluster by calling embedded() on the Builder object.
 * 
 * This has some advantages: 
 * - It is possible to debug through the application code. 
 * - Code coverage can be measured.
 * - It is possible to use node clients (org.elasticsearch.client.Client) in test code
 * - It is possible to access injectables in test code
 * - It is possible to install mock plug-ins in the cluster
 * - You have greater control over the Search Guard configuration index; you can control its name and write invalid configuration into it.
 * 
 * The disadvantages are:
 * - The cluster does not completely match the real thing.
 * - Class loading is different, especially when it comes to "jar hell".
 * - No security manager is available.
 * - Only the core ES features are available.
 * 
 * 2. The "external process cluster":
 * 
 * This is an actual ES cluster. The infrastructure will automatically download the correct ES archive, provision the cluster nodes and execute them in separate processes. 
 * You can force using an external process cluster by calling useExternalProcessCluster() on the Builder object.
 * 
 * Advantages:
 * - As close as possible to a production environment.
 * - Complete ES feature set.
 * 
 * Disadvantages:
 * - Slower when tests are manually run. When run on CI in batches, the difference is negligible, though. 
 * - Maven is used for building the plugin. In local environments, this can interfere with the build logic of the IDE.
 * 
 * 
 * By default, tests are run using the JVM embedded cluster. You have a number of system properties available to control this:
 * 
 * - sg.tests.use_ep_cluster: Use -Dsg.tests.use_ep_cluster=true to force the external process cluster ("ep cluster") wherever possible. This is used on the CI.
 * - sg.tests.sg_plugin.file: Use -Dsg.tests.sg_plugin.file=/path/to/search-guard-flx-plugin.zip to specify a pre-built plugin to be used. If not specified, the test code will build the plugin by itself.
 * - sg.tests.es_download_cache.dir: Use -sg.tests.es_download_cache.dir=/path/to/dir to control the directory where ES downloads are stored. The code will re-use existing downloads.
 * 
 * 
 * Other things to keep in mind:
 * 
 * - The JVM embedded cluster will only run with the SG modules which are in the dependency list of the module where the test is located. 
 *   Thus, it is possible that Signals or enterprise modules are not present. The external process cluster will, however, run on a full build of Search Guard with all modules. 
 *   You can still use disableModule() and enableModule() to control these.
 * 
 *
 */
public class LocalCluster extends ExternalResource implements AutoCloseable, EsClientProvider {

    static final boolean USE_EXTERNAL_PROCESS_CLUSTER_BY_DEFAULT = "true".equalsIgnoreCase(System.getProperty("sg.tests.use_ep_cluster"));

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
    private final List<GenericRestClient.RequestInfo> executedRequests;
    private final boolean externalProcessCluster;
    private final ImmutableList<String> waitForComponents;

    private volatile LocalEsCluster localEsCluster;

    private LocalCluster(String clusterName, String resourceFolder, TestSgConfig testSgConfig, Settings nodeOverride,
            ClusterConfiguration clusterConfiguration, List<Class<? extends Plugin>> plugins, TestCertificates testCertificates,
            List<LocalCluster> clusterDependencies, Map<String, LocalCluster> remotes, List<TestIndex> testIndices, List<TestAlias> testAliases,
            boolean logRequests, boolean externalProcessCluster, ImmutableList<String> waitForComponents) {
        this.resourceFolder = resourceFolder;
        this.plugins = plugins;
        this.clusterConfiguration = clusterConfiguration;
        this.testSgConfig = testSgConfig;
        this.nodeOverride = nodeOverride;
        this.clusterName = clusterName;
        this.minimumSearchGuardSettingsSupplierFactory = new MinimumSearchGuardSettingsSupplierFactory(resourceFolder, testCertificates == null);
        this.testCertificates = testCertificates;
        this.remotes = remotes;
        this.clusterDependencies = clusterDependencies;
        this.testIndices = testIndices;
        this.testAliases = testAliases;
        this.executedRequests = logRequests ? new ArrayList<>(1000) : null;
        this.externalProcessCluster = externalProcessCluster || USE_EXTERNAL_PROCESS_CLUSTER_BY_DEFAULT;
        this.waitForComponents = waitForComponents;
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
        if (this.executedRequests != null && !this.executedRequests.isEmpty()) {
            System.out.println("\n=========\nExecuted requests:\n"
                    + this.executedRequests.stream().map(r -> r.toString()).collect(Collectors.joining("\n\n\n")));
        }

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
        if (this.executedRequests != null && !this.executedRequests.isEmpty()) {
            log.info("Executed requests:\n{}", this.executedRequests.stream().map(r -> r.toString()).collect(Collectors.joining("\n\n\n")));
        }

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

    public List<? extends LocalEsCluster.Node> nodes() {
        return this.localEsCluster.getAllNodes();
    }

    public LocalEsCluster.Node getNodeByName(String name) {
        return this.localEsCluster.getNodeByName(name);
    }

    public LocalEsCluster.Node getRandomClientNode() {
        return this.localEsCluster.randomClientNode();
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

        if (getResponse.isExists()) {
            return new String(Base64.getDecoder().decode(String.valueOf(getResponse.getSource().get(configType.toLCString()))));
        } else {
            return null;
        }
    }

    public boolean isStarted() {
        return localEsCluster != null;
    }

    public Random getRandom() {
        return localEsCluster.random;
    }

    protected void start() {
        try {
            if (externalProcessCluster) {
                startExternalProcessEsCluster();
            } else {
                startEmbeddedEsCluster();
            }

            waitForComponents();
        } catch (RuntimeException e) {
            if (this.localEsCluster != null) {
                this.localEsCluster.destroy();
            }

            throw e;
        }
    }

    protected JvmEmbeddedEsCluster startEmbeddedEsCluster() {
        try {
            JvmEmbeddedEsCluster result = new JvmEmbeddedEsCluster(clusterName, clusterConfiguration,
                    minimumSearchGuardSettingsSupplierFactory.minimumSearchGuardSettings(nodeOverride), plugins, testCertificates);
            result.start();

            this.localEsCluster = result;

            try (Client client = result.clientNode().getInternalNodeClient()) {
                for (TestIndex index : this.testIndices) {
                    index.create(client);
                }
                for (TestAlias alias : this.testAliases) {
                    alias.create(client);
                }
            }

            if (testSgConfig != null) {
                initSearchGuardIndex(result, testSgConfig);
            }

            return result;
        } catch (Exception e) {
            log.error("Local ES cluster start failed", e);
            throw new RuntimeException(e);
        }

    }

    protected ExternalProcessEsCluster startExternalProcessEsCluster() {
        try {
            ExternalProcessEsCluster result = new ExternalProcessEsCluster(clusterName, clusterConfiguration,
                    minimumSearchGuardSettingsSupplierFactory.minimumSearchGuardSettings(nodeOverride), testCertificates, testSgConfig);
            result.start();

            this.localEsCluster = result;

            try (GenericRestClient client = getAdminCertRestClient()) {
                for (TestIndex index : this.testIndices) {
                    index.create(client);
                }
                for (TestAlias alias : this.testAliases) {
                    alias.create(client);
                }
            }

            return result;
        } catch (Exception e) {
            log.error("Local ES cluster start failed", e);
            throw new RuntimeException(e);
        }
    }

    protected void waitForComponents() {
        log.debug("waitForComponents: {}", this.waitForComponents);
        waitForComponents(waitForComponents, this);
    }

    private void initSearchGuardIndex(JvmEmbeddedEsCluster jvmEmbeddedEsCluster, TestSgConfig testSgConfig) {
        log.info("Initializing Search Guard index");

        try (Client client = PrivilegedConfigClient.adapt(jvmEmbeddedEsCluster.clientNode().getInternalNodeClient())) {
            testSgConfig.initIndex(client);
        }
    }

    @Override
    public Consumer<RequestInfo> getRequestInfoConsumer() {
        return this.executedRequests != null ? (r) -> this.executedRequests.add(r) : null;
    }

    public static void waitForComponents(Collection<String> waitForComponents, EsClientProvider esClientProvider) {
        if (waitForComponents.isEmpty()) {
            return;
        }

        Instant timeoutReachedAt = Instant.now().plus(Duration.ofSeconds(30));
        GenericRestClient.HttpResponse lastErrorResponse = null;

        try (GenericRestClient client = esClientProvider.getAdminCertRestClient()) {
            CheckList<String> componentsCheckList = CheckList.create(ImmutableSet.of(waitForComponents));

            while (!componentsCheckList.isComplete()) {
                for (String component : componentsCheckList.getUncheckedElements()) {
                    GenericRestClient.HttpResponse response = client.get("/_searchguard/component/" + component + "/_health");

                    if (response.getStatusCode() == 200) {
                        String value = response.getBodyAsDocNode().findSingleValueByJsonPath("$.components[?(@.name==\"" + component + "\")].state",
                                String.class);

                        log.debug("State of component {}: {}", component, value);

                        if (value != null && value.contains("INITIALIZED")) {
                            componentsCheckList.check(component);
                        } else {
                            lastErrorResponse = response;
                        }
                    } else {
                        lastErrorResponse = response;
                    }
                }

                if (Instant.now().isAfter(timeoutReachedAt)) {
                    throw new RuntimeException("Component did not become initialized:\n" + componentsCheckList + "\n" + lastErrorResponse);
                }

                Thread.sleep(10);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static class Builder {

        private final Settings.Builder nodeOverrideSettingsBuilder = Settings.builder();
        private final Set<String> disabledModules = new HashSet<>(MODULES_DISABLED_BY_DEFAULT);
        private Map<String, LocalCluster> remoteClusters = new HashMap<>();
        private List<LocalCluster> clusterDependencies = new ArrayList<>();
        private List<TestIndex> testIndices = new ArrayList<>();
        private List<TestAlias> testAliases = new ArrayList<>();
        private String resourceFolder;
        private ClusterConfiguration clusterConfiguration = ClusterConfiguration.DEFAULT;
        private TestSgConfig testSgConfig = new TestSgConfig().resources("/");
        private String clusterName = "local_cluster";
        private TestCertificates testCertificates;
        private boolean enterpriseModulesEnabled;
        private boolean logRequests;
        private boolean externalProcessCluster;
        private ImmutableList<String> waitForComponents = ImmutableList.empty();

        public Builder sslEnabled() {
            sslEnabled(TestCertificates.builder().ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
                    .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard").addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
                    .addAdminClients("CN=admin-0.example.com;OU=SearchGuard;O=SearchGuard").build());
            return this;
        }

        public Builder sslEnabled(TestCertificates certificatesContext) {
            this.testCertificates = certificatesContext;
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

        public Builder authzDebug(boolean debug) {
            this.testSgConfig.authzDebug(debug);
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
            testSgConfig.user(name, UserPassword.of(password), sgRoles);
            return this;
        }

        public Builder user(String name, String password, Role... sgRoles) {
            testSgConfig.user(name, UserPassword.of(password), sgRoles);
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

        public Builder.Embedded embedded() {
            return new Builder.Embedded(this);
        }

        public Builder logRequests() {
            this.logRequests = true;
            return this;
        }

        public Builder useExternalProcessCluster() {
            this.externalProcessCluster = true;
            return this;
        }

        public Builder waitForComponents(String... components) {
            this.waitForComponents = this.waitForComponents.with(ImmutableList.ofArray(components));
            return this;
        }

        public LocalCluster build() {
            try {
                preBuild();

                return new LocalCluster(clusterName, resourceFolder, testSgConfig, nodeOverrideSettingsBuilder.build(), clusterConfiguration,
                        ImmutableList.empty(), testCertificates, clusterDependencies, remoteClusters, testIndices, testAliases, logRequests,
                        externalProcessCluster, waitForComponents);
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

        private void preBuild() {
            nodeOverrideSettingsBuilder.put("searchguard.enterprise_modules_enabled", enterpriseModulesEnabled);

            if (this.disabledModules.size() > 0) {
                nodeOverrideSettingsBuilder.putList(SearchGuardModulesRegistry.DISABLED_MODULES.getKey(), new ArrayList<>(this.disabledModules));
            }

            clusterName += "_" + num.incrementAndGet();
        }

        public static class Embedded {
            private final Builder delegate;
            private final List<Class<? extends Plugin>> plugins = new ArrayList<>();

            Embedded(Builder delegate) {
                this.delegate = delegate;
            }

            public Builder.Embedded configIndexName(String configIndexName) {
                delegate.testSgConfig.configIndexName(configIndexName);
                return this;
            }

            public Builder.Embedded plugin(Class<? extends Plugin> plugin) {
                this.plugins.add(plugin);
                return this;
            }

            public LocalCluster.Embedded build() {
                try {
                    delegate.preBuild();

                    return new LocalCluster.Embedded(delegate.clusterName, delegate.resourceFolder, delegate.testSgConfig,
                            delegate.nodeOverrideSettingsBuilder.build(), delegate.clusterConfiguration, this.plugins, delegate.testCertificates,
                            delegate.clusterDependencies, delegate.remoteClusters, delegate.testIndices, delegate.testAliases, delegate.logRequests,
                            delegate.waitForComponents);
                } catch (Exception e) {
                    log.error("Failed to build LocalCluster", e);
                    throw new RuntimeException(e);
                }
            }

            public LocalCluster.Embedded start() {
                LocalCluster.Embedded localCluster = build();
                localCluster.start();
                return localCluster;
            }

            // ===============================
            // Only delegate methods hereafter
            // ===============================

            public Builder.Embedded dependsOn(Object object) {
                delegate.dependsOn(object);
                return this;
            }

            public Builder.Embedded clusterConfiguration(ClusterConfiguration clusterConfiguration) {
                delegate.clusterConfiguration(clusterConfiguration);
                return this;
            }

            public Builder.Embedded ignoreUnauthorizedIndices(boolean ignoreUnauthorizedIndices) {
                delegate.ignoreUnauthorizedIndices(ignoreUnauthorizedIndices);
                return this;
            }

            public Builder.Embedded authzDebug(boolean debug) {
                delegate.authzDebug(debug);
                return this;
            }

            public Builder.Embedded nodeSettings(Object... settings) {
                delegate.nodeSettings(settings);
                return this;
            }

            public Builder.Embedded enterpriseModulesEnabled() {
                delegate.enterpriseModulesEnabled();
                return this;
            }

            public Builder.Embedded disableModule(Class<? extends SearchGuardModule> moduleClass) {
                delegate.disableModule(moduleClass);
                return this;
            }

            public Builder.Embedded enableModule(Class<? extends SearchGuardModule> moduleClass) {
                delegate.enableModule(moduleClass);
                return this;
            }

            public Builder.Embedded indices(TestIndex... indices) {
                delegate.indices(indices);
                return this;
            }

            public Builder.Embedded aliases(TestAlias... aliases) {
                delegate.aliases(aliases);
                return this;
            }

            public Builder.Embedded authc(Authc authc) {
                delegate.authc(authc);
                return this;
            }

            public Builder.Embedded dlsFls(DlsFls dlsfls) {
                delegate.dlsFls(dlsfls);
                return this;
            }

            public Builder.Embedded authTokenService(AuthTokenService authTokenService) {
                delegate.authTokenService(authTokenService);
                return this;
            }

            public Builder.Embedded clusterName(String clusterName) {
                delegate.clusterName(clusterName);
                return this;
            }

            public Builder.Embedded logRequests() {
                delegate.logRequests();
                return this;
            }

            public Builder.Embedded sslEnabled() {
                delegate.sslEnabled();
                return this;
            }

            public Builder.Embedded sslEnabled(TestCertificates certificatesContext) {
                delegate.sslEnabled(certificatesContext);
                return this;
            }

            public Builder.Embedded resources(String resourceFolder) {
                delegate.resources(resourceFolder);
                return this;
            }

            public Builder.Embedded singleNode() {
                delegate.singleNode();
                return this;
            }

            public Builder.Embedded sgConfig(TestSgConfig testSgConfig) {
                delegate.sgConfig(testSgConfig);
                return this;
            }

            public Builder.Embedded remote(String name, LocalCluster anotherCluster) {
                delegate.remote(name, anotherCluster);
                return this;
            }

            public Builder.Embedded users(User... users) {
                delegate.users(users);
                return this;
            }

            public Builder.Embedded user(User user) {
                delegate.user(user);
                return this;
            }

            public Builder.Embedded user(String name, String password, String... sgRoles) {
                delegate.user(name, password, sgRoles);
                return this;
            }

            public Builder.Embedded user(String name, String password, Role... sgRoles) {
                delegate.user(name, password, sgRoles);
                return this;
            }

            public Builder.Embedded roles(Role... roles) {
                delegate.roles(roles);
                return this;
            }

            public Builder.Embedded roleMapping(RoleMapping... mappings) {
                delegate.roleMapping(mappings);
                return this;
            }

            public Builder.Embedded roleToRoleMapping(Role role, String... backendRoles) {
                delegate.roleToRoleMapping(role, backendRoles);
                return this;
            }

            public Builder.Embedded waitForComponents(String... components) {
                delegate.waitForComponents(components);
                return this;
            }

            public Builder.Embedded var(String name, Supplier<Object> value) {
                delegate.var(name, value);
                return this;
            }
        }
    }

    public static class Embedded extends LocalCluster {
        private volatile JvmEmbeddedEsCluster jvmEmbeddedEsCluster;

        public Embedded(String clusterName, String resourceFolder, TestSgConfig testSgConfig, Settings nodeOverride,
                ClusterConfiguration clusterConfiguration, List<Class<? extends Plugin>> plugins, TestCertificates testCertificates,
                List<LocalCluster> clusterDependencies, Map<String, LocalCluster> remotes, List<TestIndex> testIndices, List<TestAlias> testAliases,
                boolean logRequests, ImmutableList<String> waitForComponents) {
            super(clusterName, resourceFolder, testSgConfig, nodeOverride, clusterConfiguration, plugins, testCertificates, clusterDependencies,
                    remotes, testIndices, testAliases, logRequests, true, waitForComponents);
        }

        @Override
        protected void start() {
            this.jvmEmbeddedEsCluster = startEmbeddedEsCluster();
            waitForComponents();
        }

        public Client getInternalNodeClient() {
            return this.jvmEmbeddedEsCluster.clientNode().getInternalNodeClient();
        }

        public Client getPrivilegedInternalNodeClient() {
            return PrivilegedConfigClient.adapt(getInternalNodeClient());
        }

        public <X> X getInjectable(Class<X> clazz) {
            return this.jvmEmbeddedEsCluster.getInjectable(clazz);
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
            try (Client client = PrivilegedConfigClient.adapt(this.getInternalNodeClient())) {
                String searchGuardIndex = getConfigIndexName();
                String configurationBackup = loadConfig(configTypeToRestore, client, searchGuardIndex);
                try {
                    return callable.call();
                } finally {
                    writeConfigToIndexAndReload(client, configTypeToRestore, searchGuardIndex, configurationBackup);
                }
            }
        }

        public void updateSgConfig(CType<?> configType, String key, Map<String, Object> value) {
            try (Client client = PrivilegedConfigClient.adapt(this.getInternalNodeClient())) {
                log.info("Updating config {}.{}:{}", configType, key, value);
                String searchGuardIndex = getConfigIndexName();

                String jsonDoc = loadConfig(configType, client, searchGuardIndex);
                NestedValueMap config = NestedValueMap.fromJsonString(jsonDoc);

                if (Strings.isNullOrEmpty(key)) {
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
        public void updateRolesConfig(Role... roles) {
            NestedValueMap nestedValueMap = new NestedValueMap();
            Arrays.stream(roles).map(Role::toJsonMap).forEach(nestedValueMap::putAllFromAnyMap);
            updateSgConfig(CType.ROLES, nestedValueMap);
        }

        /**
         * Add provided roles mapping to cluster configuration
         * @param mappings role mappings which will be added to configuration, validation of mappings is bypassed
         */
        public void updateRolesMappingsConfig(RoleMapping... mappings) {
            NestedValueMap nestedValueMap = new NestedValueMap();
            Arrays.stream(mappings).map(RoleMapping::toJsonMap).forEach(nestedValueMap::putAllFromAnyMap);
            updateSgConfig(CType.ROLESMAPPING, nestedValueMap);
        }

        @Override
        public List<JvmEmbeddedEsCluster.Node> nodes() {
            return this.jvmEmbeddedEsCluster.getAllNodes();
        }

        public PluginAwareNode node() {
            return ((JvmEmbeddedEsCluster.Node) this.jvmEmbeddedEsCluster.masterNode()).esNode();
        }

        private static void writeConfigToIndexAndReload(Client client, CType<?> configType, String searchGuardIndex, String config) {
            if (config != null) {
                IndexResponse response = client
                        .index(new IndexRequest(searchGuardIndex).id(configType.toLCString()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                .source(configType.toLCString(), BytesReference.fromByteBuffer(ByteBuffer.wrap(stringToUtfByteArray(config)))))
                        .actionGet();

                if (!ImmutableSet.of(DocWriteResponse.Result.UPDATED, DocWriteResponse.Result.CREATED).contains(response.getResult())) {
                    throw new RuntimeException("Updated failed " + response);
                }
            } else {
                client.delete(new DeleteRequest(searchGuardIndex).id(configType.toLCString()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                        .actionGet();
            }

            ConfigUpdateResponse configUpdateResponse = client
                    .execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]))).actionGet();

            if (configUpdateResponse.hasFailures()) {
                throw new RuntimeException("ConfigUpdateResponse produced failures: " + configUpdateResponse.failures());
            }
        }

    }

}
