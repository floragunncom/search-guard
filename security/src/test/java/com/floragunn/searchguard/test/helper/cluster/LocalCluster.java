package com.floragunn.searchguard.test.helper.cluster;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.net.ssl.SSLContext;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.plugins.ExtensiblePlugin.ExtensionLoader;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.modules.SearchGuardModule;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.client.ContextHeaderDecoratorClient;

public class LocalCluster extends ExternalResource implements AutoCloseable {

    private static final ResourceConfig<CType> DEFAULT_RESOURCE_CONFIG_SG_CONFIG = new ResourceConfig<CType>()
            .setYamlDoc(CType.CONFIG, new File("sg_config.yml"))//
            .setYamlDoc(CType.ROLES, new File("sg_roles.yml"))//
            .setYamlDoc(CType.ROLESMAPPING, new File("sg_roles_mapping.yml"))//
            .setYamlDoc(CType.ACTIONGROUPS, new File("sg_action_groups.yml"))//
            .setYamlDoc(CType.TENANTS, new File("sg_roles_tenants.yml"))//
            .setYamlDoc(CType.INTERNALUSERS, new File("sg_internal_users.yml"))//
            .setYamlDoc(CType.BLOCKS, new File("sg_blocks.yml"));

    protected static final AtomicLong num = new AtomicLong();
    protected ClusterHelper clusterHelper = new ClusterHelper(
            "lc_utest_n" + num.incrementAndGet() + "_f" + System.getProperty("forkno") + "_t" + System.nanoTime());
    protected ClusterInfo clusterInfo;
    protected final String resourceFolder;
    private List<Class<? extends Plugin>> plugins;

    public LocalCluster(String resourceFolder, TestSgConfig testSgConfig, Settings nodeOverride, ClusterConfiguration clusterConfiguration,
            List<Class<? extends Plugin>> plugins) {
        this.resourceFolder = resourceFolder;
        this.plugins = plugins;

        setup(Settings.EMPTY, testSgConfig, nodeOverride, true, clusterConfiguration);
    }

    @Override
    protected void after() {
        if (clusterInfo != null) {
            try {
                Thread.sleep(1234);
                clusterHelper.stopCluster();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (clusterInfo != null) {
            try {
                Thread.sleep(100);
                clusterHelper.stopCluster();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public RestHelper restHelper() {
        return new RestHelper(clusterInfo, getResourceFolder());
    }

    public RestHelper restHelper(String keyStore) {
        RestHelper result = restHelper();

        result.keystore = keyStore;
        result.sendHTTPClientCertificate = true;

        return result;
    }

    public RestHelper nonSslRestHelper() {
        return new RestHelper(clusterInfo, false, false, getResourceFolder());
    }

    public <X> X getInjectable(Class<X> clazz) {
        return this.clusterHelper.node().injector().getInstance(clazz);
    }

    public PluginAwareNode node() {
        return this.clusterHelper.node();
    }

    public List<PluginAwareNode> allNodes() {
        return this.clusterHelper.allNodes();
    }

    public Client getInternalClient() {
        final String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

        Settings tcSettings = Settings.builder().put("cluster.name", clusterInfo.clustername)
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "kirk-keystore.jks"))
                .build();

        TransportClient tc = new TransportClientImpl(tcSettings, Arrays.asList(Netty4Plugin.class, SearchGuardPlugin.class));
        tc.addTransportAddress(new TransportAddress(new InetSocketAddress(clusterInfo.nodeHost, clusterInfo.nodePort)));
        return tc;
    }

    public Client getNodeClient() {
        final String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

        Settings tcSettings = Settings.builder().put("cluster.name", clusterInfo.clustername)
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "node-0-keystore.jks"))
                .build();

        TransportClient tc = new TransportClientImpl(tcSettings, Arrays.asList(Netty4Plugin.class, SearchGuardPlugin.class));
        tc.addTransportAddress(new TransportAddress(new InetSocketAddress(clusterInfo.nodeHost, clusterInfo.nodePort)));
        return tc;
    }
    
    public GenericRestClient getRestClient(TestSgConfig.User user) {
        return getRestClient(user.getName(), user.getPassword());
    }
    
    public GenericRestClient getRestClient(String user, String password) {
        BasicHeader basicAuthHeader =  new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((user + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
        
        return new GenericRestClient(clusterInfo, Collections.singletonList(basicAuthHeader), getResourceFolder());
    }

    public RestHighLevelClient getRestHighLevelClient(TestSgConfig.User user) {
        return getRestHighLevelClient(user.getName(), user.getPassword());
    }

    public RestHighLevelClient getRestHighLevelClient(String user, String password) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(clusterInfo.httpHost, clusterInfo.httpPort, "https")).setHttpClientConfigCallback(
                httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLStrategy(getSSLIOSessionStrategy()));

        return new RestHighLevelClient(builder);
    }

    public RestHighLevelClient getRestHighLevelClient(String user, String password, String tenant) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(clusterInfo.httpHost, clusterInfo.httpPort, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        .setSSLStrategy(getSSLIOSessionStrategy()).addInterceptorLast(new HttpRequestInterceptor() {

                            @Override
                            public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
                                request.setHeader("sgtenant", tenant);

                            }

                        }));

        return new RestHighLevelClient(builder);
    }

    public RestHighLevelClient getRestHighLevelClient(Header... headers) {

        RestClientBuilder builder = RestClient.builder(new HttpHost(clusterInfo.httpHost, clusterInfo.httpPort, "https")).setDefaultHeaders(headers)
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(getSSLIOSessionStrategy()));

        return new RestHighLevelClient(builder);
    }

    public RestHighLevelClient getRestHighLevelClientForNode(int node, Header... headers) {

        RestClientBuilder builder = RestClient
                .builder(new HttpHost(clusterInfo.httpAdresses.get(node).getAddress(), clusterInfo.httpAdresses.get(node).getPort(), "https"))
                .setDefaultHeaders(headers)
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(getSSLIOSessionStrategy()));

        return new RestHighLevelClient(builder);
    }

    public Client getNodeClientWithMockUser(User user) {
        Client client = getNodeClient();

        if (user != null) {
            client = new ContextHeaderDecoratorClient(client, ConfigConstants.SG_USER_HEADER, Base64Helper.serializeObject(user));
        }

        return client;
    }

    public Client getNodeClientWithMockUser(String userName, String... roles) {
        return getNodeClientWithMockUser(User.forUser(userName).backendRoles(roles).build());
    }

    public Client getPrivilegedConfigNodeClient() {
        return new ContextHeaderDecoratorClient(getNodeClient(), ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
    }

    private void setup(Settings initTransportClientSettings, TestSgConfig testSgConfig, Settings nodeOverride, boolean initSearchGuardIndex,
            ClusterConfiguration clusterConfiguration) {
        painlessWhitelistKludge();

        try {
            clusterInfo = clusterHelper.startCluster(minimumSearchGuardSettings(ccs(nodeOverride)), clusterConfiguration, plugins, 10, null);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (initSearchGuardIndex && testSgConfig != null) {
            initialize(testSgConfig);
        }
    }

    private void painlessWhitelistKludge() {
        try {
            // TODO make this optional

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
        } catch (NoClassDefFoundError e) {

        }
    }

    protected void initialize(TestSgConfig testSgConfig) {

        try (Client client = getInternalClient()) {

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

    private Settings ccs(Settings nodeOverride) throws Exception {

        return nodeOverride;
    }

    private SSLContext getSSLContext() {
        try {
            String truststoreType = "JKS";
            String truststorePassword = "changeit";
            String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

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

    private SSLIOSessionStrategy getSSLIOSessionStrategy() {

        return new SSLIOSessionStrategy(getSSLContext(), null, null, NoopHostnameVerifier.INSTANCE);
    }

    protected Settings.Builder minimumSearchGuardSettingsBuilder(int node, boolean sslOnly) {

        final String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

        Settings.Builder builder = Settings.builder()
                //.put("searchguard.ssl.transport.enabled", true)
                //.put("searchguard.no_default_init", true)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLE_OPENSSL_IF_AVAILABLE, false)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLE_OPENSSL_IF_AVAILABLE, false)
                .put("searchguard.ssl.transport.keystore_alias", "node-0")
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "node-0-keystore.jks"))
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false);

        if (!sslOnly) {
            builder.putList("searchguard.authcz.admin_dn", "CN=kirk,OU=client,O=client,l=tEst, C=De");
            builder.put(ConfigConstants.SEARCHGUARD_BACKGROUND_INIT_IF_SGINDEX_NOT_EXIST, false);
        }

        return builder;
    }

    protected NodeSettingsSupplier minimumSearchGuardSettings(Settings other) {
        return new NodeSettingsSupplier() {
            @Override
            public Settings get(int i) {
                return minimumSearchGuardSettingsBuilder(i, false).put(other).build();
            }
        };
    }

    protected NodeSettingsSupplier minimumSearchGuardSettingsSslOnly(Settings other) {
        return new NodeSettingsSupplier() {
            @Override
            public Settings get(int i) {
                return minimumSearchGuardSettingsBuilder(i, true).put(other).build();
            }
        };
    }

    public String getResourceFolder() {
        return resourceFolder;
    }

    protected static class TransportClientImpl extends TransportClient {

        public TransportClientImpl(Settings settings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, plugins);
        }

        public TransportClientImpl(Settings settings, Settings defaultSettings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, defaultSettings, plugins, null);
        }
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
        private TestSgConfig testSgConfig = new TestSgConfig();

        public Builder sslEnabled() {
            this.sslEnabled = true;
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
            nodeOverrideSettingsBuilder.putList("cluster.remote." + name + ".seeds",
                    anotherCluster.clusterInfo.nodeHost + ":" + anotherCluster.clusterInfo.nodePort);

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

        public LocalCluster build() {
            try {

                if (sslEnabled) {
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

                return new LocalCluster(resourceFolder, testSgConfig, nodeOverrideSettingsBuilder.build(), clusterConfiguration, plugins);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }

}
