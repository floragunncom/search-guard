package com.floragunn.searchsupport.jobs;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.ClusterHelper;
import com.floragunn.searchguard.test.helper.cluster.ClusterInfo;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ContextHeaderDecoratorClient;

public class LocalCluster extends ExternalResource {

    protected static final AtomicLong num = new AtomicLong();
    protected ClusterHelper clusterHelper = new ClusterHelper(
            "lc_utest_n" + num.incrementAndGet() + "_f" + System.getProperty("forkno") + "_t" + System.nanoTime());
    protected ClusterInfo clusterInfo;
    protected final String resourceFolder;

    public LocalCluster(String resourceFolder, ClusterConfiguration clusterConfiguration) throws Exception {
        this(resourceFolder, new DynamicSgConfig(), Settings.EMPTY, clusterConfiguration);
    }

    public LocalCluster(String resourceFolder, DynamicSgConfig dynamicSgSettings, Settings nodeOverride, ClusterConfiguration clusterConfiguration) {
        this.resourceFolder = resourceFolder;

        setup(Settings.EMPTY, dynamicSgSettings, nodeOverride, true, clusterConfiguration);
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

    public Client getNodeClientWithMockUser(User user) {
        Client client = getNodeClient();

        if (user != null) {
            client = new ContextHeaderDecoratorClient(client, ConfigConstants.SG_USER_HEADER, Base64Helper.serializeObject(user));
        }

        return client;
    }

    public Client getNodeClientWithMockUser(String userName, String... roles) {
        return getNodeClientWithMockUser(new User(userName, Arrays.asList(roles), null));
    }

    public Client getPrivilegedConfigNodeClient() {
        return new ContextHeaderDecoratorClient(getNodeClient(), ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
    }

    private void setup(Settings initTransportClientSettings, DynamicSgConfig dynamicSgSettings, Settings nodeOverride, boolean initSearchGuardIndex,
            ClusterConfiguration clusterConfiguration) {

        try {
            clusterInfo = clusterHelper.startCluster(minimumSearchGuardSettings(ccs(nodeOverride)), clusterConfiguration);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (initSearchGuardIndex && dynamicSgSettings != null) {
            initialize(dynamicSgSettings);
        }
    }

    protected void initialize(DynamicSgConfig sgconfig) {

        try (Client tc = getInternalClient()) {

            try {
                tc.admin().indices().create(new CreateIndexRequest("searchguard")).actionGet();
            } catch (Exception e) {
                //ignore
            }

            for (IndexRequest ir : sgconfig.getDynamicConfig(getResourceFolder())) {
                tc.index(ir).actionGet();
            }

            ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0])))
                    .actionGet();
            Assert.assertFalse(cur.failures().toString(), cur.hasFailures());
            Assert.assertEquals(clusterInfo.numNodes, cur.getNodes().size());

            Assert.assertTrue(tc.get(new GetRequest("searchguard", "config")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "internalusers")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "roles")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "rolesmapping")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "actiongroups")).actionGet().isExists());
            Assert.assertFalse(tc.get(new GetRequest("searchguard", "rolesmapping_xcvdnghtu165759i99465")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "config")).actionGet().isExists());
        }
    }

    private Settings ccs(Settings nodeOverride) throws Exception {

        return nodeOverride;
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

        public Builder sslEnabled() {
            this.sslEnabled = true;
            return this;
        }

        public Builder resources(String resourceFolder) {
            this.resourceFolder = resourceFolder;
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

        public Builder nodeSettings(Object... settings) {

            for (int i = 0; i < settings.length - 1; i += 2) {
                String key = String.valueOf(settings[i]);
                Object value = settings[i + 1];

                nodeOverrideSettingsBuilder.put(key, String.valueOf(value));
            }

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

                return new LocalCluster(resourceFolder, new DynamicSgConfig(), nodeOverrideSettingsBuilder.build(), clusterConfiguration);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

    }
}
