package com.floragunn.searchsupport.jobs;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.PluginAwareNode;
import org.junit.rules.ExternalResource;

import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.ClusterHelper;
import com.floragunn.searchguard.test.helper.cluster.ClusterInfo;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public class LocalCluster extends ExternalResource {

    protected static final AtomicLong num = new AtomicLong();
    protected ClusterHelper clusterHelper = new ClusterHelper(
            "lc_utest_n", 0);
    protected ClusterInfo clusterInfo;
    protected final String resourceFolder;


    public LocalCluster(String resourceFolder, Settings nodeOverride, ClusterConfiguration clusterConfiguration) {
        this.resourceFolder = resourceFolder;

        setup(Settings.EMPTY,  nodeOverride, true, clusterConfiguration);
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
        return getNodeClient();
    }

    public Client getNodeClient() {
        return clusterHelper.nodeClient();
    }

    public Client getPrivilegedConfigNodeClient() {
        return PrivilegedConfigClient.adapt(getNodeClient());
    }

    private void setup(Settings initTransportClientSettings, Settings nodeOverride, boolean initSearchGuardIndex,
            ClusterConfiguration clusterConfiguration) {

        try {
            clusterInfo = clusterHelper.startCluster(minimumSearchGuardSettings(ccs(nodeOverride)), clusterConfiguration);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

       
    }

    

    private Settings ccs(Settings nodeOverride) throws Exception {

        return nodeOverride;
    }

    protected Settings.Builder minimumSearchGuardSettingsBuilder(int node, boolean sslOnly) {
        try {
            final String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

            Settings.Builder builder = Settings.builder()
                    .put("searchguard.ssl.transport.keystore_alias", "node-0")
                    .put("searchguard.ssl.transport.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "node-0-keystore.jks"))
                    .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"))
                    .put("searchguard.ssl.transport.enforce_hostname_verification", false);

            if (!sslOnly) {
                builder.putList("searchguard.authcz.admin_dn", "CN=kirk,OU=client,O=client,l=tEst, C=De");
                builder.put("searchguard.background_init_if_sgindex_not_exist", false);
            }

            return builder;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
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

                return new LocalCluster(resourceFolder, nodeOverrideSettingsBuilder.build(), clusterConfiguration);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

    }
}
