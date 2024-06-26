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

package com.floragunn.searchguard.legacy.test;

import static com.floragunn.searchguard.support.ConfigConstants.SEARCHGUARD_AUTHCZ_ADMIN_DN;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;

import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.helper.cluster.ClusterInfo;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public abstract class AbstractSGUnitTest {

    protected static final AtomicLong num = new AtomicLong();
    protected static boolean withRemoteCluster;

	static {

		System.out.println("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.arch") + " "
				+ System.getProperty("os.version"));
		System.out.println(
				"Java Version: " + System.getProperty("java.version") + " " + System.getProperty("java.vendor"));
		System.out.println("JVM Impl.: " + System.getProperty("java.vm.version") + " "
				+ System.getProperty("java.vm.vendor") + " " + System.getProperty("java.vm.name"));

		withRemoteCluster = Boolean.parseBoolean(System.getenv("TESTARG_unittests_with_remote_cluster"));
		System.out.println("With remote cluster: " + withRemoteCluster);
	}

	protected final Logger log = LogManager.getLogger(this.getClass());
    public static final ThreadPool MOCK_POOL = new ThreadPool(Settings.builder().put("node.name",  "mock").build());

    //TODO Test Matrix
    //enable//disable enterprise modules
    //1node and 3 node

	@Rule
	public TestName name = new TestName();

	@Rule
    public final TemporaryFolder repositoryPath = new TemporaryFolder();

	@Rule
	public final TestWatcher testWatcher = new SGTestWatcher();

	public static Header encodeBasicHeader(final String username, final String password) {
		return new BasicHeader("Authorization", "Basic "+Base64.getEncoder().encodeToString(
				(username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
	}

	@Deprecated
	protected static class TransportClientImpl extends TransportClient {

        public TransportClientImpl(Settings settings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, plugins);
        }

        public TransportClientImpl(Settings settings, Settings defaultSettings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, defaultSettings, plugins, null);
        }
    }

    @SafeVarargs
    protected static Collection<Class<? extends Plugin>> asCollection(Class<? extends Plugin>... plugins) {
        return Arrays.asList(plugins);
    }


    @Deprecated
    protected TransportClient getInternalTransportClient(ClusterInfo info, Settings initTransportClientSettings) {

        final String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

        Settings.Builder settingsBuilder = Settings.builder();

        settingsBuilder.put("cluster.name", info.clustername);
        settingsBuilder.put("searchguard.ssl.transport.enforce_hostname_verification", false);

        if (initTransportClientSettings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH) == null
                && initTransportClientSettings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH) == null) {
            try {
                settingsBuilder.put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"));
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Could not locate truststore for " + prefix);
            }
        }

        if (initTransportClientSettings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH) == null
                && initTransportClientSettings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH) == null) {
            try {
                settingsBuilder.put("searchguard.ssl.transport.keystore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(prefix + "kirk-keystore.jks"));
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Could not locate keystore for " + prefix);
            }
        }

        settingsBuilder.put(initTransportClientSettings);

        Settings tcSettings = settingsBuilder.build();

        TransportClient tc = new TransportClientImpl(tcSettings, asCollection(Netty4Plugin.class, SearchGuardPlugin.class));
        tc.addTransportAddress(new TransportAddress(new InetSocketAddress(info.nodeHost, info.nodePort)));
        return tc;
    }

    @Deprecated
    protected TransportClient getUserTransportClient(ClusterInfo info, String keyStore, Settings initTransportClientSettings) {

        try {
            final String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

            Settings tcSettings = Settings.builder().put("cluster.name", info.clustername)
                    .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"))
                    .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                    .put("searchguard.ssl.transport.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + keyStore))
                    .put(initTransportClientSettings).build();

            TransportClient tc = new TransportClientImpl(tcSettings, asCollection(Netty4Plugin.class, SearchGuardPlugin.class));
            tc.addTransportAddress(new TransportAddress(new InetSocketAddress(info.nodeHost, info.nodePort)));
            return tc;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    protected void initialize(Client tc, Settings initTransportClientSettings, DynamicSgConfig sgconfig) {

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

        Assert.assertTrue(tc.get(new GetRequest("searchguard", "config")).actionGet().isExists());
        Assert.assertTrue(tc.get(new GetRequest("searchguard", "internalusers")).actionGet().isExists());
        Assert.assertTrue(tc.get(new GetRequest("searchguard", "roles")).actionGet().isExists());
        Assert.assertTrue(tc.get(new GetRequest("searchguard", "rolesmapping")).actionGet().isExists());
        Assert.assertTrue(tc.get(new GetRequest("searchguard", "actiongroups")).actionGet().isExists());
        Assert.assertFalse(tc.get(new GetRequest("searchguard", "rolesmapping_xcvdnghtu165759i99465")).actionGet().isExists());
        Assert.assertTrue(tc.get(new GetRequest("searchguard", "config")).actionGet().isExists());

    }

    protected Settings.Builder minimumSearchGuardSettingsBuilder(int node, boolean sslOnly, boolean hasCustomTransportSettings) {
        try {
            final String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

            Settings.Builder builder = Settings.builder();

            if (!hasCustomTransportSettings) {
                builder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, "node-0")
                        .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH,
                                FileHelper.getAbsoluteFilePathFromClassPath(prefix + "node-0-keystore.jks"))
                        .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH,
                                FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"))
                        .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false);
            }

            if (!sslOnly) {
                builder.putList(SEARCHGUARD_AUTHCZ_ADMIN_DN, "CN=kirk,OU=client,O=client,l=tEst, C=De");
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
                return minimumSearchGuardSettingsBuilder(i, false, hasCustomTransportSettings(other)).put(other).build();
            }
        };
    }

    protected boolean hasCustomTransportSettings(Settings customSettings) {
        return customSettings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH) != null;
    }

    protected NodeSettingsSupplier minimumSearchGuardSettingsSslOnly(Settings other) {
        return new NodeSettingsSupplier() {
            @Override
            public Settings get(int i) {
                return minimumSearchGuardSettingsBuilder(i, true, hasCustomTransportSettings(other)).put(other).build();
            }
        };
    }

    protected void initialize(Client client) {
        initialize(client, Settings.EMPTY, new DynamicSgConfig());
    }

    protected void initialize(Client client, DynamicSgConfig dynamicSgConfig) {
        initialize(client, Settings.EMPTY, dynamicSgConfig);
    }

    protected final void assertContains(HttpResponse res, String pattern) {
        Assert.assertTrue(WildcardMatcher.match(pattern, res.getBody()));
    }

    protected final void assertNotContains(HttpResponse res, String pattern) {
        Assert.assertFalse(WildcardMatcher.match(pattern, res.getBody()));
    }

    protected String getResourceFolder() {
        return null;
    }
}
