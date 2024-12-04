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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Objects;

import com.floragunn.searchguard.support.ConfigConstants;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public abstract class AbstractSGUnitTest {

    //protected static final AtomicLong num = new AtomicLong();
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
    public static final ThreadPool MOCK_POOL = new ThreadPool(Settings.builder().put("node.name",  "mock").build(), MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());

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

    @SafeVarargs
    protected static Collection<Class<? extends Plugin>> asCollection(Class<? extends Plugin>... plugins) {
        return Arrays.asList(plugins);
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
        initialize(client, new DynamicSgConfig());
    }

    protected void initialize(Client tc, DynamicSgConfig sgconfig) {
        try (ThreadContext.StoredContext ctx = tc.threadPool().getThreadContext().stashContext()) {
            tc.threadPool().getThreadContext().putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");

            try {
                tc.admin().indices().create(new CreateIndexRequest("searchguard")).actionGet();
            } catch (Exception e) {
                //ignore
            }

            for (IndexRequest ir : sgconfig.getDynamicConfig(getResourceFolder())) {
                DocWriteResponse res = tc.index(ir).actionGet();
            }

            ConfigUpdateResponse cur = tc
                    .execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0])))
                    .actionGet();

            Assert.assertFalse(cur.failures().toString(), cur.hasFailures());


            SearchResponse sr = tc.search(new SearchRequest("searchguard")).actionGet();
            sr.decRef();

            sr = tc.search(new SearchRequest("searchguard")).actionGet();
            sr.decRef();

            Assert.assertTrue(tc.get(new GetRequest("searchguard", "config")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "internalusers")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "roles")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "rolesmapping")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "actiongroups")).actionGet().isExists());
            Assert.assertFalse(tc.get(new GetRequest("searchguard", "rolesmapping_xcvdnghtu165759i99465")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard", "config")).actionGet().isExists());

        }
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

    protected final String getType() {
        return "_doc";
    }
}
