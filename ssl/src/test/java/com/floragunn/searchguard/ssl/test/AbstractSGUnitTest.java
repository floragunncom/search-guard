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

package com.floragunn.searchguard.ssl.test;

import java.io.FileNotFoundException;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.floragunn.searchguard.ssl.test.helper.file.FileHelper;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;

public abstract class AbstractSGUnitTest {

    protected static final AtomicLong num = new AtomicLong();

    static {
        System.out.println("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.arch") + " " + System.getProperty("os.version"));
        System.out.println("Java Version: " + System.getProperty("java.version") + " " + System.getProperty("java.vendor"));
        System.out.println("JVM Impl.: " + System.getProperty("java.vm.version") + " " + System.getProperty("java.vm.vendor") + " "
                + System.getProperty("java.vm.name"));
    }

    protected final Logger log = LogManager.getLogger(this.getClass());
    //public static final ThreadPool MOCK_POOL = new ThreadPool(Settings.builder().put("node.name", "mock").build());

    @Rule
    public TestName name = new TestName();

    @Rule
    public final TemporaryFolder repositoryPath = new TemporaryFolder();

    public static Header encodeBasicHeader(final String username, final String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
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

    protected String getResourceFolder() {
        return null;
    }
}
