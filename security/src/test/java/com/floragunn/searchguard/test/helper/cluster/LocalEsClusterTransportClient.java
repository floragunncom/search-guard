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

import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * Old-style transport client for ES. Will be removed for ES 8.
 */
@Deprecated
public class LocalEsClusterTransportClient extends TransportClient {

    public LocalEsClusterTransportClient(String clusterName, InetSocketAddress host, String truststore, String keystore) {

        this(createSettings(clusterName, host, truststore, keystore), Arrays.asList(Netty4Plugin.class, SearchGuardPlugin.class));
        this.addTransportAddress(new TransportAddress(host));
    }

    public LocalEsClusterTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        super(settings, plugins);
    }

    public LocalEsClusterTransportClient(Settings settings, Settings defaultSettings, Collection<Class<? extends Plugin>> plugins) {
        super(settings, defaultSettings, plugins, null);
    }

    public LocalEsClusterTransportClient(String clusterName, InetSocketAddress host, TestCertificate certificate, Path caCertFilePath) {
        this(createSettings(clusterName, certificate, caCertFilePath), Arrays.asList(Netty4Plugin.class, SearchGuardPlugin.class));
        this.addTransportAddress(new TransportAddress(host));
    }

    private static Settings createSettings(String clusterName, InetSocketAddress host, String truststore, String keystore) {
        try {
            return Settings.builder().put("cluster.name", clusterName)
                    .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(truststore))
                    .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                    .put("searchguard.ssl.transport.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(keystore)).build();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static Settings createSettings(String clusterName, TestCertificate certificate, Path caCertFilePath) {
        Settings.Builder builder = Settings.builder().put("cluster.name", clusterName)
                .put("searchguard.ssl.transport.pemcert_filepath", certificate.getCertificateFile().getAbsolutePath())
                .put("searchguard.ssl.transport.pemkey_filepath", certificate.getPrivateKeyFile().getAbsolutePath())
                .put("searchguard.ssl.transport.pemtrustedcas_filepath", caCertFilePath)
                .put("searchguard.ssl.transport.enforce_hostname_verification", false);

        Optional.ofNullable(certificate.getPrivateKeyPassword())
                .ifPresent(privateKeyPassword -> builder
                        .put("searchguard.ssl.transport.pemkey_password", privateKeyPassword));

        return builder.build();
    }
}