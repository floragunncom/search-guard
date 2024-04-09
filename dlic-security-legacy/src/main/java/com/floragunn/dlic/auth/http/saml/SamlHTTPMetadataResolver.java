/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.dlic.auth.http.saml;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import net.shibboleth.shared.component.ComponentInitializationException;
import net.shibboleth.shared.resolver.ResolverException;
import net.shibboleth.shared.xml.impl.BasicParserPool;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Settings;
import org.opensaml.saml.metadata.resolver.impl.HTTPMetadataResolver;

import com.floragunn.dlic.util.SettingsBasedSSLConfigurator;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator.SSLConfigException;


@Deprecated
public class SamlHTTPMetadataResolver extends HTTPMetadataResolver {
    private static int componentIdCounter = 0;

    SamlHTTPMetadataResolver(Settings esSettings, Path configPath) throws ResolverException, SSLConfigException, ComponentInitializationException {
        super(createHttpClient(esSettings, configPath), esSettings.get("idp.metadata_url"));
        setId(HTTPSamlAuthenticator.class.getName() + "_" + (++componentIdCounter));
        setRequireValidMetadata(true);
        setFailFastInitialization(false);
        setMinRefreshDelay(Duration.of(esSettings.getAsLong("idp.min_refresh_delay", 60L * 1000L), ChronoUnit.MILLIS));
        setMaxRefreshDelay(Duration.of(esSettings.getAsLong("idp.max_refresh_delay", 14400000L), ChronoUnit.MILLIS));
        setRefreshDelayFactor(esSettings.getAsFloat("idp.refresh_delay_factor", 0.75f));
        BasicParserPool basicParserPool = new BasicParserPool();
        basicParserPool.initialize();
        setParserPool(basicParserPool);
    }

    @Override
    protected byte[] fetchMetadata() throws ResolverException {
        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<byte[]>() {
                @Override
                public byte[] run() throws ResolverException {
                    return SamlHTTPMetadataResolver.super.fetchMetadata();
                }
            });
        } catch (PrivilegedActionException e) {

            if (e.getCause() instanceof ResolverException) {
                throw (ResolverException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static SettingsBasedSSLConfigurator.SSLConfig getSSLConfig(Settings settings, Path configPath) throws SSLConfigException {
        return new SettingsBasedSSLConfigurator(settings, configPath, "idp").buildSSLConfig();
    }

    private static HttpClient createHttpClient(Settings settings, Path configPath) throws SSLConfigException {
        try {
            final SecurityManager sm = System.getSecurityManager();

            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }

            return AccessController.doPrivileged(new PrivilegedExceptionAction<HttpClient>() {
                @Override
                public HttpClient run() throws Exception {
                    return createHttpClient0(settings, configPath);
                }
            });
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof SSLConfigException) {
                throw (SSLConfigException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static HttpClient createHttpClient0(Settings settings, Path configPath) throws SSLConfigException {

        HttpClientBuilder builder = HttpClients.custom();

        builder.useSystemProperties();

        SettingsBasedSSLConfigurator.SSLConfig sslConfig = getSSLConfig(settings, configPath);

        if (sslConfig != null) {
            PoolingHttpClientConnectionManagerBuilder clientConnectionManagerBuilder = PoolingHttpClientConnectionManagerBuilder.create();
            SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(
                    sslConfig.getUnrestrictedSslContext(), sslConfig.getSupportedProtocols(),
                    sslConfig.getSupportedCipherSuites(), sslConfig.getHostnameVerifier()
            );
            clientConnectionManagerBuilder.setSSLSocketFactory(sslConnectionSocketFactory);
            builder.setConnectionManager(clientConnectionManagerBuilder.build());
        }

        return builder.build();
    }

}
