/*
 * Copyright 2016-2021 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auth.saml;

import java.net.URI;

import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.opensaml.saml.metadata.resolver.impl.HTTPMetadataResolver;

import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.searchsupport.PrivilegedCode;

import net.shibboleth.shared.component.ComponentInitializationException;
import net.shibboleth.shared.resolver.ResolverException;
import net.shibboleth.shared.xml.impl.BasicParserPool;

public class SamlHTTPMetadataResolver extends HTTPMetadataResolver {
    private static int componentIdCounter = 0;
    private volatile ResolverException lastRefreshException;

    public SamlHTTPMetadataResolver(URI metadataUrl, TLSConfig tlsConfig) throws ResolverException {
        super(createHttpClient(tlsConfig), metadataUrl.toString());
        setId(SamlAuthenticator.class.getName() + "_" + (++componentIdCounter));
        setRequireValidMetadata(true);
        setFailFastInitialization(false);
        BasicParserPool basicParserPool = new BasicParserPool();
        try {
            basicParserPool.initialize();
        } catch (ComponentInitializationException e) {
            throw new RuntimeException(e);
        }
        setParserPool(basicParserPool);
    }

    @Override
    protected byte[] fetchMetadata() throws ResolverException {
        return PrivilegedCode.execute(() -> {
            byte[] result = SamlHTTPMetadataResolver.super.fetchMetadata();
            lastRefreshException = null;
            return result;
        }, ResolverException.class);
    }

    public void initializePrivileged() throws ComponentInitializationException {
        PrivilegedCode.execute(() -> {
            initialize();
        }, ComponentInitializationException.class);
    }

    private static HttpClient createHttpClient(TLSConfig tlsConfig) {
        return PrivilegedCode.execute(() -> {
            HttpClientBuilder builder = HttpClients.custom();

            builder.useSystemProperties();

            if (tlsConfig != null) {
                SSLConnectionSocketFactoryBuilder sslBuilder = SSLConnectionSocketFactoryBuilder.create()
                        .setSslContext(tlsConfig.getUnrestrictedSslContext())
                        .setHostnameVerifier(tlsConfig.getHostnameVerifier());
                String[] supportedProtocols = tlsConfig.getSupportedProtocols();
                if (supportedProtocols != null) {
                    sslBuilder.setTlsVersions(supportedProtocols);
                }
                String[] supportedCipherSuites = tlsConfig.getSupportedCipherSuites();
                if (supportedCipherSuites != null) {
                    sslBuilder.setCiphers(supportedCipherSuites);
                }
                SSLConnectionSocketFactory sslSocketFactory = sslBuilder.build();
                builder.setConnectionManager(PoolingHttpClientConnectionManagerBuilder.create()
                        .setSSLSocketFactory(sslSocketFactory)
                        .build());
            }

            return builder.build();
        });
    }

    public ResolverException getLastRefreshException() {
        return lastRefreshException;
    }

}
