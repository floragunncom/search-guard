/*
  * Copyright 2016-2021 by floragunn GmbH - All rights reserved
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

import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.searchsupport.PrivilegedCode;
import java.net.URI;
import net.shibboleth.utilities.java.support.component.ComponentInitializationException;
import net.shibboleth.utilities.java.support.resolver.ResolverException;
import net.shibboleth.utilities.java.support.xml.BasicParserPool;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.opensaml.saml.metadata.resolver.impl.HTTPMetadataResolver;

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
                builder.setSSLSocketFactory(tlsConfig.toSSLConnectionSocketFactory());
            }

            return builder.build();
        });
    }

    public ResolverException getLastRefreshException() {
        return lastRefreshException;
    }

}
