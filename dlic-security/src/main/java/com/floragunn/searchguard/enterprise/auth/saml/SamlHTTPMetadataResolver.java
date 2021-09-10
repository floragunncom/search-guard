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
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.opensearch.SpecialPermission;
import org.opensaml.saml.metadata.resolver.impl.HTTPMetadataResolver;

import com.floragunn.codova.config.net.TLSConfig;

import net.shibboleth.utilities.java.support.component.ComponentInitializationException;
import net.shibboleth.utilities.java.support.resolver.ResolverException;
import net.shibboleth.utilities.java.support.xml.BasicParserPool;

public class SamlHTTPMetadataResolver extends HTTPMetadataResolver {
    private static int componentIdCounter = 0;

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
    
    public void initializePrivileged() throws ComponentInitializationException {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws ComponentInitializationException {
                    initialize();
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof ComponentInitializationException) {
                throw (ComponentInitializationException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    private static HttpClient createHttpClient(TLSConfig tlsConfig) {
        try {
            final SecurityManager sm = System.getSecurityManager();

            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }

            return AccessController.doPrivileged(new PrivilegedExceptionAction<HttpClient>() {
                @Override
                public HttpClient run() throws Exception {
                    return createHttpClient0(tlsConfig);
                }
            });
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static HttpClient createHttpClient0(TLSConfig tlsConfig) {

        HttpClientBuilder builder = HttpClients.custom();

        builder.useSystemProperties();

        if (tlsConfig != null) {
            builder.setSSLSocketFactory(tlsConfig.toSSLConnectionSocketFactory());
        }

        return builder.build();
    }

}
