/*
 * Copyright 2020-2021 floragunn GmbH
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

package com.floragunn.signals.watch.common;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;

public class HttpClientConfig extends WatchElement {
    private final Integer connectionTimeoutSecs;
    private final Integer readTimeoutSecs;
    private final TlsConfig tlsConfig;
    private final HttpProxyConfig proxyConfig;

    public HttpClientConfig(Integer connectionTimeoutSecs, Integer readTimeoutSecs, TlsConfig tlsConfig, HttpProxyConfig proxyConfig) {
        this.connectionTimeoutSecs = connectionTimeoutSecs;
        this.readTimeoutSecs = readTimeoutSecs;
        this.tlsConfig = tlsConfig;
        this.proxyConfig = proxyConfig;
    }

    public HttpClient createHttpClient(HttpProxyConfig defaultProxyConfig) {
        RequestConfig.Builder configBuilder = RequestConfig.custom();

        if (connectionTimeoutSecs != null) {
            configBuilder.setConnectTimeout(connectionTimeoutSecs * 1000);
            configBuilder.setConnectionRequestTimeout(connectionTimeoutSecs * 1000);
        } else {
            configBuilder.setConnectTimeout(10000);
            configBuilder.setConnectionRequestTimeout(10000);
        }

        if (readTimeoutSecs != null) {
            configBuilder.setSocketTimeout(readTimeoutSecs * 1000);
        } else {
            configBuilder.setSocketTimeout(10000);
        }

        RequestConfig config = configBuilder.build();

        HttpClientBuilder clientBuilder = HttpClients.custom().setDefaultRequestConfig(config);

        clientBuilder.useSystemProperties();
        
        // If no password is set, don't ask other components in the system for credentials
        clientBuilder.setDefaultCredentialsProvider(null);

        if (tlsConfig != null) {
            clientBuilder.setSSLSocketFactory(tlsConfig.toSSLConnectionSocketFactory());
        }
        
        HttpHost proxy = null;
        
        if (defaultProxyConfig != null) {
            proxy = defaultProxyConfig.getProxy();
        }
        
        if (proxyConfig != null) {            
            if (ProxyTypeProvider.Type.USE_INLINE_PROXY == proxyConfig.getType() ||
                    ProxyTypeProvider.Type.USE_STORED_PROXY == proxyConfig.getType()) {
                proxy = proxyConfig.getProxy();
            } else if (proxyConfig.getType() == ProxyTypeProvider.Type.USE_NO_PROXY) {
                proxy = null;
            }
        }
    
        if (proxy != null) {
            clientBuilder.setProxy(proxy);
        }

        try {
            SecurityManager sm = System.getSecurityManager();

            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }

            return AccessController.doPrivileged((PrivilegedExceptionAction<HttpClient>) () -> new HttpClient(clientBuilder.build()));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e.getCause());
        }

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (readTimeoutSecs != null) {
            builder.field("read_timeout", readTimeoutSecs);
        }

        if (connectionTimeoutSecs != null) {
            builder.field("connection_timeout", connectionTimeoutSecs);
        }

        if (tlsConfig != null) {
            builder.field("tls", tlsConfig);
        }
        
        if (proxyConfig != null && proxyConfig.getType() != ProxyTypeProvider.Type.USE_DEFAULT_PROXY) {
            proxyConfig.toXContent(builder, params);
        }

        return builder;
    }

    public static HttpClientConfig create(ValidatingDocNode jsonObject, TrustManagerRegistry trustManagerRegistry,
                                          HttpProxyHostRegistry httpProxyHostRegistry, ValidationLevel validationLevel) throws ConfigValidationException {
        Integer connectionTimeout = null;
        Integer readTimeout = null;
        TlsConfig tlsConfig = null;
        HttpProxyConfig proxyConfig = null;

        // TODO support units

        if (jsonObject.hasNonNull("read_timeout")) {
            readTimeout = jsonObject.get("read_timeout").asInteger();
        }

        if (jsonObject.hasNonNull("connection_timeout")) {
            connectionTimeout = jsonObject.get("connection_timeout").asInteger();
        }
        
        tlsConfig = jsonObject.get("tls").by(node -> TlsConfig.create(node, trustManagerRegistry, validationLevel));
        proxyConfig = HttpProxyConfig.create(jsonObject, httpProxyHostRegistry, validationLevel);

        return new HttpClientConfig(connectionTimeout, readTimeout, tlsConfig, proxyConfig);
    }

    public boolean isNull() {
        return connectionTimeoutSecs == null && readTimeoutSecs == null && tlsConfig == null && proxyConfig == null;
    }

    public HttpProxyConfig getProxyConfig() {
        return proxyConfig;
    }
    
    
}
