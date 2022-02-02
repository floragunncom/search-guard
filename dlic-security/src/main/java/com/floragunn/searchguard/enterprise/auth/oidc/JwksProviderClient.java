/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auth.oidc;

import java.io.IOException;
import java.net.URI;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;
import org.apache.cxf.rs.security.jose.jwk.JwkUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.SpecialPermission;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;

public class JwksProviderClient {

    private final TLSConfig tlsConfig;
    private final ProxyConfig proxyConfig;
    private int requestTimeoutMs = 10000;

    public JwksProviderClient(TLSConfig tlsConfig, ProxyConfig proxyConfig) {
        this.tlsConfig = tlsConfig;
        this.proxyConfig = proxyConfig;
    }

    public JsonWebKeys getJsonWebKeys(URI uri) throws AuthenticatorUnavailableException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<JsonWebKeys>) () -> getJsonWebKeysPrivileged(uri));
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof AuthenticatorUnavailableException) {
                throw (AuthenticatorUnavailableException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    private JsonWebKeys getJsonWebKeysPrivileged(URI uri) throws AuthenticatorUnavailableException {
        try (CloseableHttpClient httpClient = createHttpClient()) {

            HttpGet httpGet = new HttpGet(uri);

            RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(getRequestTimeoutMs())
                    .setConnectTimeout(getRequestTimeoutMs()).setSocketTimeout(getRequestTimeoutMs()).build();

            httpGet.setConfig(requestConfig);

            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                StatusLine statusLine = response.getStatusLine();

                if (statusLine.getStatusCode() < 200 || statusLine.getStatusCode() >= 300) {
                    throw new AuthenticatorUnavailableException("Error while retrieving JWKS OIDC config",
                            statusLine + (response.getEntity() != null ? "\n" + EntityUtils.toString(response.getEntity()) : "")).details("jwks_uri",
                                    uri);
                }

                HttpEntity httpEntity = response.getEntity();

                if (httpEntity == null) {
                    throw new AuthenticatorUnavailableException("Error while retrieving JWKS OIDC config", "Empty response").details("jwks_uri", uri);
                }

                JsonWebKeys keySet = JwkUtils.readJwkSet(httpEntity.getContent());

                return keySet;
            }
        } catch (IOException e) {
            throw new AuthenticatorUnavailableException("Error while retrieving JWKS OIDC config", e).details("jwks_uri", uri);
        }
    }

    private CloseableHttpClient createHttpClient() {
        HttpClientBuilder builder = HttpClients.custom();

        if (proxyConfig != null) {
            proxyConfig.apply(builder);
        }

        builder.useSystemProperties();

        if (tlsConfig != null) {
            builder.setSSLSocketFactory(tlsConfig.toSSLConnectionSocketFactory());
        }

        return builder.build();
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }
}
