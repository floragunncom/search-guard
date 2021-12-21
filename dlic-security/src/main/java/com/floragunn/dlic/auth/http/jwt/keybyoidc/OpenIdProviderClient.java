/*
 * Copyright 2016-2020 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.jwt.keybyoidc;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;
import org.apache.cxf.rs.security.jose.jwk.JwkUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.cache.HttpCacheContext;
import org.apache.http.client.cache.HttpCacheStorage;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.cache.BasicHttpCacheStorage;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClients;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.documents.DocParseException;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.dlic.auth.http.jwt.oidc.json.OidcProviderConfig;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator.SSLConfig;

public class OpenIdProviderClient {
    private final static Logger log = LogManager.getLogger(KeySetRetriever.class);
    private static final long CACHE_STATUS_LOG_INTERVAL_MS = 60L * 60L * 1000L;

    private String openIdConnectEndpoint;
    private SSLConfig sslConfig;
    private ProxyConfig proxyConfig;
    private int requestTimeoutMs = 10000;
    private CacheConfig cacheConfig;
    private HttpCacheStorage oidcHttpCacheStorage;
    private int oidcCacheHits = 0;
    private int oidcCacheMisses = 0;
    private int oidcCacheHitsValidated = 0;
    private int oidcCacheModuleResponses = 0;
    private long oidcRequests = 0;
    private long lastCacheStatusLog = 0;

    OpenIdProviderClient(String openIdConnectEndpoint, SSLConfig sslConfig, ProxyConfig proxyConfig, boolean useCacheForOidConnectEndpoint) {
        this.openIdConnectEndpoint = openIdConnectEndpoint;
        this.sslConfig = sslConfig;
        this.proxyConfig = proxyConfig;

        if (useCacheForOidConnectEndpoint) {
            cacheConfig = CacheConfig.custom().setMaxCacheEntries(10).setMaxObjectSize(1024L * 1024L).build();
            oidcHttpCacheStorage = new BasicHttpCacheStorage(cacheConfig);
        }
    }

    public OidcProviderConfig getOidcConfiguration() {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<OidcProviderConfig>) () -> {
            try (CloseableHttpClient httpClient = createHttpClient(oidcHttpCacheStorage)) {

                HttpGet httpGet = new HttpGet(openIdConnectEndpoint);

                RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(getRequestTimeoutMs())
                        .setConnectTimeout(getRequestTimeoutMs()).setSocketTimeout(getRequestTimeoutMs()).build();

                httpGet.setConfig(requestConfig);

                HttpCacheContext httpContext = null;

                if (oidcHttpCacheStorage != null) {
                    httpContext = new HttpCacheContext();
                }

                try (CloseableHttpResponse response = httpClient.execute(httpGet, httpContext)) {
                    if (httpContext != null) {
                        logCacheResponseStatus(httpContext);
                    }

                    StatusLine statusLine = response.getStatusLine();

                    if (statusLine.getStatusCode() < 200 || statusLine.getStatusCode() >= 300) {
                        throw new AuthenticatorUnavailableException("Error while getting " + openIdConnectEndpoint + ": " + statusLine);
                    }

                    HttpEntity httpEntity = response.getEntity();

                    if (httpEntity == null) {
                        throw new AuthenticatorUnavailableException("Error while getting " + openIdConnectEndpoint + ": Empty response entity");
                    }

                    return new OidcProviderConfig(DocReader.json().readObject(httpEntity.getContent()));
                }

            } catch (DocParseException | IOException | UnexpectedDocumentStructureException e) {
                throw new AuthenticatorUnavailableException("Error while getting " + openIdConnectEndpoint + ": " + e, e);
            }
        });

    }

    public JsonWebKeys getJsonWebKeys() throws AuthenticatorUnavailableException {
        String uri = getJwksUri();

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<JsonWebKeys>) () -> {
            try (CloseableHttpClient httpClient = createHttpClient(null)) {

                HttpGet httpGet = new HttpGet(uri);

                RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(getRequestTimeoutMs())
                        .setConnectTimeout(getRequestTimeoutMs()).setSocketTimeout(getRequestTimeoutMs()).build();

                httpGet.setConfig(requestConfig);

                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    StatusLine statusLine = response.getStatusLine();

                    if (statusLine.getStatusCode() < 200 || statusLine.getStatusCode() >= 300) {
                        throw new AuthenticatorUnavailableException("Error while getting " + uri + ": " + statusLine);
                    }

                    HttpEntity httpEntity = response.getEntity();

                    if (httpEntity == null) {
                        throw new AuthenticatorUnavailableException("Error while getting " + uri + ": Empty response entity");
                    }

                    JsonWebKeys keySet = JwkUtils.readJwkSet(httpEntity.getContent());

                    return keySet;
                }
            } catch (IOException e) {
                throw new AuthenticatorUnavailableException("Error while getting " + uri + ": " + e, e);
            }
        });
    }

    public HttpResponse callTokenEndpoint(byte[] body, ContentType contentType) {
        OidcProviderConfig oidcProviderConfg = getOidcConfiguration();
        String tokenEndpoint = oidcProviderConfg.getTokenEndpoint();

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<HttpResponse>) () -> {
            try (CloseableHttpClient httpClient = createHttpClient(null)) {

                HttpPost httpPost = new HttpPost(tokenEndpoint);
                httpPost.setEntity(new ByteArrayEntity(body, contentType));

                RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(getRequestTimeoutMs())
                        .setConnectTimeout(getRequestTimeoutMs()).setSocketTimeout(getRequestTimeoutMs()).build();

                httpPost.setConfig(requestConfig);

                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    BasicHttpResponse copiedResponse = new BasicHttpResponse(response.getStatusLine());
                    copiedResponse.setEntity(
                            new ByteArrayEntity(EntityUtils.toByteArray(response.getEntity()), ContentType.getOrDefault(response.getEntity())));

                    return copiedResponse;
                }
            } catch (IOException e) {
                throw new AuthenticatorUnavailableException("Error while calling " + tokenEndpoint, e);
            }
        });
    }

    String getJwksUri() throws AuthenticatorUnavailableException {
        return getOidcConfiguration().getJwksUri();
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int httpTimeoutMs) {
        this.requestTimeoutMs = httpTimeoutMs;
    }

    private void logCacheResponseStatus(HttpCacheContext httpContext) {
        this.oidcRequests++;

        switch (httpContext.getCacheResponseStatus()) {
        case CACHE_HIT:
            this.oidcCacheHits++;
            break;
        case CACHE_MODULE_RESPONSE:
            this.oidcCacheModuleResponses++;
            break;
        case CACHE_MISS:
            this.oidcCacheMisses++;
            break;
        case VALIDATED:
            this.oidcCacheHitsValidated++;
            break;
        }

        long now = System.currentTimeMillis();

        if (this.oidcRequests >= 2 && now - lastCacheStatusLog > CACHE_STATUS_LOG_INTERVAL_MS) {
            log.info("Cache status for KeySetRetriever:\noidcCacheHits: " + oidcCacheHits + "\noidcCacheHitsValidated: " + oidcCacheHitsValidated
                    + "\noidcCacheModuleResponses: " + oidcCacheModuleResponses + "\noidcCacheMisses: " + oidcCacheMisses);

            lastCacheStatusLog = now;
        }

    }

    private CloseableHttpClient createHttpClient(HttpCacheStorage httpCacheStorage) {
        HttpClientBuilder builder;

        if (httpCacheStorage != null) {
            builder = CachingHttpClients.custom().setCacheConfig(cacheConfig).setHttpCacheStorage(httpCacheStorage);
        } else {
            builder = HttpClients.custom();
        }

        if (proxyConfig != null) {
            proxyConfig.apply(builder);
        }

        builder.useSystemProperties();

        if (sslConfig != null) {
            builder.setSSLSocketFactory(sslConfig.toSSLConnectionSocketFactory());
        }

        return builder.build();
    }

    public int getOidcCacheHits() {
        return oidcCacheHits;
    }

    public int getOidcCacheMisses() {
        return oidcCacheMisses;
    }

    public int getOidcCacheHitsValidated() {
        return oidcCacheHitsValidated;
    }

    public int getOidcCacheModuleResponses() {
        return oidcCacheModuleResponses;
    }
}
