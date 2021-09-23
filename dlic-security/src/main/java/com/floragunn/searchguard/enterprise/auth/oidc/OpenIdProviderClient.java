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

package com.floragunn.searchguard.enterprise.auth.oidc;

import java.io.IOException;
import java.net.URI;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLHandshakeException;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;
import org.apache.cxf.rs.security.jose.jwk.JwkUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.cache.HttpCacheContext;
import org.apache.http.client.cache.HttpCacheStorage;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
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
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.dlic.auth.http.jwt.oidc.json.OidcProviderConfig;
import com.floragunn.searchguard.auth.AuthenticatorUnavailableException;
import com.floragunn.searchsupport.tls.SSLExceptions;

public class OpenIdProviderClient {
    private final static Logger log = LogManager.getLogger(KeySetRetriever.class);
    private static final long CACHE_STATUS_LOG_INTERVAL_MS = 60L * 60L * 1000L;

    private URI openIdConnectEndpoint;
    private TLSConfig tlsConfig;
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

    public OpenIdProviderClient(URI openIdConnectEndpoint, TLSConfig tlsConfig, ProxyConfig proxyConfig, boolean useCacheForOidConnectEndpoint) {
        this.openIdConnectEndpoint = openIdConnectEndpoint;
        this.tlsConfig = tlsConfig;
        this.proxyConfig = proxyConfig;

        if (useCacheForOidConnectEndpoint) {
            cacheConfig = CacheConfig.custom().setMaxCacheEntries(10).setMaxObjectSize(1024L * 1024L).build();
            oidcHttpCacheStorage = new BasicHttpCacheStorage(cacheConfig);
        }
    }

    public OidcProviderConfig getOidcConfiguration() throws AuthenticatorUnavailableException {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<OidcProviderConfig>) () -> {
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
                            throw new AuthenticatorUnavailableException("Error while getting " + openIdConnectEndpoint + ": " + statusLine
                                    + (response.getEntity() != null ? "\n" + EntityUtils.toString(response.getEntity()) : ""));
                        }

                        HttpEntity httpEntity = response.getEntity();

                        if (httpEntity == null) {
                            throw new AuthenticatorUnavailableException("Error while getting " + openIdConnectEndpoint + ": Empty response entity");
                        }

                        return new OidcProviderConfig(DocReader.json().readObject(httpEntity.getContent()));
                    }
                } catch (SSLHandshakeException e) {
                    throw new AuthenticatorUnavailableException("Error while getting " + openIdConnectEndpoint + ": " + SSLExceptions.toHumanReadableError(e), e);
                } catch (IOException e) {
                    throw new AuthenticatorUnavailableException("Error while getting " + openIdConnectEndpoint + ": " + e, e);
                }
            });
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

    public JsonWebKeys getJsonWebKeys() throws AuthenticatorUnavailableException {
        String uri = getJwksUri();

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<JsonWebKeys>) () -> {
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

    public TokenResponse callTokenEndpoint(String clientId, String clientSecret, String scope, String authCode, String redirectUri)
            throws AuthenticatorUnavailableException {
        List<NameValuePair> request = Arrays.asList(new BasicNameValuePair("client_id", clientId),
                new BasicNameValuePair("client_secret", clientSecret), new BasicNameValuePair("grant_type", "authorization_code"),
                new BasicNameValuePair("code", authCode), new BasicNameValuePair("redirect_uri", redirectUri));

        OidcProviderConfig oidcProviderConfg = getOidcConfiguration();
        String tokenEndpoint = oidcProviderConfg.getTokenEndpoint();

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<TokenResponse>) () -> {
                try (CloseableHttpClient httpClient = createHttpClient(null)) {

                    HttpPost httpPost = new HttpPost(tokenEndpoint);
                    httpPost.setEntity(new UrlEncodedFormEntity(request, "utf-8"));

                    RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(getRequestTimeoutMs())
                            .setConnectTimeout(getRequestTimeoutMs()).setSocketTimeout(getRequestTimeoutMs()).build();

                    httpPost.setConfig(requestConfig);

                    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                        String responseBody = EntityUtils.toString(response.getEntity());

                        if (response.getStatusLine().getStatusCode() >= 300 || response.getStatusLine().getStatusCode() < 200) {
                            throw new AuthenticatorUnavailableException(
                                    "Error response from token endpoint:\n" + response.getStatusLine() + "\n" + responseBody);
                        }

                        try {
                            Map<String, Object> responseJsonBody = DocReader.json().readObject(responseBody);

                            return new TokenResponse(responseJsonBody);
                        } catch (IOException e) {
                            throw new AuthenticatorUnavailableException(
                                    "Error while parsing result from " + tokenEndpoint + ":\n" + response + "\n" + responseBody, e);
                        }
                    }
                } catch (IOException e) {
                    throw new AuthenticatorUnavailableException("Error while calling " + tokenEndpoint, e);
                }
            });
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

    public HttpResponse callTokenEndpoint(byte[] body, ContentType contentType) throws AuthenticatorUnavailableException {
        OidcProviderConfig oidcProviderConfg = getOidcConfiguration();
        String tokenEndpoint = oidcProviderConfg.getTokenEndpoint();

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<HttpResponse>) () -> {
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

        if (tlsConfig != null) {
            builder.setSSLSocketFactory(tlsConfig.toSSLConnectionSocketFactory());
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

    public static class TokenResponse {

        private final String accessToken;
        private final String tokenType;
        private final String refreshToken;
        private final long expiresIn;
        private final String idToken;

        TokenResponse(String accessToken, String tokenType, String refreshToken, long expiresIn, String idToken) {
            this.accessToken = accessToken;
            this.tokenType = tokenType;
            this.refreshToken = refreshToken;
            this.expiresIn = expiresIn;
            this.idToken = idToken;
        }

        TokenResponse(Map<String, Object> document) {
            this.accessToken = document.get("access_token") != null ? String.valueOf(document.get("access_token")) : null;
            this.tokenType = document.get("token_type") != null ? String.valueOf(document.get("token_type")) : null;
            this.refreshToken = document.get("refresh_token") != null ? String.valueOf(document.get("refresh_token")) : null;
            this.idToken = document.get("id_token") != null ? String.valueOf(document.get("id_token")) : null;
            this.expiresIn = document.get("expires_in") instanceof Number ? ((Number) document.get("expires_in")).longValue() : null;
        }

        public String getAccessToken() {
            return accessToken;
        }

        public String getTokenType() {
            return tokenType;
        }

        public String getRefreshToken() {
            return refreshToken;
        }

        public long getExpiresIn() {
            return expiresIn;
        }

        public String getIdToken() {
            return idToken;
        }

        public Map<String, Object> asMap() {
            HashMap<String, Object> result = new HashMap<>();

            result.put("access_token", accessToken);
            result.put("token_type", tokenType);
            result.put("refresh_token", refreshToken);
            result.put("expires_in", expiresIn);
            result.put("id_token", idToken);

            return result;
        }

    }
}
