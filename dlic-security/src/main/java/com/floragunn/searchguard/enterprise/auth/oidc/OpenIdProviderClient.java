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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLHandshakeException;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;
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

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchsupport.PrivilegedCode;

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
    private final JwksProviderClient jwkProviderClient;

    public OpenIdProviderClient(URI openIdConnectEndpoint, TLSConfig tlsConfig, ProxyConfig proxyConfig, boolean useCacheForOidConnectEndpoint) {
        this.openIdConnectEndpoint = openIdConnectEndpoint;
        this.tlsConfig = tlsConfig;
        this.proxyConfig = proxyConfig;

        if (useCacheForOidConnectEndpoint) {
            cacheConfig = CacheConfig.custom().setMaxCacheEntries(10).setMaxObjectSize(1024L * 1024L).build();
            oidcHttpCacheStorage = new BasicHttpCacheStorage(cacheConfig);
        }

        this.jwkProviderClient = new JwksProviderClient(tlsConfig, proxyConfig);
    }

    public ValidationResult<OidcProviderConfig> getOidcConfiguration() throws AuthenticatorUnavailableException {
        return PrivilegedCode.execute(() -> {

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
                        throw new AuthenticatorUnavailableException("Error while retrieving OIDC config",
                                statusLine + (response.getEntity() != null ? "\n" + EntityUtils.toString(response.getEntity()) : ""))
                                        .details("openid_configuration_url", openIdConnectEndpoint);
                    }

                    HttpEntity httpEntity = response.getEntity();

                    if (httpEntity == null) {
                        throw new AuthenticatorUnavailableException("Error while retrieving OIDC config", "Empty response")
                                .details("openid_configuration_url", openIdConnectEndpoint);
                    }

                    return OidcProviderConfig.parse(DocNode.parse(Format.JSON).from(httpEntity.getContent()));
                }
            } catch (SSLHandshakeException e) {
                throw new AuthenticatorUnavailableException("Error while retrieving OIDC config", e).details("openid_configuration_url",
                        openIdConnectEndpoint);
            } catch (IOException e) {
                throw new AuthenticatorUnavailableException("Error while retrieving OIDC config", e).details("openid_configuration_url",
                        openIdConnectEndpoint);
            } catch (DocumentParseException e) {
                throw new AuthenticatorUnavailableException("Error while retrieving OIDC config", e).details("openid_configuration_url",
                        openIdConnectEndpoint);
            }
        }, AuthenticatorUnavailableException.class);

    }

    public JsonWebKeys getJsonWebKeys() throws AuthenticatorUnavailableException {
        return jwkProviderClient.getJsonWebKeys(getJwksUri());
    }

    public TokenResponse callTokenEndpoint(Map<String, String> params) throws AuthenticatorUnavailableException {
        List<NameValuePair> request = new ArrayList<>(params.size() + 1);

        request.add(new BasicNameValuePair("grant_type", "authorization_code"));

        for (Map.Entry<String, String> entry : params.entrySet()) {
            request.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }

        URI tokenEndpoint;
        try {
            tokenEndpoint = getOidcConfiguration().get().getTokenEndpoint();
        } catch (ConfigValidationException e) {
            throw new AuthenticatorUnavailableException("Invalid token endpoint in OIDC metadata", e).details("openid_configuration_url",
                    openIdConnectEndpoint);
        }

        return PrivilegedCode.execute(() -> {
            try (CloseableHttpClient httpClient = createHttpClient(null)) {

                HttpPost httpPost = new HttpPost(tokenEndpoint);
                httpPost.setEntity(new UrlEncodedFormEntity(request, "utf-8"));

                RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(getRequestTimeoutMs())
                        .setConnectTimeout(getRequestTimeoutMs()).setSocketTimeout(getRequestTimeoutMs()).build();

                httpPost.setConfig(requestConfig);

                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    String responseBody = EntityUtils.toString(response.getEntity());
                    StatusLine statusLine = response.getStatusLine();

                    if (response.getStatusLine().getStatusCode() >= 300 || response.getStatusLine().getStatusCode() < 200) {
                        throw new AuthenticatorUnavailableException("Error exchanging OIDC auth_code",
                                statusLine + (responseBody != null ? "\n" + responseBody : "")).details("openid_configuration_url",
                                        openIdConnectEndpoint, "token_endpoint", tokenEndpoint);
                    }

                    Map<String, Object> responseJsonBody = DocReader.json().readObject(responseBody);

                    return new TokenResponse(responseJsonBody);
                }
            } catch (IOException e) {
                throw new AuthenticatorUnavailableException("Error exchanging OIDC auth_code", e).details("openid_configuration_url",
                        openIdConnectEndpoint, "token_endpoint", tokenEndpoint);
            } catch (DocumentParseException | UnexpectedDocumentStructureException e) {
                throw new AuthenticatorUnavailableException("Error exchanging OIDC auth_code", e).details("openid_configuration_url",
                        openIdConnectEndpoint, "token_endpoint", tokenEndpoint);
            }
        }, AuthenticatorUnavailableException.class);
    }

    public HttpResponse callTokenEndpoint(byte[] body, ContentType contentType) throws AuthenticatorUnavailableException {
        URI tokenEndpoint;
        try {
            tokenEndpoint = getOidcConfiguration().get().getTokenEndpoint();
        } catch (ConfigValidationException e) {
            throw new AuthenticatorUnavailableException("Invalid token endpoint in OIDC metadata", e).details("openid_configuration_url",
                    openIdConnectEndpoint);
        }

        return PrivilegedCode.execute(() -> {

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
                throw new AuthenticatorUnavailableException("Error exchanging OIDC auth_code", e).details("openid_configuration_url",
                        openIdConnectEndpoint, "token_endpoint", tokenEndpoint);
            }
        }, AuthenticatorUnavailableException.class);
    }

    URI getJwksUri() throws AuthenticatorUnavailableException {
        try {
            return getOidcConfiguration().get().getJwksUri();
        } catch (ConfigValidationException e) {
            throw new AuthenticatorUnavailableException("Invalid jwk_uri in OIDC metadata", e).details("openid_configuration_url",
                    openIdConnectEndpoint);
        }
    }

    public Map<String, Object> callUserInfoEndpoint(String accessToken, JwtVerifier userInfoJwtVerifier) throws AuthenticatorUnavailableException {
        URI userInfoEndpoint;
        try {
            userInfoEndpoint = getOidcConfiguration().get().getUserinfoEndpoint();
        } catch (ConfigValidationException e) {
            throw new AuthenticatorUnavailableException("Invalid userinfo endpoint in OIDC metadata", e).details("openid_configuration_url",
                    openIdConnectEndpoint);
        }

        return PrivilegedCode.execute(() -> {

            try (CloseableHttpClient httpClient = createHttpClient(null)) {

                HttpGet httpGet = new HttpGet(userInfoEndpoint);
                httpGet.setHeader("Authorization", "Bearer " + accessToken);

                RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(getRequestTimeoutMs())
                        .setConnectTimeout(getRequestTimeoutMs()).setSocketTimeout(getRequestTimeoutMs()).build();

                httpGet.setConfig(requestConfig);

                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    StatusLine statusLine = response.getStatusLine();

                    if (statusLine.getStatusCode() == 200) {
                        HttpEntity entity = response.getEntity();
                        String entityString = EntityUtils.toString(entity);

                        if (entity.getContentType() == null) {
                            throw new AuthenticatorUnavailableException("Error calling OIDC userinfo endpoint",
                                    "Response did not specify Content-Type").details("openid_configuration_url", openIdConnectEndpoint,
                                            "userinfo_endpoint", userInfoEndpoint, "response", EntityUtils.toString(entity));
                        }

                        if (entity.getContentType().getValue().startsWith("application/json")) {
                            try {
                                return DocReader.json().readObject(entityString);
                            } catch (DocumentParseException | UnexpectedDocumentStructureException | UnsupportedOperationException e) {
                                throw new AuthenticatorUnavailableException("Error parsing OIDC userinfo response",
                                        "Unexpected response Content-Type " + entity.getContentType().getValue()).details("openid_configuration_url",
                                                openIdConnectEndpoint, "userinfo_endpoint", userInfoEndpoint, "response", entityString);
                            }
                        } else if (entity.getContentType().getValue().startsWith("application/jwt")) {
                            try {
                                return userInfoJwtVerifier.getVerifiedJwtToken(entityString).getClaims().asMap();
                            } catch (BadCredentialsException e) {
                                throw new AuthenticatorUnavailableException("Error calling OIDC userinfo endpoint", e).details(
                                        "openid_configuration_url", openIdConnectEndpoint, "userinfo_endpoint", userInfoEndpoint, "response",
                                        entityString);
                            }
                        } else {
                            throw new AuthenticatorUnavailableException("Error calling OIDC userinfo endpoint",
                                    "Unexpected response Content-Type " + entity.getContentType().getValue()).details("openid_configuration_url",
                                            openIdConnectEndpoint, "userinfo_endpoint", userInfoEndpoint, "response", entityString);
                        }
                    } else {
                        String wwwAuthenticateError = response.getFirstHeader("WWW-Authenticate") != null
                                ? response.getFirstHeader("WWW-Authenticate").getValue()
                                : null;

                        throw new AuthenticatorUnavailableException("Error calling OIDC userinfo endpoint", statusLine.toString()).details(
                                "openid_configuration_url", openIdConnectEndpoint, "userinfo_endpoint", userInfoEndpoint, "response",
                                EntityUtils.toString(response.getEntity()), "www_authenticate_header", wwwAuthenticateError);
                    }
                }
            } catch (IOException e) {
                throw new AuthenticatorUnavailableException("Error calling OIDC userinfo endpoint", e).details("openid_configuration_url",
                        openIdConnectEndpoint, "userinfo_endpoint", userInfoEndpoint);
            }
        }, AuthenticatorUnavailableException.class);
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int httpTimeoutMs) {
        this.requestTimeoutMs = httpTimeoutMs;
        this.jwkProviderClient.setRequestTimeoutMs(httpTimeoutMs);
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
        private final Long expiresIn;
        private final String idToken;

        TokenResponse(String accessToken, String tokenType, String refreshToken, Long expiresIn, String idToken) {
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

        public Long getExpiresIn() {
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
