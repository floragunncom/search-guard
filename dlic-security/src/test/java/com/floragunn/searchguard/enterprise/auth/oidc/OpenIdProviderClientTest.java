/*
  * Copyright 2016-2020 by floragunn GmbH - All rights reserved
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

import com.browserup.bup.BrowserUpProxy;
import com.browserup.bup.BrowserUpProxyServer;
import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OpenIdProviderClientTest {

    private static final TLSConfig IDP_TLS_CONFIG;

    static {
        try {
            IDP_TLS_CONFIG = new TLSConfig.Builder().trust(FileHelper.getAbsoluteFilePathFromClassPath("oidc/idp/root-ca.pem").toFile())
                    .clientCert(FileHelper.getAbsoluteFilePathFromClassPath("oidc/idp/idp.pem").toFile(),
                            FileHelper.getAbsoluteFilePathFromClassPath("oidc/idp/idp.key").toFile(), "secret")
                    .build();
        } catch (FileNotFoundException | ConfigValidationException e) {
            throw new RuntimeException(e);
        }
    }

    protected static MockIpdServer mockIdpServer;
    protected static BrowserUpProxy httpProxy;

    @BeforeClass
    public static void setUp() throws Exception {
        mockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL).start();
        httpProxy = new BrowserUpProxyServer();
        httpProxy.start(0, InetAddress.getByName("127.0.0.8"), InetAddress.getByName("127.0.0.9"));
        mockIdpServer.setRequireValidCodes(false);
    }

    @AfterClass
    public static void tearDown() {
        if (mockIdpServer != null) {
            try {
                mockIdpServer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (httpProxy != null) {
            httpProxy.abort();
        }
    }

    @Test
    public void proxyTest() throws Exception {
        try (MockIpdServer proxyOnlyMockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL)
                .acceptConnectionsOnlyFromInetAddress(InetAddress.getByName("127.0.0.9")).start()) {
            OpenIdProviderClient openIdProviderClientWithoutProxySettings = new OpenIdProviderClient(proxyOnlyMockIdpServer.getDiscoverUri(), null,
                    null, true);

            proxyOnlyMockIdpServer.setRequireValidCodes(false);

            try {
                openIdProviderClientWithoutProxySettings.getOidcConfiguration();
                Assert.fail();
            } catch (AuthenticatorUnavailableException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("HTTP/1.1 451"));
            }
            Map<String, Object> proxySettings = ImmutableMap.of("proxy.host", "127.0.0.8", "proxy.port", httpProxy.getPort(), "proxy.scheme", "http");

            OpenIdProviderClient openIdProviderClient = new OpenIdProviderClient(proxyOnlyMockIdpServer.getDiscoverUri(), null,
                    ProxyConfig.parse(proxySettings, "proxy"), true);

            OidcProviderConfig oidcProviderConfig = openIdProviderClient.getOidcConfiguration().get();

            Assert.assertTrue(oidcProviderConfig.toJsonString(), oidcProviderConfig.toBasicObject().containsKey("token_endpoint"));

            String tokenEndpointRequest = "grant_type=authorization_code&code=wusch";

            HttpResponse response = openIdProviderClient.callTokenEndpoint(tokenEndpointRequest.getBytes(),
                    ContentType.create("application/x-www-form-urlencoded"));
            String entity = EntityUtils.toString(response.getEntity());

            Assert.assertEquals(entity, 200, response.getStatusLine().getStatusCode());

            Assert.assertTrue(entity, entity.contains("access_token"));
        }
    }

    @Test
    public void cacheTest() throws AuthenticatorUnavailableException {
        OpenIdProviderClient openIdProviderClient = new OpenIdProviderClient(mockIdpServer.getDiscoverUri(), null, null, true);
        KeySetRetriever keySetRetriever = new KeySetRetriever(openIdProviderClient);

        keySetRetriever.get();

        Assert.assertEquals(1, openIdProviderClient.getOidcCacheMisses());
        Assert.assertEquals(0, openIdProviderClient.getOidcCacheHits());

        keySetRetriever.get();
        Assert.assertEquals(1, openIdProviderClient.getOidcCacheMisses());
        Assert.assertEquals(1, openIdProviderClient.getOidcCacheHits());
    }

    @Test
    public void clientCertTest() throws Exception {

        try (MockIpdServer sslMockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL).useCustomTlsConfig(IDP_TLS_CONFIG)
                .requireTlsClientCertFingerprint("67f4d3453f1d52c7d3868e76f052cfd696a18bf4a70d8ececd6306e2428bec96").start()) {
            TLSConfig tlsConfig = new TLSConfig.Builder().trust(FileHelper.getAbsoluteFilePathFromClassPath("oidc/idp/root-ca.pem").toFile())
                    .clientCert(FileHelper.getAbsoluteFilePathFromClassPath("oidc/idp/client.pem").toFile(),
                            FileHelper.getAbsoluteFilePathFromClassPath("oidc/idp/client.key").toFile(), "secret")
                    .build();

            OpenIdProviderClient openIdProviderClient = new OpenIdProviderClient(sslMockIdpServer.getDiscoverUri(), tlsConfig, null, true);

            KeySetRetriever keySetRetriever = new KeySetRetriever(openIdProviderClient);

            keySetRetriever.get();

        }
    }
}
