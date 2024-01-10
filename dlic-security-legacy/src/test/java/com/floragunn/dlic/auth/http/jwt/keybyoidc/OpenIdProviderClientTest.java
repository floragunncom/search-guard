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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.KeyStore;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Map;

import com.floragunn.searchsupport.proxy.wiremock.WireMockRequestHeaderAddingFilter;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.ssl.PrivateKeyDetails;
import org.apache.http.ssl.PrivateKeyStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class OpenIdProviderClientTest {
    protected static MockIpdServer mockIdpServer;

    private static final WireMockRequestHeaderAddingFilter REQUEST_HEADER_ADDING_FILTER = new WireMockRequestHeaderAddingFilter("Proxy", "wire-mock");

    @ClassRule
    public static WireMockRule wireMockProxy = new WireMockRule(WireMockConfiguration.options()
            .bindAddress("127.0.0.8")
            .trustAllProxyTargets(true)
            .enableBrowserProxying(true)
            .dynamicPort()
            .extensions(REQUEST_HEADER_ADDING_FILTER));

    @BeforeClass
    public static void setUp() throws Exception {
        mockIdpServer = MockIpdServer.start(TestJwk.Jwks.ALL);
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
    }

    @Ignore("TODO why is this ignored?")
    @Test
    public void proxyTest() throws Exception {
        try (MockIpdServer proxyOnlyMockIdpServer = MockIpdServer.start(TestJwk.Jwks.ALL)
                .acceptOnlyRequestsWithHeader(REQUEST_HEADER_ADDING_FILTER.getHeader())) {

            proxyOnlyMockIdpServer.setRequireValidCodes(false);

            OpenIdProviderClient openIdProviderClientWithoutProxySettings = new OpenIdProviderClient(proxyOnlyMockIdpServer.getDiscoverUri(), null,
                    null, true);

            try {
                openIdProviderClientWithoutProxySettings.getOidcConfiguration();
                Assert.fail();
            } catch (AuthenticatorUnavailableException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("HTTP/1.1 451"));
            }
            Map<String, Object> proxySettings = ImmutableMap.of("proxy.host", "127.0.0.8", "proxy.port", wireMockProxy.port(), "proxy.scheme", "http");

            OpenIdProviderClient openIdProviderClient = new OpenIdProviderClient(proxyOnlyMockIdpServer.getDiscoverUri(), null,
                    ProxyConfig.parse(proxySettings, "proxy"), true);

            OidcProviderConfig oidcProviderConfig = openIdProviderClient.getOidcConfiguration();

            Assert.assertTrue(oidcProviderConfig.getParsedJson() + "", oidcProviderConfig.getParsedJson().containsKey("token_endpoint"));

            String tokenEndpointRequest = "grant_type=authorization_code&code=wusch";

            HttpResponse response = openIdProviderClient.callTokenEndpoint(tokenEndpointRequest.getBytes(),
                    ContentType.create("application/x-www-form-urlencoded"));

            Assert.assertEquals(response.toString(), 200, response.getStatusLine().getStatusCode());

            String entity = EntityUtils.toString(response.getEntity());

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

        try (MockIpdServer sslMockIdpServer = new MockIpdServer(TestJwk.Jwks.ALL, SocketUtils.findAvailableTcpPort(), true) {
            @Override
            protected void handleDiscoverRequest(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {

                MockIpdServer.SSLTestHttpServerConnection connection = (MockIpdServer.SSLTestHttpServerConnection) ((HttpCoreContext) context)
                        .getConnection();

                X509Certificate peerCert = (X509Certificate) connection.getPeerCertificates()[0];

                try {
                    String sha256Fingerprint = Hashing.sha256().hashBytes(peerCert.getEncoded()).toString();

                    Assert.assertEquals("04b2b8baea7a0a893f0223d95b72081e9a1e154a0f9b1b4e75998085972b1b68", sha256Fingerprint);

                } catch (CertificateEncodingException e) {
                    throw new RuntimeException(e);
                }

                super.handleDiscoverRequest(request, response, context);
            }
        }) {
            SSLContextBuilder sslContextBuilder = SSLContexts.custom();

            KeyStore trustStore = KeyStore.getInstance("JKS");
            InputStream trustStream = new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath("jwt/truststore.jks").toFile());
            trustStore.load(trustStream, "changeit".toCharArray());

            KeyStore keyStore = KeyStore.getInstance("JKS");
            InputStream keyStream = new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath("jwt/spock-keystore.jks").toFile());

            keyStore.load(keyStream, "changeit".toCharArray());

            sslContextBuilder.loadTrustMaterial(trustStore, null);

            sslContextBuilder.loadKeyMaterial(keyStore, "changeit".toCharArray(), new PrivateKeyStrategy() {

                @Override
                public String chooseAlias(Map<String, PrivateKeyDetails> aliases, Socket socket) {
                    return "spock";
                }
            });

            SettingsBasedSSLConfigurator.SSLConfig sslConfig = new SettingsBasedSSLConfigurator.SSLConfig(sslContextBuilder.build(),
                    new String[] { "TLSv1.2", "TLSv1.1" }, null, null, false, false, false, trustStore, null, keyStore, null, null, false);
            OpenIdProviderClient openIdProviderClient = new OpenIdProviderClient(mockIdpServer.getDiscoverUri(), sslConfig, null, true);

            KeySetRetriever keySetRetriever = new KeySetRetriever(openIdProviderClient);

            keySetRetriever.get();

        }
    }
}
