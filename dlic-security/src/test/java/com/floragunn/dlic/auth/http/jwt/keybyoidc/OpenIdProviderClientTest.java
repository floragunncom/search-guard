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
import java.net.InetAddress;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Map;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.browserup.bup.BrowserUpProxy;
import com.browserup.bup.BrowserUpProxyServer;
import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.dlic.auth.http.jwt.oidc.json.OidcProviderConfig;
import com.floragunn.searchguard.auth.AuthenticatorUnavailableException;
import com.floragunn.searchguard.enterprise.auth.oidc.KeySetRetriever;
import com.floragunn.searchguard.enterprise.auth.oidc.OpenIdProviderClient;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.floragunn.searchsupport.xcontent.ObjectTreeXContent;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;

public class OpenIdProviderClientTest {
    protected static MockIpdServer mockIdpServer;
    protected static BrowserUpProxy httpProxy;

    @BeforeClass
    public static void setUp() throws Exception {
        mockIdpServer = MockIpdServer.start(TestJwk.Jwks.ALL);
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
        try (MockIpdServer proxyOnlyMockIdpServer = MockIpdServer.start(TestJwk.Jwks.ALL)
                .acceptConnectionsOnlyFromInetAddress(InetAddress.getByName("127.0.0.9"))) {
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

            OidcProviderConfig oidcProviderConfig = openIdProviderClient.getOidcConfiguration();

            Assert.assertTrue(oidcProviderConfig.getParsedJson() + "", oidcProviderConfig.getParsedJson().containsKey("token_endpoint"));

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
            TLSConfig tlsConfig = new TLSConfig.Builder()
                    .trustJks(FileHelper.getAbsoluteFilePathFromClassPath("jwt/truststore.jks").toFile(), "changeit")
                    .clientCertJks(FileHelper.getAbsoluteFilePathFromClassPath("jwt/spock-keystore.jks").toFile(), "changeit", "spock").build();

            OpenIdProviderClient openIdProviderClient = new OpenIdProviderClient(mockIdpServer.getDiscoverUri(), tlsConfig, null, true);

            KeySetRetriever keySetRetriever = new KeySetRetriever(openIdProviderClient);

            keySetRetriever.get();

        }
    }
}
