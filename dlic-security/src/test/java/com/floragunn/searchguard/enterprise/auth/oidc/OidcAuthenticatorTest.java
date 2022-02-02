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

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.browserup.bup.BrowserUpProxy;
import com.browserup.bup.BrowserUpProxyServer;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.user.AuthCredentials;
import com.google.common.collect.ImmutableMap;

public class OidcAuthenticatorTest {
    protected static MockIpdServer mockIdpServer;
    protected static MockIpdServer pkceMockIdpServer;

    protected static BrowserUpProxy httpProxy;

    private static ConfigurationRepository.Context testContext = new ConfigurationRepository.Context(null, null, null, null);
    private static Map<String, Object> basicAuthenticatorSettings;
    private static String FRONTEND_BASE_URL = "http://whereever";
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

    @BeforeClass
    public static void setUp() throws Exception {
        mockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL).start();
        httpProxy = new BrowserUpProxyServer();
        httpProxy.setMitmDisabled(true);
        httpProxy.start(0, InetAddress.getByName("127.0.0.8"), InetAddress.getByName("127.0.0.9"));
        basicAuthenticatorSettings = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "client_id",
                "Der Klient", "client_secret", "Das Geheimnis", "pkce", false);

        pkceMockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL).requirePkce(true).start();
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
    public void basicTest() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
    }

    @Test
    public void pkceTest() throws Exception {
        Map<String, Object> authenticatorSettings = ImmutableMap.of("idp.openid_configuration_url", pkceMockIdpServer.getDiscoverUri().toString(),
                "client_id", "Der Klient");

        OidcAuthenticator authenticator = new OidcAuthenticator(authenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = pkceMockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
    }

    @Test
    public void pkceMissingTest() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = pkceMockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Assert.assertNull(ssoResponse);
    }

    @Test
    public void nextUrlTest() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        String redirectTarget = "/goto/0f8bc3727ebe162dc2ceeae137e607a1?sg_tenant=management";
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, redirectTarget, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Assert.assertTrue(ssoResponse, ssoResponse.matches(".*state=[A-Za-z0-9\\-_]+%7C" + URLEncoder.encode(redirectTarget, "utf-8")));

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        Assert.assertEquals(redirectTarget, authCredentials.getRedirectUri());
    }

    @Test
    public void proxyTest() throws Exception {
        try (MockIpdServer proxyOnlyMockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL)
                .acceptConnectionsOnlyFromInetAddress(InetAddress.getByName("127.0.0.9")).start()) {

            Map<String, Object> config = DocNode.of("idp.openid_configuration_url", proxyOnlyMockIdpServer.getDiscoverUri().toString(),
                    "idp.proxy.host", "127.0.0.8", "idp.proxy.port", httpProxy.getPort(), "idp.proxy.scheme", "http", "client_id", "x",
                    "client_secret", "x");

            OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
            authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            Assert.assertNotNull(authMethod);
            Assert.assertNotNull(authMethod.toString(), authMethod.getSsoLocation());

            String ssoResponse = proxyOnlyMockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

            Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                    FRONTEND_BASE_URL);

            AuthCredentials authCredentials = authenticator.extractCredentials(request);

            Assert.assertNotNull(authCredentials);
            Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        }
    }

    @Test
    public void proxyWithTlsConfigTest() throws Exception {
        try (MockIpdServer proxyOnlyMockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL)
                .acceptConnectionsOnlyFromInetAddress(InetAddress.getByName("127.0.0.9")).useCustomTlsConfig(IDP_TLS_CONFIG).start()) {
            Map<String, Object> config = DocNode.of("idp.openid_configuration_url", proxyOnlyMockIdpServer.getDiscoverUri().toString(),
                    "idp.proxy.host", "127.0.0.8", "idp.proxy.port", httpProxy.getPort(), "idp.proxy.scheme", "http", "client_id", "x",
                    "client_secret", "x", "idp.tls.trusted_cas",
                    "#{file:" + FileHelper.getAbsoluteFilePathFromClassPath("oidc/idp/root-ca.pem") + "}", "idp.tls.verify_hostnames", false);

            OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
            authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            Assert.assertNotNull(authMethod);
            Assert.assertNotNull(authMethod.toString(), authMethod.getSsoLocation());

            String ssoResponse = proxyOnlyMockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

            Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                    FRONTEND_BASE_URL);

            AuthCredentials authCredentials = authenticator.extractCredentials(request);

            Assert.assertNotNull(authCredentials);
            Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        }
    }

    @Test
    public void testExp() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_EXPIRED_SIGNED_OCT_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        try {
            AuthCredentials authCredentials = authenticator.extractCredentials(request);
            Assert.fail("Expected exception, got: " + authCredentials);
        } catch (CredentialsException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("The token has expired"));
        }
    }

    @Test
    public void testRS256() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_RSA_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertNotNull(authCredentials);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
    }

    @Test
    public void testBadSignature() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_RSA_X);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        try {
            AuthCredentials authCredentials = authenticator.extractCredentials(request);
            Assert.fail("Expected exception, got: " + authCredentials);
        } catch (CredentialsException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Invalid JWT signature"));
        }
    }

    @Test
    public void testPeculiarJsonEscaping() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.PeculiarEscaping.MC_COY_SIGNED_RSA_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertNotNull(authCredentials);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
    }

}
