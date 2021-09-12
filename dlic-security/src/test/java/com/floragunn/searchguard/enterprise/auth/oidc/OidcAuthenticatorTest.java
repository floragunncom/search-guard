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

import static com.floragunn.dlic.auth.http.jwt.keybyoidc.TestJwts.MCCOY_SUBJECT;
import static com.floragunn.dlic.auth.http.jwt.keybyoidc.TestJwts.ROLES_CLAIM;
import static com.floragunn.dlic.auth.http.jwt.keybyoidc.TestJwts.TEST_AUDIENCE;
import static com.floragunn.dlic.auth.http.jwt.keybyoidc.TestJwts.create;
import static com.floragunn.dlic.auth.http.jwt.keybyoidc.TestJwts.createSigned;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.browserup.bup.BrowserUpProxy;
import com.browserup.bup.BrowserUpProxyServer;
import com.floragunn.dlic.auth.http.jwt.keybyoidc.MockIpdServer;
import com.floragunn.dlic.auth.http.jwt.keybyoidc.TestJwk;
import com.floragunn.dlic.auth.http.jwt.keybyoidc.TestJwts;
import com.floragunn.searchguard.auth.AuthenticationFrontend;
import com.floragunn.searchguard.auth.CredentialsException;
import com.floragunn.searchguard.auth.frontend.ActivatedFrontendConfig;
import com.floragunn.searchguard.auth.frontend.GetFrontendConfigAction;
import com.floragunn.searchguard.user.AuthCredentials;
import com.google.common.collect.ImmutableMap;

public class OidcAuthenticatorTest {
    protected static MockIpdServer mockIdpServer;
    protected static BrowserUpProxy httpProxy;

    private static AuthenticationFrontend.Context testContext = new AuthenticationFrontend.Context(null, null, null);
    private static Map<String, Object> basicAuthenticatorSettings;
    private static String FRONTEND_BASE_URL = "http://whereever";

    @BeforeClass
    public static void setUp() throws Exception {
        mockIdpServer = MockIpdServer.start(TestJwk.Jwks.ALL);
        httpProxy = new BrowserUpProxyServer();
        httpProxy.start(0, InetAddress.getByName("127.0.0.8"), InetAddress.getByName("127.0.0.9"));
        basicAuthenticatorSettings = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "client_id",
                "Der Klient", "client_secret", "Das Geheimnis", "user_mapping.roles", "roles");
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
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, authCredentials.getAttributes().get("attr.jwt.aud"));
    }

    @Test
    public void nextUrlTest() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod,
                new GetFrontendConfigAction.Request(null, "http://redirect", FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Assert.assertTrue(ssoResponse, ssoResponse.contains("next_url=http%3A%2F%2Fredirect"));

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL, "next_url", "http://redirect");

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, authCredentials.getAttributes().get("attr.jwt.aud"));
    }

    @Ignore // TODO
    @Test
    public void proxyTest() throws Exception {
        try (MockIpdServer proxyOnlyMockIdpServer = MockIpdServer.start(TestJwk.Jwks.ALL)
                .acceptConnectionsOnlyFromInetAddress(InetAddress.getByName("127.0.0.9"))) {
            // TODO config dot notation is not available any more
            Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "proxy.host",
                    "127.0.0.8", "proxy.port", httpProxy.getPort(), "proxy.scheme", "http");

            OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
            authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

            Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                    FRONTEND_BASE_URL);

            AuthCredentials authCredentials = authenticator.extractCredentials(request);

            Assert.assertNotNull(authCredentials);
            Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
            Assert.assertEquals(TestJwts.TEST_AUDIENCE, authCredentials.getAttributes().get("attr.jwt.aud"));
        }
    }

    @Test
    public void testRoles() throws Exception {
        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "user_mapping.roles",
                TestJwts.ROLES_CLAIM, "client_id", "Der Klient", "client_secret", "Das Geheimnis");

        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertNotNull(authCredentials);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        Assert.assertEquals(TestJwts.TEST_ROLES, authCredentials.getBackendRoles());
    }

    @Test
    public void testRolesJsonPath() throws Exception {
        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "user_mapping.roles",
                "$." + TestJwts.ROLES_CLAIM, "subject_path", "$.sub", "client_id", "Der Klient", "client_secret", "Das Geheimnis");

        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertNotNull(authCredentials);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        Assert.assertEquals(TestJwts.TEST_ROLES, authCredentials.getBackendRoles());
    }

    @Test
    public void testRolesCollectionJsonPath() throws Exception {
        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "user_mapping.roles",
                "$." + TestJwts.ROLES_CLAIM, "user_mapping.subject", "$.sub", "client_id", "Der Klient", "client_secret", "Das Geheimnis");

        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(),
                createSigned(create(MCCOY_SUBJECT, TEST_AUDIENCE, ROLES_CLAIM, Arrays.asList("role 1", "role 2", "role 3, role 4")), TestJwk.OCT_1));

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertNotNull(authCredentials);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        Assert.assertThat(authCredentials.getBackendRoles(), CoreMatchers.hasItems("role 1", "role 2", "role 3", "role 4"));
    }

    @Test
    public void testInvalidSubjectJsonPath() throws Exception {
        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "user_mapping.roles",
                "$." + TestJwts.ROLES_CLAIM, "user_mapping.subject", "$.subasd", "client_id", "Der Klient", "client_secret", "Das Geheimnis");

        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        try {
            AuthCredentials authCredentials = authenticator.extractCredentials(request);
            Assert.fail("Expected exception, got: " + authCredentials);
        } catch (CredentialsException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("No subject found in JWT token"));
        }
    }

    @Test
    public void testInvalidRolesJsonPath() throws Exception {
        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "user_mapping.roles",
                "$.asd", "user_mapping.subject", "$.sub", "client_id", "Der Klient", "client_secret", "Das Geheimnis");

        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        try {
            AuthCredentials authCredentials = authenticator.extractCredentials(request);
            Assert.fail("Expected exception, got: " + authCredentials);
        } catch (CredentialsException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("The roles JSON path was not found in the Id token claims"));
        }
    }

    @Test
    public void testExp() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

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
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_RSA_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertNotNull(authCredentials);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, authCredentials.getAttributes().get("attr.jwt.aud"));
        Assert.assertEquals(2, authCredentials.getBackendRoles().size());
        Assert.assertEquals(3, authCredentials.getAttributes().size());
    }

    @Test
    public void testBadSignature() throws Exception {
        OidcAuthenticator authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

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
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.PeculiarEscaping.MC_COY_SIGNED_RSA_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);

        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertNotNull(authCredentials);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, authCredentials.getAttributes().get("attr.jwt.aud"));
        Assert.assertEquals(2, authCredentials.getBackendRoles().size());
        Assert.assertEquals(3, authCredentials.getAttributes().size());
    }

    @Test
    public void testSubjectPattern() throws Exception {
        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(),
                "user_mapping.subject_pattern", "^(.)(?:.*)$", "client_id", "Der Klient", "client_secret", "Das Geheimnis", "user_mapping.roles",
                "roles");

        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);

        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                FRONTEND_BASE_URL);
        AuthCredentials authCredentials = authenticator.extractCredentials(request);

        Assert.assertNotNull(authCredentials);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT.substring(0, 1), authCredentials.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, authCredentials.getAttributes().get("attr.jwt.aud"));
        Assert.assertEquals(2, authCredentials.getBackendRoles().size());
        Assert.assertEquals(3, authCredentials.getAttributes().size());
    }
}
