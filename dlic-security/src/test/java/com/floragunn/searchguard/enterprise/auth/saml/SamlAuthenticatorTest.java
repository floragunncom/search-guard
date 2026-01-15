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

package com.floragunn.searchguard.enterprise.auth.saml;

import java.security.Security;
import java.util.Arrays;
import java.util.Map;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensaml.core.config.InitializationService;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;


public class SamlAuthenticatorTest {

    static {

        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }

        ensureOpenSamlInitialization();
    }

    protected static MockSamlIdpServer mockSamlIdpServer;

    private static ConfigurationRepository.Context testContext = new ConfigurationRepository.Context(null, null, null, null, null).withExternalResources();

    private static Map<String, Object> basicIdpConfig;
    private static Map<String, Object> basicAuthenticatorSettings;
    private static String FRONTEND_BASE_URL = "http://whereever";

    @BeforeClass
    public static void setUp() throws Exception {
        mockSamlIdpServer = new MockSamlIdpServer();
        mockSamlIdpServer.start();
        basicIdpConfig = ImmutableMap.of("metadata_url", mockSamlIdpServer.getMetadataUri(), "entity_id", mockSamlIdpServer.getIdpEntityId());
        basicAuthenticatorSettings = ImmutableMap.of("idp", basicIdpConfig);
    }

    @AfterClass
    public static void tearDown() {
        if (mockSamlIdpServer != null) {
            try {
                mockSamlIdpServer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void basicTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);

        try (SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext)) {
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);

            authMethod = samlAuthenticator.activateFrontendConfig(authMethod,
                    new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

            Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                    "frontend_base_url", FRONTEND_BASE_URL);

            AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

            Assert.assertEquals("horst", authCredentials.getUsername());
        }
    }

    @Test
    public void inlineXmlTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);

        Map<String, Object> inlineMetadataIdpConfig = ImmutableMap.of("metadata_xml", " " + mockSamlIdpServer.createMetadata(), "entity_id",
                mockSamlIdpServer.getIdpEntityId(), "frontend_base_url", FRONTEND_BASE_URL);
        Map<String, Object> inlineMetadataAuthenticatorSettings = ImmutableMap.of("idp", inlineMetadataIdpConfig);

        try (SamlAuthenticator samlAuthenticator = new SamlAuthenticator(inlineMetadataAuthenticatorSettings, testContext)) {
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);

            authMethod = samlAuthenticator.activateFrontendConfig(authMethod,
                    new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

            Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                    "frontend_base_url", FRONTEND_BASE_URL);

            AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

            Assert.assertEquals("horst", authCredentials.getUsername());
        }
    }

    @Test
    public void inlineXmlParsingTest() throws Exception {
        String yml = "      idp:\n" + "        metadata_xml: | \n"
                + "            <EntityDescriptor entityID=\"urn:searchguard.eu.auth0.com\" xmlns=\"urn:oasis:names:tc:SAML:2.0:metadata\">\n"
                + "              <IDPSSODescriptor protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">\n"
                + "                <KeyDescriptor use=\"signing\">\n" + "                  <KeyInfo xmlns=\"http://www.w3.org/2000/09/xmldsig#\">\n"
                + "                    <X509Data>\n"
                + "                      <X509Certificate>MIIDCzCCAfOgAwIBAgIJdqTEVOBFJFb+MA0GCSqGSIb3DQEBCwUAMCMxITAfBgNVBAMTGHNlYXJjaGd1YXJkLmV1LmF1dGgwLmNvbTAeFw0xODA2MDIwOTUyMTZaFw0zMjAyMDkwOTUyMTZaMCMxITAfBgNVBAMTGHNlYXJjaGd1YXJkLmV1LmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALzQtPED4GXSPca5MuPKf6b9Jza2yLOasMJ9jRIqg7MdKea05yx4jnDn9bXU3NocTisLR8jV2QCijOiUEv+CExBzhZhj8xGcr7IzhPIejpOeDaLTHCCK9VLVjH2RtDHJ6YT+jxlALTqaJnHu2yNwAVs0mlfSGOTi2rcCZTXCk/04FmYyo6RPtGwpuyLlqexwDI6dXO2T+/MJqox/hZ0m5KycKeQpdOcNPb4I3M7suUdFs5W0mYg67Ayp/XbwVjmlD4r+Z/TNknaDlHLEMwdYYTH6PpaUSdls2Gxl2JLu0o8SuHfvI/KyxQGc8EBBIFQRZ/6X/dphpnkpYmq0OD5Xj0sCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU413vkg/THPSv9VulJMJzMa5IOS4wDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQB9HiG2/Zcm+LhuUvmobPxSLzWbsOQdqAnmV8T1H560cFLtDUh5bcGhcSnZBmxW8Vdy7vNSm+TOhVsmYqqsWBc53yVFSi+1mgh8GlK+V1cN/l3/teZp70sOLncpxGQWMWxpiOkTYkmaaoJbg59oJECSYGvSESuWhugsLd6lBF1Rn9k0tJqYxuy7RJuDpjDLGTP+F9sNcY4Inn+nB5NiaFs1F5HCZgnJGzc706a9FfXKkvVrKd2FuyuXA5m4ScyiO77+Wbx1IcnKGTj9a+ZhNhNkHj84DHYiiKn9ZJgmPHW4J1t+IcbUjPLQD/ro4RabMqx9rkHBAs7EeFL1IRcHdPXV</X509Certificate>\n"
                + "                    </X509Data>\n" + "                  </KeyInfo>\n" + "                </KeyDescriptor>\n"
                + "                <SingleLogoutService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\" Location=\"https://searchguard.eu.auth0.com/samlp/rDlT7CzxPHjozMsOMXanoHtZwZR7Rih1/logout\"/>\n"
                + "                <SingleLogoutService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" Location=\"https://searchguard.eu.auth0.com/samlp/rDlT7CzxPHjozMsOMXanoHtZwZR7Rih1/logout\"/>\n"
                + "                <NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</NameIDFormat>\n"
                + "                <NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:persistent</NameIDFormat>\n"
                + "                <NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:transient</NameIDFormat>\n"
                + "                <SingleSignOnService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\" Location=\"https://searchguard.eu.auth0.com/samlp/rDlT7CzxPHjozMsOMXanoHtZwZR7Rih1\"/>\n"
                + "                <SingleSignOnService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" Location=\"https://searchguard.eu.auth0.com/samlp/rDlT7CzxPHjozMsOMXanoHtZwZR7Rih1\"/>\n"
                + "                <Attribute Name=\"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress\" NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\" FriendlyName=\"E-Mail Address\" xmlns=\"urn:oasis:names:tc:SAML:2.0:assertion\"/>\n"
                + "                <Attribute Name=\"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname\" NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\" FriendlyName=\"Given Name\" xmlns=\"urn:oasis:names:tc:SAML:2.0:assertion\"/>\n"
                + "                <Attribute Name=\"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name\" NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\" FriendlyName=\"Name\" xmlns=\"urn:oasis:names:tc:SAML:2.0:assertion\"/>\n"
                + "                <Attribute Name=\"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname\" NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\" FriendlyName=\"Surname\" xmlns=\"urn:oasis:names:tc:SAML:2.0:assertion\"/>\n"
                + "                <Attribute Name=\"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier\" NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\" FriendlyName=\"Name ID\" xmlns=\"urn:oasis:names:tc:SAML:2.0:assertion\"/>\n"
                + "              </IDPSSODescriptor>\n" + "            </EntityDescriptor>\n" + "        entity_id: urn:searchguard.eu.auth0.com\n"
                + "      sp:\n" + "        entity_id: es-saml\n";

        new SamlAuthenticator(DocNode.parse(Format.YAML).from(yml), testContext).close();
    }

    @Test
    public void unsolicitedSsoTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);
        mockSamlIdpServer.setDefaultAssertionConsumerService("http://whereever/searchguard/saml/acs/idpinitiated");

        try (SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext)) {
            String encodedSamlResponse = mockSamlIdpServer.createUnsolicitedSamlResponse();

            Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "frontend_base_url", FRONTEND_BASE_URL);

            AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);
            Assert.assertEquals("horst", authCredentials.getUsername());
        }
    }

    @Test
    public void badUnsolicitedSsoTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);
        mockSamlIdpServer.setDefaultAssertionConsumerService("http://whereever/searchguard/saml/acs/idpinitiated");

        try (SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext)) {
            String encodedSamlResponse = mockSamlIdpServer.createUnsolicitedSamlResponse();

            Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", "saml_request_id:wrong_request_id",
                    "frontend_base_url", FRONTEND_BASE_URL);

            try {
                AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);
                Assert.fail("Expected exception, got: " + authCredentials);
            } catch (CredentialsException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("does not match the ID of the AuthNRequest sent by the SP"));
            }
        }
    }

    @Test
    public void wrongCertTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);

        try (SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext)) {
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
            authMethod = samlAuthenticator.activateFrontendConfig(authMethod,
                    new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            mockSamlIdpServer.loadSigningKeys("saml/spock-keystore.jks", "spock");

            String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

            Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                    "frontend_base_url", FRONTEND_BASE_URL);

            try {
                AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);
                Assert.fail("Expected exception, got: " + authCredentials);
            } catch (CredentialsException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("Signature validation failed"));
            }
        }
    }

    @Test
    public void noSignatureTest() throws Exception {
        mockSamlIdpServer.setSignResponses(false);
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);

        try (SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext)) {
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
            authMethod = samlAuthenticator.activateFrontendConfig(authMethod,
                    new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

            Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                    "frontend_base_url", FRONTEND_BASE_URL);

            try {
                AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);
                Assert.fail("Expected exception, got " + authCredentials);
            } catch (CredentialsException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("No Signature found"));
            }
        }
    }

    @Test
    public void rolesTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setAuthenticateUserRoles(Arrays.asList("a", "b"));
        mockSamlIdpServer.setEndpointQueryString(null);

        try (SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext)) {
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
            authMethod = samlAuthenticator.activateFrontendConfig(authMethod,
                    new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

            Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                    "frontend_base_url", FRONTEND_BASE_URL);
            AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

            Assert.assertEquals("horst", authCredentials.getUsername());
            Assert.assertEquals(ImmutableMap.of("roles", ImmutableList.of("a", "b")),
                    authCredentials.getAttributesForUserMapping().get("saml_response"));
        }
    }

    @Test
    public void idpEndpointWithQueryStringTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString("extra=query");

        try (SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext)) {
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
            authMethod = samlAuthenticator.activateFrontendConfig(authMethod,
                    new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

            Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                    "frontend_base_url", FRONTEND_BASE_URL);
            AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

            Assert.assertEquals("horst", authCredentials.getUsername());
        }
    }

    @Test
    public void initialConnectionFailureTest() throws Exception {
        try (MockSamlIdpServer mockSamlIdpServer = new MockSamlIdpServer()) {
            Map<String, Object> idpConfig = ImmutableMap.of("metadata_url", mockSamlIdpServer.getMetadataUri(), "entity_id",
                    mockSamlIdpServer.getIdpEntityId());
            Map<String, Object> config = ImmutableMap.of("idp", idpConfig, "idp.min_refresh_delay", 100);

            try (SamlAuthenticator samlAuthenticator = new SamlAuthenticator(config, testContext)) {
                ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);

                try {
                    authMethod = samlAuthenticator.activateFrontendConfig(authMethod,
                            new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));
                    Assert.fail(authMethod.toString());
                } catch (AuthenticatorUnavailableException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("SAML metadata is not yet available"));
                }

                String encodedSamlResponse = "whatever";
                Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "frontend_base_url", FRONTEND_BASE_URL);

                try {
                    samlAuthenticator.extractCredentials(request);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(e.toString(), e.getMessage().contains("SAML metadata is not yet available"));
                }

                mockSamlIdpServer.start();

                mockSamlIdpServer.setSignResponses(true);
                mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
                mockSamlIdpServer.setAuthenticateUser("horst");
                mockSamlIdpServer.setEndpointQueryString(null);

                Thread.sleep(500);

                authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
                authMethod = samlAuthenticator.activateFrontendConfig(authMethod,
                        new GetActivatedFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

                encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());
                request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                        FRONTEND_BASE_URL);
                AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

                Assert.assertEquals("horst", authCredentials.getUsername());
            }
        }
    }

    static void ensureOpenSamlInitialization() {

        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();

        try {

            thread.setContextClassLoader(InitializationService.class.getClassLoader());

            InitializationService.initialize();

            new org.opensaml.saml.config.impl.XMLObjectProviderInitializer().init();
            new org.opensaml.saml.config.impl.SAMLConfigurationInitializer().init();
            new org.opensaml.xmlsec.config.impl.XMLObjectProviderInitializer().init();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            thread.setContextClassLoader(originalClassLoader);
        }
    }

}
