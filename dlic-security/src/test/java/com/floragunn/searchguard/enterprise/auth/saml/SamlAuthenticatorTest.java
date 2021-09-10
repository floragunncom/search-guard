/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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
import org.opensearch.common.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensaml.core.config.InitializationService;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.searchguard.auth.AuthenticationFrontend;
import com.floragunn.searchguard.auth.CredentialsException;
import com.floragunn.searchguard.auth.frontend.ActivatedFrontendConfig;
import com.floragunn.searchguard.auth.frontend.GetFrontendConfigAction;
import com.floragunn.searchguard.modules.StandardComponents;
import com.floragunn.searchguard.sgconf.impl.v7.FrontendConfig;
import com.floragunn.searchguard.user.AuthCredentials;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SamlAuthenticatorTest {

    static {

        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }

        ensureOpenSamlInitialization();
    }

    protected static MockSamlIdpServer mockSamlIdpServer;

    private static AuthenticationFrontend.Context testContext = new AuthenticationFrontend.Context(null, null, null);

    private static Map<String, Object> basicIdpConfig;
    private static Map<String, Object> basicAuthenticatorSettings;
    private static String FRONTEND_BASE_URL = "http://whereever";

    @BeforeClass
    public static void setUp() throws Exception {
        mockSamlIdpServer = new MockSamlIdpServer();
        mockSamlIdpServer.start();
        basicIdpConfig = ImmutableMap.of("metadata_url", mockSamlIdpServer.getMetadataUri(), "entity_id", mockSamlIdpServer.getIdpEntityId());
        basicAuthenticatorSettings = ImmutableMap.of("idp", basicIdpConfig, "roles_key", "roles");
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

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);

        authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

        Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                "frontend_base_url", FRONTEND_BASE_URL);

        AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

        Assert.assertEquals("horst", authCredentials.getUsername());
    }

    @Test
    public void inlineXmlTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);

        Map<String, Object> inlineMetadataIdpConfig = ImmutableMap.of("metadata_xml", " " + mockSamlIdpServer.createMetadata(), "entity_id",
                mockSamlIdpServer.getIdpEntityId(), "frontend_base_url", FRONTEND_BASE_URL);
        Map<String, Object> inlineMetadataAuthenticatorSettings = ImmutableMap.of("idp", inlineMetadataIdpConfig, "roles_key", "roles");

        System.out.println(inlineMetadataIdpConfig);

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(inlineMetadataAuthenticatorSettings, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);

        authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

        Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                "frontend_base_url", FRONTEND_BASE_URL);

        AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

        Assert.assertEquals("horst", authCredentials.getUsername());
    }

    @Test
    public void inlineXmlParsingTest() throws Exception {
        String yml = "  base_url: \"https://kibana.example.com:5601/\"\n" + "  authcz:\n" + "    - type: saml\n" + "      label: \"SAML Login\"\n"
                + "      idp:\n" + "        metadata_xml: | \n"
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
                + "      sp:\n" + "        entity_id: es-saml\n" + "      roles_key: http://schemas.auth0.com/https://kibana;example;com/roles";

        FrontendConfig frontendConfig = FrontendConfig.parse(DocReader.yaml().readObject(yml), StandardComponents.apiAuthenticationFrontends, null);

        System.out.println(frontendConfig.getAuthcz());
    }

    @Test
    public void unsolicitedSsoTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);
        mockSamlIdpServer.setDefaultAssertionConsumerService("http://whereever/searchguard/saml/acs/idpinitiated");

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext);

        String encodedSamlResponse = mockSamlIdpServer.createUnsolicitedSamlResponse();

        Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "frontend_base_url", FRONTEND_BASE_URL);

        AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);
        Assert.assertEquals("horst", authCredentials.getUsername());
    }

    @Test
    public void badUnsolicitedSsoTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);
        mockSamlIdpServer.setDefaultAssertionConsumerService("http://whereever/searchguard/saml/acs/idpinitiated");

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext);

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

    @Test
    public void wrongCertTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext);

        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
        authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

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

    @Test
    public void noSignatureTest() throws Exception {
        mockSamlIdpServer.setSignResponses(false);
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString(null);

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext);

        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
        authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

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

    @Test
    public void rolesTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setAuthenticateUserRoles(Arrays.asList("a", "b"));
        mockSamlIdpServer.setEndpointQueryString(null);

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext);

        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
        authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

        Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                "frontend_base_url", FRONTEND_BASE_URL);
        AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

        Assert.assertEquals("horst", authCredentials.getUsername());
        Assert.assertEquals(ImmutableSet.of("a", "b"), authCredentials.getBackendRoles());
    }

    @Test
    public void idpEndpointWithQueryStringTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setEndpointQueryString("extra=query");

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(basicAuthenticatorSettings, testContext);

        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
        authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

        Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                "frontend_base_url", FRONTEND_BASE_URL);
        AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

        Assert.assertEquals("horst", authCredentials.getUsername());
    }

    @Test
    public void commaSeparatedRolesTest() throws Exception {
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUserRoles(Arrays.asList("a,b"));
        mockSamlIdpServer.setEndpointQueryString(null);

        Map<String, Object> config = ImmutableMap.of("idp", basicIdpConfig, "roles_key", "roles", "roles_seperator", ",");

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(config, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
        authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

        Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                "frontend_base_url", FRONTEND_BASE_URL);
        AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

        Assert.assertEquals("horst", authCredentials.getUsername());
        Assert.assertEquals(ImmutableSet.of("a", "b"), authCredentials.getBackendRoles());
    }

    @Test
    public void initialConnectionFailureTest() throws Exception {
        try (MockSamlIdpServer mockSamlIdpServer = new MockSamlIdpServer()) {
            Map<String, Object> idpConfig = ImmutableMap.of("metadata_url", mockSamlIdpServer.getMetadataUri(), "entity_id",
                    mockSamlIdpServer.getIdpEntityId());
            Map<String, Object> config = ImmutableMap.of("idp", idpConfig, "roles_key", "roles", "idp.min_refresh_delay", 100);

            SamlAuthenticator samlAuthenticator = new SamlAuthenticator(config, testContext);
            ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
            authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            String encodedSamlResponse = "whatever";
            Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "frontend_base_url", FRONTEND_BASE_URL);

            try {
                samlAuthenticator.extractCredentials(request);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(e.toString(), "SAML authentication is currently unavailable", e.getMessage());
            }

            mockSamlIdpServer.start();

            mockSamlIdpServer.setSignResponses(true);
            mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
            mockSamlIdpServer.setAuthenticateUser("horst");
            mockSamlIdpServer.setEndpointQueryString(null);

            Thread.sleep(500);

            authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
            authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

            encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());
            request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
                    FRONTEND_BASE_URL);
            AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

            Assert.assertEquals("horst", authCredentials.getUsername());
        }
    }

    @Test
    public void subjectPatternTest() throws Exception {
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("leonard@example.com");
        mockSamlIdpServer.setEndpointQueryString(null);

        Map<String, Object> config = ImmutableMap.of("idp", basicIdpConfig, "roles_key", "roles", "subject_pattern", "^(.+)@(?:.+)$");

        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(config, testContext);
        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
        authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));

        String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());

        Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
                "frontend_base_url", FRONTEND_BASE_URL);
        AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);

        Assert.assertEquals("leonard", authCredentials.getUsername());
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
