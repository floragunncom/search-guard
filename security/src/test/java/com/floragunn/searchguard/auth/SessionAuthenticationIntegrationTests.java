package com.floragunn.searchguard.auth;

public class SessionAuthenticationIntegrationTests {
    // TODO subject pattern 
    //  @Test
//    public void subjectPatternTest() throws Exception {
//        mockSamlIdpServer.setSignResponses(true);
//        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
//        mockSamlIdpServer.setAuthenticateUser("leonard@example.com");
//        mockSamlIdpServer.setEndpointQueryString(null);
//
//        Map<String, Object> config = ImmutableMap.of("idp", basicIdpConfig, "user_mapping.roles", "roles", "user_mapping.subject_pattern",
//                "^(.+)@(?:.+)$");
//
//        SamlAuthenticator samlAuthenticator = new SamlAuthenticator(config, testContext);
//        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("saml", "SAML", null);
//        authMethod = samlAuthenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));
//
//        String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation());
//
//        Map<String, Object> request = ImmutableMap.of("saml_response", encodedSamlResponse, "sso_context", authMethod.getSsoContext(),
//                "frontend_base_url", FRONTEND_BASE_URL);
//        AuthCredentials authCredentials = samlAuthenticator.extractCredentials(request);
//
//        Assert.assertEquals("leonard", authCredentials.getUsername());
//    }
//
}
