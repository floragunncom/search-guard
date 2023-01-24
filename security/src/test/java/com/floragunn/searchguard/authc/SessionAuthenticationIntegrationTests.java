/*
 * Copyright 2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.searchguard.authc;

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
