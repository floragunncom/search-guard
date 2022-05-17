/*
 * Copyright 2022 floragunn GmbH
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

public class UserMappingTest {
    //    @Test
    //    public void testInvalidRolesJsonPath() throws Exception {
    //        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "user_mapping.roles",
    //                "$.asd", "user_mapping.subject", "$.sub", "client_id", "Der Klient", "client_secret", "Das Geheimnis");
    //
    //        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
    //        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
    //        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));
    //
    //        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);
    //
    //        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
    //                FRONTEND_BASE_URL);
    //
    //        try {
    //            AuthCredentials authCredentials = authenticator.extractCredentials(request);
    //            Assert.fail("Expected exception, got: " + authCredentials);
    //        } catch (CredentialsException e) {
    //            Assert.assertTrue(e.getMessage(), e.getMessage().contains("The roles JSON path was not found in the Id token claims"));
    //        }
    //    }

    //    @Test
    //    public void testInvalidSubjectJsonPath() throws Exception {
    //        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "user_mapping.roles",
    //                "$." + TestJwts.ROLES_CLAIM, "user_mapping.subject", "$.subasd", "client_id", "Der Klient", "client_secret", "Das Geheimnis");
    //
    //        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
    //        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
    //        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));
    //
    //        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);
    //
    //        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
    //                FRONTEND_BASE_URL);
    //
    //        try {
    //            AuthCredentials authCredentials = authenticator.extractCredentials(request);
    //            Assert.fail("Expected exception, got: " + authCredentials);
    //        } catch (CredentialsException e) {
    //            Assert.assertTrue(e.getMessage(), e.getMessage().contains("The configured JSON Path could not be found in the JWT"));
    //        }
    //    }
    //    @Test
    //    public void testRolesCollectionJsonPath() throws Exception {
    //        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "user_mapping.roles",
    //                TestJwts.ROLES_CLAIM, "user_mapping.subject", "$.sub", "client_id", "Der Klient", "client_secret", "Das Geheimnis");
    //
    //        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
    //        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
    //        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));
    //
    //        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(),
    //                createSigned(create(MCCOY_SUBJECT, TEST_AUDIENCE, ROLES_CLAIM, Arrays.asList("role 1", "role 2", "role 3, role 4")), TestJwk.OCT_1));
    //
    //        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
    //                FRONTEND_BASE_URL);
    //
    //        AuthCredentials authCredentials = authenticator.extractCredentials(request);
    //
    //        Assert.assertNotNull(authCredentials);
    //        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
    //        Assert.assertThat(authCredentials.getBackendRoles(), CoreMatchers.hasItems("role 1", "role 2", "role 3", "role 4"));
    //    }

    //    @Test
    //    public void testRolesJsonPath() throws Exception {
    //        Map<String, Object> config = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "user_mapping.roles",
    //                "$." + TestJwts.ROLES_CLAIM, "user_mapping.subject", "$.sub", "client_id", "Der Klient", "client_secret", "Das Geheimnis");
    //
    //        OidcAuthenticator authenticator = new OidcAuthenticator(config, testContext);
    //        ActivatedFrontendConfig.AuthMethod authMethod = new ActivatedFrontendConfig.AuthMethod("oidc", "OIDC", null);
    //        authMethod = authenticator.activateFrontendConfig(authMethod, new GetFrontendConfigAction.Request(null, null, FRONTEND_BASE_URL));
    //
    //        String ssoResponse = mockIdpServer.handleSsoGetRequestURI(authMethod.getSsoLocation(), TestJwts.MC_COY_SIGNED_OCT_1);
    //
    //        Map<String, Object> request = ImmutableMap.of("sso_result", ssoResponse, "sso_context", authMethod.getSsoContext(), "frontend_base_url",
    //                FRONTEND_BASE_URL);
    //
    //        AuthCredentials authCredentials = authenticator.extractCredentials(request);
    //
    //        Assert.assertNotNull(authCredentials);
    //        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, authCredentials.getUsername());
    //        Assert.assertEquals(TestJwts.TEST_ROLES, authCredentials.getBackendRoles());
    //    }
}
