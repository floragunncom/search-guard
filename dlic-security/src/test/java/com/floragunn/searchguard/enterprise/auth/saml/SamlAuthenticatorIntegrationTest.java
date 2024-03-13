/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;

public class SamlAuthenticatorIntegrationTest {
    static {

        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }

    }

    protected static MockSamlIdpServer mockSamlIdpServer;
    public static LocalCluster cluster;
    private static String FRONTEND_BASE_URL = "http://whereever";

    @BeforeClass
    public static void setUp() throws Exception {
        mockSamlIdpServer = new MockSamlIdpServer();
        mockSamlIdpServer.start();
        mockSamlIdpServer.setSignResponses(true);
        mockSamlIdpServer.loadSigningKeys("saml/kirk-keystore.jks", "kirk");
        mockSamlIdpServer.setAuthenticateUser("horst");
        mockSamlIdpServer.setAuthenticateUserRoles(Arrays.asList("SGS_KIBANA_USER"));
        mockSamlIdpServer.setEndpointQueryString(null);

        TestSgConfig testSgConfig = new TestSgConfig().resources("saml")//
                .frontendAuthc("default", //
                        new TestSgConfig.FrontendAuthc().authDomain(new TestSgConfig.FrontendAuthDomain("saml").label("SAML Label")//
                                .config("user_mapping.roles.from", "saml_response.roles", //
                                        "saml.idp.metadata_url", mockSamlIdpServer.getMetadataUri(), //
                                        "saml.idp.entity_id", mockSamlIdpServer.getIdpEntityId())))//
                .frontendAuthc("invalid", //
                        new TestSgConfig.FrontendAuthc().authDomain(new TestSgConfig.FrontendAuthDomain("saml").label("SAML Label")//
                                .config("user_mapping.roles.from", "saml_response.roles", //
                                        "saml.idp.metadata_url", mockSamlIdpServer.getMetadataUri(), //
                                        "saml.idp.entity_id", "invalid")))//
                .frontendAuthcDebug("invalid", true);

        cluster = new LocalCluster.Builder().sslEnabled().singleNode().resources("saml").enterpriseModulesEnabled().sgConfig(testSgConfig).embedded().start();
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

        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            cluster = null;
        }
    }

    @Test
    public void basic() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {

            HttpResponse response = client.get("/_searchguard/auth/config?next_url=/abc/def&frontend_base_url=" + FRONTEND_BASE_URL);

            String ssoLocation = response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("sso_location");
            String ssoContext = response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("sso_context");
            String id = response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("id");

            Assert.assertNotNull(response.getBody(), ssoLocation);

            String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(ssoLocation);

            response = client.postJson("/_searchguard/auth/session", ImmutableMap.of("method", "saml", "id", id, "saml_response", encodedSamlResponse,
                    "sso_context", ssoContext, "frontend_base_url", FRONTEND_BASE_URL));

            System.out.println(response.getBody());

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            String token = response.getBodyAsDocNode().getAsString("token");

            Header tokenAuth = new BasicHeader("Authorization", "Bearer " + token);

            try (GenericRestClient tokenClient = cluster.getRestClient(tokenAuth)) {

                response = tokenClient.get("/_searchguard/auth/session");

                System.out.println(response.getBody());

                String logoutAddress = response.getBodyAsDocNode().getAsString("sso_logout_url");

                Assert.assertNotNull(logoutAddress);
            }
        }
    }

    @Test
    public void loginFailure() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {

            HttpResponse response = client.get("/_searchguard/auth/config?next_url=/abc/def&frontend_base_url=" + FRONTEND_BASE_URL);

            System.out.println(response.getBody());

            String ssoLocation = response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("sso_location");
            String ssoContext = response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("sso_context");
            String id = response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("id");

            Assert.assertNotNull(response.getBody(), ssoLocation);

            response = client.postJson("/_searchguard/auth/session", ImmutableMap.of("method", "saml", "id", id, "saml_response", "invalid",
                    "sso_context", ssoContext, "frontend_base_url", FRONTEND_BASE_URL));

            System.out.println(response.getBody());

            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());

        }
    }

    @Test
    public void invalidEntityId() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {

            HttpResponse response = client
                    .get("/_searchguard/auth/config?config_id=invalid&next_url=/abc/def&frontend_base_url=" + FRONTEND_BASE_URL);

            Assert.assertEquals(response.getBody(), "Could not find entity descriptor for invalid",
                    response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("message_body"));
        }
    }

}
