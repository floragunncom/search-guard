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

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
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

        TestSgConfig testSgConfig = new TestSgConfig().resources("saml")
                .frontendAuthcz(new TestSgConfig.FrontendAuthcz("saml").label("SAML Label").config("roles_key", "roles", "idp.metadata_url",
                        mockSamlIdpServer.getMetadataUri(), "idp.entity_id", mockSamlIdpServer.getIdpEntityId()));

        cluster = new LocalCluster.Builder().sslEnabled().singleNode().resources("saml").sgConfig(testSgConfig).build();
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
    public void basicTest() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {

            HttpResponse response = client.get("/_searchguard/auth/config?next_url=/abc/def&frontend_base_url=" + FRONTEND_BASE_URL);

            System.out.println(response.getBody());
            
            String ssoLocation = response.toJsonNode().path("auth_methods").path(0).path("sso_location").textValue();
            String ssoContext = response.toJsonNode().path("auth_methods").path(0).path("sso_context").textValue();
            String id = response.toJsonNode().path("auth_methods").path(0).path("id").textValue();

            Assert.assertNotNull(response.getBody(), ssoLocation);

            String encodedSamlResponse = mockSamlIdpServer.handleSsoGetRequestURI(ssoLocation);

            response = client.postJson("/_searchguard/auth/session", ImmutableMap.of("method", "saml", "id", id, "saml_response", encodedSamlResponse,
                    "sso_context", ssoContext, "frontend_base_url", FRONTEND_BASE_URL));

            System.out.println(response.getBody());

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            String token = response.toJsonNode().path("token").textValue();

            Header tokenAuth = new BasicHeader("Authorization", "Bearer " + token);

            try (GenericRestClient tokenClient = cluster.getRestClient(tokenAuth)) {

                response = tokenClient.get("/_searchguard/authinfo");

                System.out.println(response.getBody());

                String logoutAddress = response.toJsonNode().path("sso_logout_url").textValue();

                Assert.assertNotNull(logoutAddress);
            }
        }

    }

}
