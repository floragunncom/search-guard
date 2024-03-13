/*
 * Copyright 2021 by floragunn GmbH - All rights reserved
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

import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchsupport.proxy.wiremock.WireMockRequestHeaderAddingFilter;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class OidcAuthenticatorIntegrationTest {
    protected static MockIpdServer mockIdpServer;

    private static final TestCertificates testCertificates = TestCertificates.builder()
            .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard", 2, "password")
            .addClients("CN=client.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .build();

    private static final WireMockRequestHeaderAddingFilter REQUEST_HEADER_ADDING_FILTER = new WireMockRequestHeaderAddingFilter("Proxy", "wire-mock");


    @ClassRule
    public static WireMockClassRule wireMockProxy = new WireMockClassRule(WireMockConfiguration.options()
            .bindAddress("127.0.0.8")
            .caKeystorePath(testCertificates.getCaCertificate().getJksFile().getAbsolutePath())
            .trustAllProxyTargets(true)
            .enableBrowserProxying(true)
            .dynamicPort()
            .extensions(REQUEST_HEADER_ADDING_FILTER));

    private static String FRONTEND_BASE_URL = "http://whereever";
    private static final TLSConfig IDP_TLS_CONFIG;
    public static LocalCluster.Embedded cluster;

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

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @BeforeClass
    public static void setUp() throws Exception {

        mockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL)
                .acceptOnlyRequestsWithHeader(REQUEST_HEADER_ADDING_FILTER.getHeader())
                .useCustomTlsConfig(IDP_TLS_CONFIG).start();

        TestSgConfig testSgConfig = new TestSgConfig().resources("oidc").frontendAuthc(new TestSgConfig.FrontendAuthc()
                .authDomain(new TestSgConfig.FrontendAuthDomain("oidc").label("Label").config(
                        "oidc.idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "oidc.client_id", "Der Klient", "oidc.client_secret",
                        "Das Geheimnis", "user_mapping.roles.from", ImmutableMap.of("json_path", "jwt.roles", "split", ","), "oidc.idp.proxy.host",
                        "127.0.0.8", "oidc.idp.proxy.port", wireMockProxy.port(), "oidc.idp.proxy.scheme", "http", "oidc.idp.tls.trusted_cas",
                        "#{file:" + testCertificates.getCaCertificate().getCertificateFile().getAbsolutePath() + "}", "oidc.idp.tls.verify_hostnames", false)));

        cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().singleNode().resources("oidc").sgConfig(testSgConfig).embedded().start();
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

            String nextUrl = "/abc/def";

            HttpResponse response = client.get("/_searchguard/auth/config?next_url=" + nextUrl + "&frontend_base_url=" + FRONTEND_BASE_URL);

            System.out.println(response.getBody());

            String ssoLocation = response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("sso_location");
            String ssoContext = response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("sso_context");
            String id = response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("id");


            Assert.assertNotNull(response.getBody(), ssoLocation);

            String ssoResult = mockIdpServer.handleSsoGetRequestURI(ssoLocation, TestJwts.MC_COY_SIGNED_OCT_1);

            response = client.postJson("/_searchguard/auth/session", DocNode.of("method", "oidc", "id", id, "sso_result", ssoResult, "sso_context",
                    ssoContext, "frontend_base_url", FRONTEND_BASE_URL));

            System.out.println(response.getBody());

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());
            Assert.assertEquals(nextUrl, response.getBodyAsDocNode().getAsString("redirect_uri"));
            
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
}
