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

package com.floragunn.dlic.auth.http.jwt.keybyoidc;

import static com.floragunn.searchguard.enterprise.auth.oidc.TestJwts.MCCOY_SUBJECT;
import static com.floragunn.searchguard.enterprise.auth.oidc.TestJwts.ROLES_CLAIM;
import static com.floragunn.searchguard.enterprise.auth.oidc.TestJwts.TEST_AUDIENCE;
import static com.floragunn.searchguard.enterprise.auth.oidc.TestJwts.create;
import static com.floragunn.searchguard.enterprise.auth.oidc.TestJwts.createSigned;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.floragunn.searchsupport.proxy.wiremock.WireMockRequestHeaderAddingFilter;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.internal.LoggerFactoryImpl;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.internal.spi.LoggerFactory;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.util.FakeRestRequest;
import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

@Deprecated
public class HTTPJwtKeyByOpenIdConnectAuthenticatorTest {

    static {
        LoggerFactory.setInstance(new LoggerFactoryImpl());
    }

    protected static MockIpdServer mockIdpServer;

    private static final WireMockRequestHeaderAddingFilter REQUEST_HEADER_ADDING_FILTER = new WireMockRequestHeaderAddingFilter("Proxy", "wire-mock");

    @ClassRule
    public static WireMockRule wireMockProxy = new WireMockRule(WireMockConfiguration.options()
            .bindAddress("127.0.0.8")
            .enableBrowserProxying(true)
            .proxyPassThrough(true)
            .dynamicPort()
            .extensions(REQUEST_HEADER_ADDING_FILTER));

    @BeforeClass
    public static void setUp() throws Exception {
        mockIdpServer = MockIpdServer.start(TestJwk.Jwks.ALL);
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
    }

    @Test
    public void basicTest() {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString()).build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_OCT_1), new HashMap<String, String>()), null);

        Assert.assertNotNull(creds);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
        Assert.assertEquals(0, creds.getBackendRoles().size());
        Assert.assertEquals(3, creds.getAttributes().size());
    }

    @Test
    public void proxyTest() throws Exception {
        try (MockIpdServer proxyOnlyMockIdpServer = MockIpdServer.start(TestJwk.Jwks.ALL)
                .acceptOnlyRequestsWithHeader(REQUEST_HEADER_ADDING_FILTER.getHeader())) {
            proxyOnlyMockIdpServer.setRequireValidCodes(false);

            Settings settings = Settings.builder().put("openid_connect_url", proxyOnlyMockIdpServer.getDiscoverUri().toString()).put("proxy.host", "127.0.0.8")
                    .put("proxy.port", wireMockProxy.port()).put("proxy.scheme", "http").build();

            HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

            AuthCredentials creds = jwtAuth.extractCredentials(
                    new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_OCT_1), new HashMap<String, String>()), null);

            Assert.assertNotNull(creds);
            Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
            Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
            Assert.assertEquals(0, creds.getBackendRoles().size());
            Assert.assertEquals(3, creds.getAttributes().size());

            FakeRestRequest restRequest = new FakeRestRequest();
            TestRestChannel restChannel = new TestRestChannel(restRequest);

            jwtAuth.handleMetaRequest(restRequest, restChannel, "/_searchguard/test/openid", "config", null);
            String response = restChannel.response.content().utf8ToString();
            Map<String, Object> parsedResponse = DocReader.json().readObject(response);

            Assert.assertTrue(response, parsedResponse.containsKey("token_endpoint_proxy"));

            restRequest = new FakeRestRequest.Builder().withMethod(Method.POST)
                    .withContent(new BytesArray("grant_type=authorization_code&code=wusch"))
                    .withHeaders(ImmutableMap.of("Content-Type", "application/x-www-form-urlencoded")).build();
            restChannel = new TestRestChannel(restRequest);

            jwtAuth.handleMetaRequest(restRequest, restChannel, "/_searchguard/test/openid", "token", null);

            response = restChannel.response.content().utf8ToString();
            //System.out.println(response);
            parsedResponse = DocReader.json().readObject(response);

            Assert.assertTrue(response, parsedResponse.containsKey("id_token"));
        }
    }

    @Test
    public void bearerTest() {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString()).build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", "Bearer " + TestJwts.MC_COY_SIGNED_OCT_1), new HashMap<String, String>()), null);

        Assert.assertNotNull(creds);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
        Assert.assertEquals(0, creds.getBackendRoles().size());
        Assert.assertEquals(3, creds.getAttributes().size());
    }

    @Test
    public void testRoles() throws Exception {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString()).put("roles_key", TestJwts.ROLES_CLAIM)
                .build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_OCT_1), new HashMap<String, String>()), null);

        Assert.assertNotNull(creds);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
        Assert.assertEquals(TestJwts.TEST_ROLES, creds.getBackendRoles());
    }

    @Test
    public void testRolesJsonPath() throws Exception {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString())
                .put("roles_path", "$." + TestJwts.ROLES_CLAIM).put("subject_path", "$.sub").build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth
                .extractCredentials(new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_OCT_1), new HashMap<>()), null);

        Assert.assertNotNull(creds);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
        Assert.assertEquals(TestJwts.TEST_ROLES, creds.getBackendRoles());
    }

    @Test
    public void testRolesCollectionJsonPath() throws Exception {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString())
                .put("roles_path", "$." + TestJwts.ROLES_CLAIM).put("subject_path", "$.sub").build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth
                .extractCredentials(
                        new FakeRestRequest(
                                ImmutableMap
                                        .of("Authorization",
                                                createSigned(create(MCCOY_SUBJECT, TEST_AUDIENCE, ROLES_CLAIM,
                                                        Arrays.asList("role 1", "role 2", "role 3, role 4")), TestJwk.OCT_1)),
                                new HashMap<>()),
                        null);

        Assert.assertNotNull(creds);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
        Assert.assertThat(creds.getBackendRoles(), CoreMatchers.hasItems("role 1", "role 2", "role 3", "role 4"));
    }

    @Test
    public void testInvalidSubjectJsonPath() throws Exception {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString())
                .put("roles_path", "$." + TestJwts.ROLES_CLAIM).put("subject_path", "$.subasd").build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_OCT_1), new HashMap<String, String>()), null);

        Assert.assertNull(creds);
    }

    @Test
    public void testInvalidRolesJsonPath() throws Exception {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString())
                .put("roles_path", "$.asd" + TestJwts.ROLES_CLAIM).put("subject_path", "$.sub").build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_OCT_1), new HashMap<String, String>()), null);

        Assert.assertNotNull(creds);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
        Assert.assertEquals(Collections.emptySet(), creds.getBackendRoles());
    }

    @Test
    public void testExp() throws Exception {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString()).build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_EXPIRED_SIGNED_OCT_1), new HashMap<String, String>()), null);

        Assert.assertNull(creds);
    }

    @Test
    public void testRS256() throws Exception {

        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString()).build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_RSA_1), new HashMap<String, String>()), null);

        Assert.assertNotNull(creds);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
        Assert.assertEquals(0, creds.getBackendRoles().size());
        Assert.assertEquals(3, creds.getAttributes().size());
    }

    @Test
    public void testBadSignature() throws Exception {

        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString()).build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_RSA_X), new HashMap<String, String>()), null);

        Assert.assertNull(creds);
    }

    @Test
    public void testPeculiarJsonEscaping() {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString()).build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.PeculiarEscaping.MC_COY_SIGNED_RSA_1), new HashMap<String, String>()),
                null);

        Assert.assertNotNull(creds);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
        Assert.assertEquals(0, creds.getBackendRoles().size());
        Assert.assertEquals(3, creds.getAttributes().size());
    }
    
    @Test
    public void testSubjectPattern() {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString()).put("subject_pattern", "^(.)(?:.*)$").build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_OCT_1), new HashMap<String, String>()), null);

        Assert.assertNotNull(creds);
        Assert.assertEquals(TestJwts.MCCOY_SUBJECT.substring(0, 1), creds.getUsername());
        Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
        Assert.assertEquals(0, creds.getBackendRoles().size());
        Assert.assertEquals(3, creds.getAttributes().size());
    }

    @Test
    public void testSubjectJsonPathWithList() throws Exception {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString())
                .put("roles_path", "$." + TestJwts.ROLES_CLAIM).put("subject_path", "$.n").build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_LIST_CLAIM_SIGNED_OCT_1), new HashMap<>()), null);

        Assert.assertNotNull(creds);
        Assert.assertEquals("mcl", creds.getUsername());
        Assert.assertEquals(TestJwts.TEST_ROLES, creds.getBackendRoles());
    }

    @Test
    public void testSubjectJsonPathWithListSize2() throws Exception {
        Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri().toString())
                .put("roles_path", "$." + TestJwts.ROLES_CLAIM).put("subject_path", "$.n").build();

        HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

        AuthCredentials creds = jwtAuth.extractCredentials(
                new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_LIST_2_CLAIM_SIGNED_OCT_1), new HashMap<>()), null);

        Assert.assertNull(creds);
    }

    static class TestRestChannel implements RestChannel {

        final RestRequest restRequest;
        RestResponse response;

        TestRestChannel(RestRequest restRequest) {
            this.restRequest = restRequest;
        }

        @Override
        public XContentBuilder newBuilder() throws IOException {
            return null;
        }

        @Override
        public XContentBuilder newErrorBuilder() throws IOException {
            return null;
        }

        @Override
        public XContentBuilder newBuilder(XContentType xContentType, boolean useFiltering) throws IOException {
            return null;
        }

        @Override
        public BytesStreamOutput bytesOutput() {
            return null;
        }

        @Override
        public void releaseOutputBuffer() {

        }

        @Override
        public RestRequest request() {
            return restRequest;
        }

        @Override
        public boolean detailedErrorsEnabled() {
            return false;
        }

        @Override
        public void sendResponse(RestResponse response) {
            this.response = response;

        }

        @Override
        public XContentBuilder newBuilder(XContentType xContentType, XContentType responseContentType, boolean useFiltering) throws IOException {
            return null;
        }

        @Override
        public XContentBuilder newBuilder(XContentType xContentType, XContentType responseContentType, boolean useFiltering, OutputStream out) throws IOException {
            return null;
        }
    }

}
