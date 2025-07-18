package com.floragunn.searchguard.enterprise.auth.jwt;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.enterprise.auth.oidc.TestJwk;
import com.floragunn.searchguard.enterprise.auth.oidc.TestJwts;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.UserMapping;
import com.floragunn.searchguard.test.TestSgConfig.JsonWebKey;
import com.floragunn.searchguard.test.TestSgConfig.Jwks;
import com.floragunn.searchguard.test.TestSgConfig.JwtDomain;
import com.floragunn.searchguard.test.TestSgConfig.Signing;
import com.floragunn.searchguard.test.helper.cluster.BearerAuthorization;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Created based on legacy test <code>com.floragunn.dlic.auth.http.jwt.HTTPJwtAuthenticatorIntegrationTest</code>.
 */
public class HTTPJwtAuthenticatorIntegrationTest {

    private final static Logger log = LogManager.getLogger(HTTPJwtAuthenticatorIntegrationTest.class);

    public static final JwtDomain JWK_DOMAIN = new JwtDomain()
            .signing(new Signing().jwks(new Jwks().addKey(new JsonWebKey().kty("oct").kid("kid/a").use("sig").alg("HS256").k(TestJwk.OCT_1_K))))//
            .urlParameter("custom_jwt_param");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().roles(ALL_ACCESS)
            .roleMapping(new TestSgConfig.RoleMapping(ALL_ACCESS.getName()).backendRoles("role1")).authc(new Authc(new Domain("jwt").jwt(JWK_DOMAIN)
                    .userMapping(new UserMapping().rolesFromCommaSeparatedString("jwt.roles").attrsFrom("from_jwt_claim_n", "jwt.m"))))
            .build();

    /**
     * Moved from test_jwt.sh
     */
    @Test
    public void simple() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(new BearerAuthorization(TestJwts.MC_LIST_CLAIM_SIGNED_OCT_1))) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("user_name", is("McList"))));
            assertThat(response, json(nodeAt("backend_roles", containsInAnyOrder("role1", "kibana_user"))));
            assertThat(response, json(nodeAt("attribute_names", containsInAnyOrder("from_jwt_claim_n", "__auth_type"))));
        }
    }

    /**
     * Moved from test_jwt.sh
     */
    @Test
    public void urlParam() throws Exception {
        try (GenericRestClient client = cluster.getRestClient()) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo?custom_jwt_param=" + TestJwts.MC_LIST_CLAIM_SIGNED_OCT_1);
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("user_name", is("McList"))));
        }
    }

    /**
     * Tests a case where both a JWT header and a JWT URL param is sent. The header shall take precedence.
     * <p>
     * Moved from test_jwt.sh
     */
    @Test
    public void headerAndUrlParamMixed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(new BearerAuthorization(TestJwts.MC_COY_SIGNED_OCT_1))) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo?custom_jwt_param=" + TestJwts.MC_LIST_CLAIM_SIGNED_OCT_1);
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("user_name", is("Leonard McCoy"))));
        }
    }

    @Test
    public void shouldCreateSessionWithJwtToken() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            HttpResponse response = client.get("/_searchguard/auth/config?next_url=/abc/def");
            log.info("Auth config response: '{}'", response.getBody());
            String body = DocNode.of("method", "jwt", "jwt", TestJwts.MC_COY_SIGNED_OCT_1).toJsonString();

            response = client.postJson("/_searchguard/auth/session", body);

            log.info("POST /_searchguard/auth/session response '{}'.", response.getBody());
            assertThat(response.getBody(), response.getStatusCode(), equalTo(201));
            String token = response.getBodyAsDocNode().getAsString("token");
            try (GenericRestClient tokenClient = cluster.getRestClient(new BearerAuthorization(token))) {
                response = tokenClient.get("/_searchguard/authinfo");
                log.info("Session token '{}' used to retrieve auth info '{}'.", token, response.getBody());
                assertThat(response.getStatusCode(), equalTo(200));
            }
        }
    }

    @Test
    public void shouldNotCreateSessionWhenTokenSignatureIsIncorrect() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            HttpResponse response = client.get("/_searchguard/auth/config?next_url=/abc/def");
            log.info("Auth config response: '{}'", response.getBody());
            //token with invalid signature
            String body = DocNode.of("method", "jwt", "jwt", TestJwts.MC_COY_SIGNED_RSA_1).toJsonString();

            response = client.postJson("/_searchguard/auth/session", body);

            log.info("POST /_searchguard/auth/session response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(401));
        }
    }

    @Test
    public void shouldNotCreateSessionWhenTokenIsMissing() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            HttpResponse response = client.get("/_searchguard/auth/config?next_url=/abc/def");
            log.info("Auth config response: '{}'", response.getBody());
            //missing JWT token
            String body = DocNode.of("method", "jwt").toJsonString();

            response = client.postJson("/_searchguard/auth/session", body);

            log.info("POST /_searchguard/auth/session response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(401));
        }
    }
}
