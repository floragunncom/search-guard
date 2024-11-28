/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auth.session;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.BearerAuthorization;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class ExternalSearchGuardSessionAuthenticationBackendTest {

    final static TestSgConfig.User SESSION_TEST_USER = new TestSgConfig.User("session_test_user").roles("sg_all_access", "SGS_KIBANA_USER");
    final static String HS512_KEY = "rJr-CU8cedCQxHetNz5jgNWVPrfDmgUMjiNcXmvxODZozLkNCbgDQRneS6kNXlnOLFC8IKx5mACOmcd4bsDD2w";
    final static DocNode HS512_JWK = DocNode.of("kty", "oct", "kid", "kid_a", "k", HS512_KEY, "alg", "HS512");
    final static String SESSION_JWT_AUDIENCE = "test_session_audience";

    @ClassRule
    public static LocalCluster sessionProvidingCluster = new LocalCluster.Builder().sgConfig(//
            new TestSgConfig()//
                    .authc(new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db")))//
                    .frontendAuthc("default", new TestSgConfig.FrontendAuthc()
                            .authDomain(new TestSgConfig.FrontendAuthDomain("basic").label("Basic Login")))//
                    .sessions(new TestSgConfig.Sessions().jwtSigningKeyHs512(HS512_KEY).jwtAudience(SESSION_JWT_AUDIENCE))//
                    .user(SESSION_TEST_USER))
            .singleNode().sslEnabled().enterpriseModulesEnabled().build();

    @ClassRule
    public static LocalCluster sessionConsumingCluster = new LocalCluster.Builder().sgConfig(//
            new TestSgConfig()//
                    .authc(new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("jwt/external_session")
                            .frontend(DocNode.of("signing.jwks.keys", Arrays.asList(HS512_JWK), "required_audience", SESSION_JWT_AUDIENCE))
                            .backend(DocNode.of("hosts",
                                    ImmutableList.of("#{var:session_hosts}", "https://invalidhost.example.com:9200",
                                            "https://invalidhost2.example.com:9200"),
                                    "tls.trust_all", true, "tls.verify_hostnames", false))))//
                    .var("session_hosts", () -> sessionProvidingCluster.getHttpAddressAsURI().toString()))
            .singleNode().sslEnabled().enterpriseModulesEnabled().build();

    @Test
    public void basicTest() throws Exception {
        String token;

        try (GenericRestClient restClient = sessionProvidingCluster.getRestClient()) {
            HttpResponse response = restClient.postJson("/_searchguard/auth/session",
                    DocNode.of("mode", "basic", "user", SESSION_TEST_USER.getName(), "password", SESSION_TEST_USER.getPassword()));

            //System.out.println(response.getBody());

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            token = response.getBodyAsDocNode().getAsString("token");

            Assert.assertNotNull(response.getBody(), token);
        }

        try (GenericRestClient restClient = sessionConsumingCluster.getRestClient(new BearerAuthorization(token))) {
            HttpResponse response = restClient.get("/_searchguard/authinfo");

            //System.out.println(response.getBody());

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), SESSION_TEST_USER.getName(), response.getBodyAsDocNode().getAsString("user_name"));
            Assert.assertEquals(response.getBody(), SESSION_TEST_USER.getRoleNames(),
                    ImmutableSet.of(response.getBodyAsDocNode().getAsListOfStrings("sg_roles")));
        }
    }
}
