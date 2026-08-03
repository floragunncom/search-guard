/*
 * Copyright 2026 by floragunn GmbH - All rights reserved
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 */
package com.floragunn.searchguard.enterprise.auth.oidc;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.enterprise.auth.jwt.Jose;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.AdditionalUserInformation;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.UserMapping;
import com.floragunn.searchguard.test.helper.cluster.BearerAuthorization;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class OidcUserInfoBackendIntegrationTest {
    private static final String USER_INFO_ROLE = "role_from_user_info";

    private static MockIpdServer mockIdpServer;
    private static LocalCluster.Embedded cluster;

    @BeforeClass
    public static void setUp() throws Exception {
        mockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL).start();
        mockIdpServer.userInfoForAccessToken(TestJwts.MC_COY_SIGNED_OCT_1,
                java.util.Map.of("sub", TestJwts.MCCOY_SUBJECT, "roles", java.util.List.of(USER_INFO_ROLE)));

        Authc authc = new Authc(new Domain("jwt")
                .frontend(DocNode.of("signing.jwks", Jose.toBasicObject(TestJwk.OCT_1_2_3)))
                .additionalUserInformation(new AdditionalUserInformation("oidc_userinfo",
                        DocNode.of("openid_configuration_url", mockIdpServer.getDiscoverUri().toString())))
                .userMapping(new UserMapping().rolesFrom("oidc_user_info.roles")));

        cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled().authc(authc).embedded().start();
    }

    @AfterClass
    public static void tearDown() {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
        if (mockIdpServer != null) {
            try {
                mockIdpServer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            mockIdpServer = null;
        }
    }

    @Test
    public void shouldEnrichJwtUserWithInformationFromUserInfoEndpoint() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(new BearerAuthorization(TestJwts.MC_COY_SIGNED_OCT_1))) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("user_name", is(TestJwts.MCCOY_SUBJECT))));
            assertThat(response, json(nodeAt("backend_roles", contains(USER_INFO_ROLE))));
        }
    }
}
