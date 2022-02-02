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

package com.floragunn.searchguard.enterprise.auth;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;

import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.enterprise.auth.jwt.Jose;
import com.floragunn.searchguard.enterprise.auth.oidc.TestJwk;
import com.floragunn.searchguard.enterprise.auth.oidc.TestJwts;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.UserMapping;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.util.ImmutableSet;

public class RestAuthenticationIntegrationTests {

    static TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(
            new Authc.Domain("jwt").frontend(DocNode.of("signing.jwks", Jose.toBasicObject(TestJwk.OCT_1_2_3)))//
                    .skipIps("127.0.0.4")//
                    .userMapping(new UserMapping().rolesFrom("jwt.n").attrsFrom("a_n", "jwt.n").attrsFrom("a_m", "jwt.m")),

            new Authc.Domain("jwt").frontend(DocNode.of("signing.jwks", Jose.toBasicObject(TestJwk.OCT_1_2_3)))//
                    .acceptIps("127.0.0.4")//
                    .userMapping(new UserMapping().rolesFromCommaSeparatedString("jwt.roles")));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().authc(AUTHC).enterpriseModulesEnabled().build();

    @Test
    public void jwt() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(new BasicHeader("Authorization", "bearer " + TestJwts.MC_LIST_2_CLAIM_SIGNED_OCT_1))) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "McList", response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), Arrays.asList("mcl", "mcl2"), response.getBodyAsDocNode().get("backend_roles"));
            Assert.assertTrue(response.getBody(), ((Collection<?>) response.getBodyAsDocNode().get("attribute_names")).contains("a_m"));
        }
    }

    @Test
    public void jwt_commaSeparatedRoles() throws Exception {

        try (GenericRestClient client = cluster.getRestClient(new BasicHeader("Authorization", "bearer " + TestJwts.MC_COY_SIGNED_OCT_1))) {
            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 4 }));

            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), TestJwts.MC_COY.getClaims().getSubject(), response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), TestJwts.TEST_ROLES,
                    ImmutableSet.of((Collection<?>) response.getBodyAsDocNode().get("backend_roles")));
        }
    }

}
