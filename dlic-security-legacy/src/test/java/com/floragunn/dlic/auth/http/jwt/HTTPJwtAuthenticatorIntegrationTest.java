/*
  * Copyright 2023 by floragunn GmbH - All rights reserved
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
package com.floragunn.dlic.auth.http.jwt;

import com.floragunn.searchguard.enterprise.auth.oidc.TestJwts;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class HTTPJwtAuthenticatorIntegrationTest {
    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().resources("jwt").sslEnabled().enterpriseModulesEnabled().build();

    @Test
    public void basicTest() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {

            HttpResponse response = client.get("/_searchguard/auth/config?next_url=/abc/def");

            System.out.println(response.getBody());

            response = client.postJson("/_searchguard/auth/session", "{\"method\": \"jwt\", \"jwt\": \"" + TestJwts.MC_COY_SIGNED_OCT_1 + "\" }");

            System.out.println(response.getBody());

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            String token = response.toJsonNode().path("token").textValue();

            Header tokenAuth = new BasicHeader("Authorization", "Bearer " + token);

            try (GenericRestClient tokenClient = cluster.getRestClient(tokenAuth)) {

                response = tokenClient.get("/_searchguard/authinfo");

                System.out.println(response.getBody());

            }
        }

    }
}
