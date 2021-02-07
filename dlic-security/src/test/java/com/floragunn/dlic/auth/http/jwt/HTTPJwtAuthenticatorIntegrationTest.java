package com.floragunn.dlic.auth.http.jwt;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.dlic.auth.http.jwt.keybyoidc.TestJwts;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;

public class HTTPJwtAuthenticatorIntegrationTest {
    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().resources("jwt").sslEnabled().build();

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
