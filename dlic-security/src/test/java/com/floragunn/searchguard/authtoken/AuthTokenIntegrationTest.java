package com.floragunn.searchguard.authtoken;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class AuthTokenIntegrationTest {

    private static String SGCONFIG = //
            "_sg_meta:\n" + //
                    "  type: \"config\"\n" + //
                    "  config_version: 2\n" + //
                    "\n" + //
                    "sg_config:\n" + //
                    "  dynamic:\n" + //
                    "    auth_token_provider: \n" + //
                    "      enabled: true\n" + //
                    "      jwt_signing_key: \"bmRSMW00c2pmNUk4Uk9sVVFmUnhjZEhXUk5Hc0V5MWgyV2p1RFE3Zk1wSTE=\"\n" + // 
                    "      jwt_aud: \"searchguard_tokenauth\"\n" + //
                    "      max_validity: \"1y\"    \n" + //
                    "    authc:\n" + //
                    "      sg_issued_jwt_auth_domain:\n" + //
                    "        description: \"Authenticate via Json Web Tokens issued by Search Guard\"\n" + //
                    "        http_enabled: true\n" + //
                    "        transport_enabled: false\n" + //
                    "        order: 0\n" + //
                    "        http_authenticator:\n" + //
                    "          type: jwt\n" + //
                    "          challenge: false\n" + //
                    "          config:\n" + //
                    "            signing_key: \"bmRSMW00c2pmNUk4Uk9sVVFmUnhjZEhXUk5Hc0V5MWgyV2p1RFE3Zk1wSTE=\"\n" + //
                    "            jwt_header: \"Authorization\"\n" + //
                    "        authentication_backend:\n" + //
                    "          type: authtoken";

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().sgConfig(CType.CONFIG, SGCONFIG).build();

    private static RestHelper rh = null;

    @BeforeClass
    public static void setupDependencies() {
        rh = cluster.restHelper();
    }

    @Test
    public void basicTest() throws Exception {
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: 'at_test*'\n  allowed_actions: '*'"));

        request.setTokenName("my_new_token");

        Header auth = basicAuth("spock", "spock");

        HttpResponse response = rh.executePostRequest("/_searchguard/authtoken", request.toJson(), auth);

        System.out.println(response.getBody());
    }

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }
}
