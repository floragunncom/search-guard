package com.floragunn.searchguard.configuration.variables;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ConfigVarIntegrationTest {

    private static final Logger log = LogManager.getLogger(ConfigVarIntegrationTest.class);
    public static final String VALUE_PLAIN = "value_plain_for_index_name";
    public static final String VALUE_BCRYPT = "BCrypt_user_password";

    /**
     * Password for a user is set by a config var and bcrypt pipe function
     */
    private static User USER_BCRYPT_ADMIN = new User("bcrypt_admin")//
        .passwordExpression("#{var:plain_password|bcrypt}")//
        .roles(Role.ALL_ACCESS);

    private static User USER_TEST = new User("test") //
        .roles(new Role("var").clusterPermissions("*").indexPermissions("*").on("#{var:index_var_plain}"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder() //
        .var("index_var_plain", () -> VALUE_PLAIN) //
        .var("plain_password", () -> VALUE_BCRYPT) //
        .users(USER_TEST, USER_BCRYPT_ADMIN) //
        .enterpriseModulesEnabled() //
        .sslEnabled() //
        .build();

    @Test
    public void shouldUsePlainVariableValue() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TEST)) {

            Awaitility.await("create document ").until(() -> {
                HttpResponse response = client.postJson("/" + VALUE_PLAIN + "/_doc", DocNode.EMPTY);

                log.debug("Response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
                return SC_CREATED == response.getStatusCode();
            });
        }
    }

    @Test
    public void shouldUseBcryptPipeFunction() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_BCRYPT_ADMIN.getName(), VALUE_BCRYPT)) {

            HttpResponse response = client.postJson("/any_index/_doc", DocNode.EMPTY);

            log.debug("Response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
        }
    }
}
