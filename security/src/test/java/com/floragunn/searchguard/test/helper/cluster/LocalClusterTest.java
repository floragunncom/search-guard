package com.floragunn.searchguard.test.helper.cluster;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.RoleMapping;
import com.floragunn.searchguard.test.TestSgConfig.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class LocalClusterTest {

    private static final Logger log = LogManager.getLogger(LocalClusterTest.class);

    private static final String LIMITED_ROLE_NAME = "limited-role";
    private static final User USER_LIMITED = new User("limited-user").roles(LIMITED_ROLE_NAME);

    private static final User USER_WITHOUT_ROLE = new User("user-without-role");
    private static final User USER_ADMIN = new User("admin").roles(ALL_ACCESS.getName());
    private static final String INDEX_NAME = "some-index";
    @ClassRule
    public static LocalCluster.Embedded CLUSTER = new LocalCluster.Builder().singleNode()//
        .authc(new TestSgConfig.Authc(new Domain("basic/internal_users_db")))//
        .roles(ALL_ACCESS)//
        .user(USER_ADMIN).users(USER_LIMITED).user(USER_WITHOUT_ROLE)//
        .sslEnabled().enterpriseModulesEnabled().embedded().build();

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @BeforeClass
    public static void setupData() {
        try (Client client = CLUSTER.getPrivilegedInternalNodeClient()) {
            client.index(new IndexRequest(INDEX_NAME).id("contradiction").source("yes", "no")//
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        }
    }

    @Test
    public void shouldUpdateAndRestoreConfigurationOfRoles() throws Exception {
        Role role = new Role(LIMITED_ROLE_NAME).clusterPermissions("*").indexPermissions("*").on(INDEX_NAME);

        try (GenericRestClient client = CLUSTER.getRestClient(USER_LIMITED)) {

            HttpResponse response = client.get("/some-index/_search");
            log.info("Search response before config update '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(403));
        }

        AtomicBoolean updateExecutedCorrectly = new AtomicBoolean(false);

        HttpResponse response = CLUSTER.callAndRestoreConfig(CType.ROLES, () -> {
            CLUSTER.updateRolesConfig(role);
            try (GenericRestClient client = CLUSTER.getRestClient(USER_LIMITED)) {
                updateExecutedCorrectly.set(true);
                return client.get("/some-index/_search");
            }
        });

        log.info("Search response after config update '{}'", response.getBody());
        assertThat(response.getStatusCode(), equalTo(200));

        assertThat(updateExecutedCorrectly.get(), equalTo(true));

        try (GenericRestClient client = CLUSTER.getRestClient(USER_LIMITED)) {

            response = client.get("/some-index/_search");
            log.info("Search response after restore config '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(403));
        }
    }

    @Test
    public void shouldUpdateAndRestoreConfigurationOfRoleMappings() throws Exception {
        RoleMapping mapping = new RoleMapping(ALL_ACCESS.getName()).users(USER_WITHOUT_ROLE.getName());

        try (GenericRestClient client = CLUSTER.getRestClient(USER_WITHOUT_ROLE)) {
            HttpResponse response = client.get("/some-index/_search");
            log.info("Search response before config update '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(403));
        }

        AtomicBoolean updateExecutedCorrectly = new AtomicBoolean(false);

        HttpResponse response = CLUSTER.callAndRestoreConfig(CType.ROLESMAPPING, () -> {
            CLUSTER.updateRolesMappingsConfig(mapping);
            try (GenericRestClient client = CLUSTER.getRestClient(USER_WITHOUT_ROLE)) {
                updateExecutedCorrectly.set(true);
                return client.get("/some-index/_search");
            }
        });

        assertThat(updateExecutedCorrectly.get(), equalTo(true));

        log.info("Search response after config update '{}'", response.getBody());
        assertThat(response.getStatusCode(), equalTo(200));

        try (GenericRestClient client = CLUSTER.getRestClient(USER_WITHOUT_ROLE)) {
            response = client.get("/some-index/_search");
            log.info("Search response after config restore '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(403));
        }
    }
}