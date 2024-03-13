package com.floragunn.searchguard.enterprise.dlsfls;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain;
import com.floragunn.searchguard.test.TestSgConfig.DlsFls;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.RoleMapping;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.ConfigurationUpdater;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containSubstring;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class InvalidRolesAndMappingConfigurationTest {

    private static final Logger log = LogManager.getLogger(InvalidRolesAndMappingConfigurationTest.class);

    private static final Authc AUTHC = new Authc(new Domain("basic/internal_users_db"));

    private static final DlsFls DLSFLS = new DlsFls().useImpl("flx").metrics("detailed");

    private static final User USER_ADMIN = new User("admin").roles(ALL_ACCESS.getName());
    public static final String LIMITED_ROLE_NAME = "limited-role";
    private static final User USER_LIMITED = new User("limited-user").roles(LIMITED_ROLE_NAME);

    private static final Role ROLE_VALID = new Role("valid-role").clusterPermissions("*").indexPermissions("*").fls("~secret").on("index*");

    private static final Role ROLE_USED_WITH_INCORRECT_MAPPING = new Role("invalid-mapping-role").clusterPermissions("*");
    private static final String ERROR_TYPE = "status_exception";

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded CLUSTER = new LocalCluster.Builder().singleNode().authc(AUTHC).dlsFls(DLSFLS)//
        .roles(ALL_ACCESS)//
        .user(USER_ADMIN).user(USER_LIMITED)//
        .sslEnabled().enterpriseModulesEnabled().embedded().build();

    private ConfigurationUpdater configurationUpdater;

    @Before
    public void beforeEach() {
        this.configurationUpdater = new ConfigurationUpdater(CLUSTER, USER_ADMIN);
    }

    @Test
    public void shouldPerformSearchWhenConfigurationIsValid() throws Exception {
        HttpResponse response = configurationUpdater.callWithRole(ROLE_VALID, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });
        
        assertThat(response.getStatusCode(), equalTo(200));

    }
    @Test
    public void shouldReportConfigurationErrorWhenRoleContainsInvalidIndexPattern() throws Exception {
        Role roleInvalidIndexPattern = new Role("invalid-role-index").clusterPermissions("*")//
            .indexPermissions("*").fls("~secret").on("/index-(.+/");

        HttpResponse response = configurationUpdater.callWithRole(roleInvalidIndexPattern, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldReportConfigurationErrorWhenRoleContainsInvalidPermissionPattern() throws Exception {
        Role roleWithInvalidPermission = new Role("invalid-role-permission").clusterPermissions("*")//
            .indexPermissions("/permission-(.+/").fls("~secret").on("index");

        HttpResponse response = configurationUpdater.callWithRole(roleWithInvalidPermission, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldReportConfigurationErrorWhenRoleContainsDefinedDlsAndInvalidIndexPattern() throws Exception {
        Role roleWithDlsIncorrectIndexPattern = new Role("invalid-role-dls-index-pattern").clusterPermissions("*")//
            .indexPermissions("*").dls("{\"match\":{\"country_code\":\"ad\"}}").fls("~secret").on("/index(.+/");

        HttpResponse response = configurationUpdater.callWithRole(roleWithDlsIncorrectIndexPattern, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldReportConfigurationErrorWhenRoleContainsIncorrectJsonInDlsQuery() throws Exception {
        Role roleWithIncorrectDlsJsonQuery = new Role("invalid-role-dls-incorrect-json").clusterPermissions("*")//
            .indexPermissions("*").dls("{\"match\":{\"country_code\":ad}").fls("~secret").on("index");

        HttpResponse response = configurationUpdater.callWithRole(roleWithIncorrectDlsJsonQuery, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldReportConfigurationErrorWhenRoleContainsIncorrectDlsQuery() throws Exception {
        Role roleWithIncorrectDlsQuery = new Role("invalid-role-incorrect-dls-query").clusterPermissions("*")//
            .indexPermissions("indices:data/read/search*", "indices:monitor/*").dls("{\"term\":{}}").fls("~secret")//
            .on("index");

        HttpResponse response = configurationUpdater.callWithRole(roleWithIncorrectDlsQuery, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldReportConfigurationErrorWhenRoleContainsIncorrectFlsPattern() throws Exception {
        Role roleWithIncorrectFlsPattern = new Role("invalid-role-fls-pattern").clusterPermissions("*")//
            .indexPermissions("indices:data/read/search*", "indices:monitor/*").dls("{\"match\":{\"country_code\":\"ad\"}}")//
            .fls("/public-(.+/").on("index");

        HttpResponse response = configurationUpdater.callWithRole(roleWithIncorrectFlsPattern, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldPerformSearchWhenConfigurationWithRolesAndMappingsIsValid() throws Exception {
        RoleMapping roleMappingsValid = new RoleMapping(ROLE_VALID.getName()).backendRoles("accountant").ips("192.178.1.5")//
            .hosts("*.search-guard.com");

        HttpResponse response = configurationUpdater.callWithMapping(roleMappingsValid, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThat(response.getStatusCode(), equalTo(200));
    }

    @Test
    public void shouldReportConfigurationErrorWhenMappingContainInvalidPatternInBackendRoleName() throws Exception {
        RoleMapping roleMappingsInvalidBackendRolePattern =
            new RoleMapping(ROLE_USED_WITH_INCORRECT_MAPPING.getName()).backendRoles("/accountant(.+/");

        HttpResponse response = configurationUpdater.callWithMapping(roleMappingsInvalidBackendRolePattern, client -> {
            assertThatUserCanAuthenticate(client);
           return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldReportConfigurationErrorWhenMappingContainInvalidPatternInUsername() throws Exception {
        RoleMapping roleMappingsInvalidUserPattern = new RoleMapping(ROLE_USED_WITH_INCORRECT_MAPPING.getName()).users("/admin-(.+/");

        HttpResponse response = configurationUpdater.callWithMapping(roleMappingsInvalidUserPattern, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldReportConfigurationErrorWhenMappingContainInvalidIpAddress() throws Exception {
        RoleMapping roleMappingsInvalidIpAddress =
            new RoleMapping(ROLE_USED_WITH_INCORRECT_MAPPING.getName()).ips("this is not an IP address!");

        HttpResponse response = configurationUpdater.callWithMapping(roleMappingsInvalidIpAddress, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldReportConfigurationErrorWhenMappingContainInvalidHostPattern() throws Exception {
        RoleMapping roleMappingsInvalidHostPattern =
            new RoleMapping(ROLE_USED_WITH_INCORRECT_MAPPING.getName()).hosts("/(.+search-guard.com/");

        HttpResponse response = configurationUpdater.callWithMapping(roleMappingsInvalidHostPattern, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        });

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldReportMultipleErrorsWhenConfigurationIsInvalid() throws Exception {
        RoleMapping roleMappingsInvalidHostPattern =
            new RoleMapping(ROLE_USED_WITH_INCORRECT_MAPPING.getName()).hosts("/(.+search-guard.com/");
        Role roleWithInvalidPermission = new Role("invalid-role-permission").clusterPermissions("*")//
            .indexPermissions("/permission-(.+/").fls("~secret").on("index");

        HttpResponse response = configurationUpdater.callWithMapping(roleMappingsInvalidHostPattern, unused -> configurationUpdater
            .callWithRole(roleWithInvalidPermission, client -> {
            assertThatUserCanAuthenticate(client);
            return searchAll(client);
        }));

        assertThatSearchIsRejectedDueToIncorrectConfiguration(response);
    }

    @Test
    public void shouldRestoreAccessWhenConfigurationIsCorrected() throws Exception {
        Role roleInvalidIndexPattern = new Role("invalid-role-index").clusterPermissions("*")//
            .indexPermissions("*").fls("~secret").on("/index-(.+/");

        HttpResponse response = configurationUpdater.callWithRole(roleInvalidIndexPattern,
            InvalidRolesAndMappingConfigurationTest::searchAll);

        assertThat(response.getStatusCode(), equalTo(500));

        try (GenericRestClient client = CLUSTER.getRestClient(USER_ADMIN)) {
            response = searchAll(client);
            assertThat(response.getStatusCode(), equalTo(200));
        }
    }

    private static void assertThatSearchIsRejectedDueToIncorrectConfiguration(HttpResponse response) throws Exception {
        log.info("Search response body '{}'", response.getBody());
        assertThat(response.getStatusCode(), equalTo(500));
        DocNode body = response.getBodyAsDocNode();
        assertThat(body, containsValue("$.error.type", ERROR_TYPE));
        assertThat(body, containSubstring("$.error.reason",//
            "Incorrect configuration of SearchGuard roles or roles mapping, please check the log file for more details."));
    }

    private static void assertThatUserCanAuthenticate(GenericRestClient client) throws Exception {
        HttpResponse response = client.get("/_searchguard/authinfo");
        assertThat(response.getStatusCode(), equalTo(200));
        response = client.get("/_searchguard/health");
        assertThat(response.getStatusCode(), equalTo(200));
        log.info("Health response body '{}'", response.getBody());
    }

    private static HttpResponse searchAll(GenericRestClient client) throws Exception {
        return client.get("/*/_search");
    }
}
