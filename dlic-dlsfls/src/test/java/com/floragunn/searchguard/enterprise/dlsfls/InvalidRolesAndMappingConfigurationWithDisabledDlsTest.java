package com.floragunn.searchguard.enterprise.dlsfls;

import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain;
import com.floragunn.searchguard.test.TestSgConfig.DlsFls;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.ConfigurationUpdater;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class InvalidRolesAndMappingConfigurationWithDisabledDlsTest {

    private static final Logger log = LogManager.getLogger(InvalidRolesAndMappingConfigurationWithDisabledDlsTest.class);

    private static final Authc AUTHC = new Authc(new Domain("basic/internal_users_db"));

    //modern implementation is disabled by using .useImpl("legacy") so no DLS impl is available
    private static final DlsFls DLSFLS = new DlsFls().useImpl("legacy").metrics("detailed");

    private static final User USER_ADMIN = new User("admin").roles(ALL_ACCESS.getName());
    public static final String LIMITED_ROLE_NAME = "limited-role";
    private static final User USER_LIMITED = new User("limited-user").roles(LIMITED_ROLE_NAME);

    private static final String ERROR_TYPE = "dls_fls_configuration_exception";

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
    public void shouldReportConfigurationErrorWhenRoleContainsInvalidIndexPattern() throws Exception {
        Role roleInvalidIndexPattern = new Role("invalid-role-index").clusterPermissions("*")//
            .indexPermissions("*").fls("~secret").on("/index-(.+/");

        HttpResponse response = configurationUpdater.callWithRole(roleInvalidIndexPattern, client -> client.get("/*/_search"));

        //DLS is disabled here so that configuration validation is not performed
        assertThat(response.getStatusCode(), equalTo(200));
    }
}
