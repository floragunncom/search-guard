package com.floragunn.searchguard.legacy;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.ClassRule;
import org.junit.Test;

import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CrossClusterSearchWithoutRemoteClusterSetupTest {

    public static final User USER_ADMIN = new User("admin").roles(ALL_ACCESS);
    public static final User USER_LIMITED = new User("limited")
        .roles(new TestSgConfig.Role("limited").clusterPermissions("*").indexPermissions("*").on("my-index"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().authc(new Authc(new Domain("basic/internal_users_db")))
        .users(USER_ADMIN, USER_LIMITED)
        .sslEnabled().enterpriseModulesEnabled().build();

    @Test
    public void shouldPerformCrossClusterSearchForNonExistingIndex() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/*:.monitoring-es/_search");

            assertThat(response.getStatusCode(), equalTo(200));
        }
    }

    @Test
    public void shouldPerformCrossClusterSearchForNonExistingIndexWhenLackingPermissionForTheIndex() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_LIMITED)) {
            GenericRestClient.HttpResponse response = client.get("/*:.monitoring-es/_search");

            assertThat(response.getStatusCode(), equalTo(200));
        }
    }
}
