package com.floragunn.searchguard.modules;

import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;

public class ComponentStateIntegrationTest {

    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().user(ADMIN_USER).build();

    @Test
    public void basicTest() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {

            HttpResponse response = client.get("/_searchguard/component/_all/_health");

            System.out.println(response.getBody());
        }
    }
}
