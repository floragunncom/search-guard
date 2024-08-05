package com.floragunn.searchguard.legacy;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class Double401ResponsesReproductionIntTest {

    private static final Logger log = LogManager.getLogger(Double401ResponsesReproductionIntTest.class);

    private static TestSgConfig.User USER = new TestSgConfig.User("user").roles(new TestSgConfig.Role("role").indexPermissions("*").on("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .singleNode()
            .sslEnabled()
            .resources("doubleUnauthorized")
            .users(USER)
            .build();


    @Test
    @Ignore("This test is for manual execution only, to reproduce bug related to double 401 responses")
    public void reproduceDouble401ResponsesBug() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(new BasicHeader("Authorization", "Bearer blabla"))) {
            GenericRestClient.HttpResponse response = client.get("_searchguard/authinfo");
            log.debug("Actual response is '{}'", response.getBody());
        }
    }
}
