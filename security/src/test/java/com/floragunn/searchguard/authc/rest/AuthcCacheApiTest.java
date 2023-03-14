package com.floragunn.searchguard.authc.rest;

import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class AuthcCacheApiTest {
    private final static TestSgConfig.User[] USERS = new TestSgConfig.User[5];
    static {
        for (int i = 0; i < USERS.length; i++) {
            USERS[i] = new TestSgConfig.User("admin_" + i).password("pw")
                    .roles(new TestSgConfig.Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));
        }
    }

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().users(USERS).build();

    @Test
    public void testClearCache() throws Exception {
        for (int i = 0; i < USERS.length; i++) {
            GenericRestClient client = cluster.getRestClient(USERS[i].getName(), USERS[i].getPassword());
            GenericRestClient.HttpResponse response = client.get("/_searchguard/component/rest_filter/_health");
            Assert.assertEquals(200, response.getStatusCode());
            Assert.assertEquals(i + 1, getUserCacheSizeFromResponse(response));
        }

        GenericRestClient client = cluster.getRestClient(USERS[0].getName(), USERS[0].getPassword());
        GenericRestClient.HttpResponse response = client.delete("/_searchguard/authc/cache");
        Assert.assertEquals(200, response.getStatusCode());

        response = client.get("/_searchguard/component/rest_filter/_health");
        Assert.assertEquals(200, response.getStatusCode());
        Assert.assertEquals(1, getUserCacheSizeFromResponse(response));
    }

    private int getUserCacheSizeFromResponse(GenericRestClient.HttpResponse response) throws DocumentParseException, Format.UnknownDocTypeException, NullPointerException {
        AtomicInteger size = new AtomicInteger();
        response.getBodyAsDocNode().getAsListOfNodes("components").get(0).getAsListOfNodes("parts").forEach(
                docNode -> size.addAndGet((int) docNode.getAsListOfNodes("parts")
                        .stream().filter(docNode1 -> docNode1.get("name").equals("rest_authentication_processor")).findFirst().orElse(null)
                        .getAsNode("metrics")
                        .getAsNode("user_cache")
                        .getAsNode("cache")
                        .get("current_size")));
        return size.get();
    }
}