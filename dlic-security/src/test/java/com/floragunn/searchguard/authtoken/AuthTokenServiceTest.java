package com.floragunn.searchguard.authtoken;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.user.User;

public class AuthTokenServiceTest {

    private static AuthTokenService authTokenService;
    private static ConfigHistoryService configHistoryService;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy"))
                    .actionGet();
        }
    }

    @BeforeClass
    public static void setupDependencies() {
        authTokenService = cluster.getInjectable(AuthTokenService.class);
        configHistoryService = cluster.getInjectable(ConfigHistoryService.class);
    }

    @Test
    public void testWebhookAction() throws Exception {
        AuthTokenServiceConfig config = new AuthTokenServiceConfig();
        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);

        AuthTokenService authTokenService = new AuthTokenService(PrivilegedConfigClient.adapt(cluster.getInternalClient()), configHistoryService,
                Settings.EMPTY, config);

        RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions: - cluster:test");
        CreateAuthTokenRequest request = new CreateAuthTokenRequest();

        AuthToken authToken = authTokenService.create(User.forUser("test").backendRoles("r1", "r2", "3").build(), request);

        System.out.println(authToken);
    }
}
