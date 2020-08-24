package com.floragunn.searchguard.authtoken;

import java.util.Collections;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenResponse;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.user.User;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;

public class AuthTokenServiceTest {

    private static AuthTokenService authTokenService;
    private static ConfigHistoryService configHistoryService;
    private static ThreadPool threadPool;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().build();

    @BeforeClass
    public static void setupTestData() {
        /*
        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy"))
                    .actionGet();
        }
        */
    }

    @BeforeClass
    public static void setupDependencies() {
        authTokenService = cluster.getInjectable(AuthTokenService.class);
        configHistoryService = cluster.getInjectable(ConfigHistoryService.class);
        threadPool = cluster.getInjectable(ThreadPool.class);
    }

    @Test
    public void basicTest() throws Exception {
        User testUser = User.forUser("test_user").backendRoles("r1", "r2", "r3").build();
        AuthTokenServiceConfig config = new AuthTokenServiceConfig();
        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);
        config.setJwtAud("_test_aud");

        AuthTokenService authTokenService = new AuthTokenService(PrivilegedConfigClient.adapt(cluster.node().client()), configHistoryService,
                Settings.EMPTY, threadPool, config);

        RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test\nroles:\n- r1\n- r0");
        CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);

        CreateAuthTokenResponse response = authTokenService.createJwt(testUser, request);

        JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K));

        Claims claims = jwtParser.parseClaimsJws(response.getJwt()).getBody();

        System.out.println(claims);

        Assert.assertEquals(testUser.getName(), claims.getSubject());
        Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));
        Assert.assertEquals(Collections.singletonList("r1"), ((Map<?, ?>) claims.get("base")).get("roles_be"));
        Assert.assertEquals(config.getJwtAud(), claims.getAudience());

        AuthToken authToken = authTokenService.getByClaims(claims);

        System.out.println(authToken);

        Assert.assertEquals(testUser.getName(), authToken.getUserName());
        Assert.assertEquals(requestedPrivileges.getClusterPermissions(), authToken.getRequestedPrivilges().getClusterPermissions());
        Assert.assertEquals(Collections.singletonList("r1"), authToken.getBase().getBackendRoles());

    }
}
