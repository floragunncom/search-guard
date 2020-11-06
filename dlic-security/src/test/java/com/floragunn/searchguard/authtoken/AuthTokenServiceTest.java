package com.floragunn.searchguard.authtoken;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.apache.cxf.rs.security.jose.jwt.JwtConstants;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenResponse;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.StaticSgConfig;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.user.User;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;

public class AuthTokenServiceTest {

    private static ConfigurationRepository configurationRepository;
    private static DynamicConfigFactory dynamicConfigFactory;
    private static ProtectedConfigIndexService protectedConfigIndexService;
    private static ThreadPool threadPool;
    private static PrivilegedConfigClient privilegedConfigClient;
    private static StaticSgConfig staticSgConfig;
    private static ClusterService clusterService;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().resources("authtoken").singleNode().sslEnabled()
            .disableModule(AuthTokenModule.class).build();

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
        configurationRepository = cluster.getInjectable(ConfigurationRepository.class);
        dynamicConfigFactory = cluster.getInjectable(DynamicConfigFactory.class);
        protectedConfigIndexService = cluster.getInjectable(ProtectedConfigIndexService.class);
        clusterService = cluster.getInjectable(ClusterService.class);
        threadPool = cluster.getInjectable(ThreadPool.class);
        staticSgConfig = cluster.getInjectable(StaticSgConfig.class);
        privilegedConfigClient = PrivilegedConfigClient.adapt(cluster.node().client());
    }

    @Test
    public void basicTest() throws Exception {
        User testUser = User.forUser("test_user").backendRoles("r1", "r2", "r3").build();
        AuthTokenServiceConfig config = new AuthTokenServiceConfig();

        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);
        config.setJwtAud("_test_aud");

        ConfigHistoryService configHistoryService = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                protectedConfigIndexService, dynamicConfigFactory, Settings.EMPTY);
        AuthTokenService authTokenService = new AuthTokenService(privilegedConfigClient, configHistoryService, Settings.EMPTY, threadPool,
                clusterService, protectedConfigIndexService, config);
        try {
            authTokenService.setSendTokenUpdates(false);
            authTokenService.waitForInitComplete(10000);

            RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test\nroles:\n- r1\n- r0");
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);

            CreateAuthTokenResponse response = authTokenService.createJwt(testUser, request);

            System.out.println(response.getJwt());

            JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K));

            Claims claims = jwtParser.parseClaimsJws(response.getJwt()).getBody();

            System.out.println(claims);

            Assert.assertEquals(testUser.getName(), claims.getSubject());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));
            Assert.assertEquals(Collections.singletonList("r1"), ((Map<?, ?>) claims.get("base")).get("r_be"));
            Assert.assertEquals(config.getJwtAud(), claims.getAudience());

            AuthToken authToken = authTokenService.getByClaims(claims);

            System.out.println(authToken);

            Assert.assertEquals(testUser.getName(), authToken.getUserName());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), authToken.getRequestedPrivileges().getClusterPermissions());
            Assert.assertEquals(Collections.singletonList("r1"), authToken.getBase().getBackendRoles());

        } finally {
            authTokenService.shutdown();
        }
    }

    @Test
    public void reloadFromCacheTest() throws Exception {
        User testUser = User.forUser("test_user").backendRoles("r1", "r2", "r3").build();
        AuthTokenServiceConfig config = new AuthTokenServiceConfig();
        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);
        config.setJwtAud("_test_aud");

        ConfigHistoryService configHistoryService = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                protectedConfigIndexService, dynamicConfigFactory, Settings.EMPTY);
        AuthTokenService authTokenService = new AuthTokenService(privilegedConfigClient, configHistoryService, Settings.EMPTY, threadPool,
                clusterService, protectedConfigIndexService, config);

        try {
            authTokenService.setSendTokenUpdates(false);
            authTokenService.waitForInitComplete(10000);

            RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test\nroles:\n- r1\n- r0");
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);

            CreateAuthTokenResponse response = authTokenService.createJwt(testUser, request);

            JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K));

            Claims claims = jwtParser.parseClaimsJws(response.getJwt()).getBody();

            System.out.println(claims);

            Assert.assertEquals(testUser.getName(), claims.getSubject());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));
            Assert.assertEquals(Collections.singletonList("r1"), ((Map<?, ?>) claims.get("base")).get("r_be"));
            Assert.assertEquals(config.getJwtAud(), claims.getAudience());

            AuthToken authToken = authTokenService.getByClaims(claims);

            System.out.println(authToken);

            Assert.assertEquals(testUser.getName(), authToken.getUserName());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), authToken.getRequestedPrivileges().getClusterPermissions());
            Assert.assertEquals(Collections.singletonList("r1"), authToken.getBase().getBackendRoles());

        } finally {
            authTokenService.shutdown();
        }
    }

    @Test
    public void reloadFromIndexTest() throws Exception {
        User testUser = User.forUser("test_user").backendRoles("r1", "r2", "r3").build();
        AuthTokenServiceConfig config = new AuthTokenServiceConfig();
        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);
        config.setJwtAud("_test_aud");

        ConfigHistoryService configHistoryService = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                protectedConfigIndexService, dynamicConfigFactory, Settings.EMPTY);
        AuthTokenService authTokenService = new AuthTokenService(privilegedConfigClient, configHistoryService, Settings.EMPTY, threadPool,
                clusterService, protectedConfigIndexService, config);

        try {
            authTokenService.setSendTokenUpdates(false);
            authTokenService.waitForInitComplete(20000);

            RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test\nroles:\n- r1\n- r0");
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);
            CreateAuthTokenResponse response = authTokenService.createJwt(testUser, request);

            JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K));

            Claims claims = jwtParser.parseClaimsJws(response.getJwt()).getBody();

            System.out.println(claims);

            Assert.assertEquals(testUser.getName(), claims.getSubject());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));
            Assert.assertEquals(Collections.singletonList("r1"), ((Map<?, ?>) claims.get("base")).get("r_be"));
            Assert.assertEquals(config.getJwtAud(), claims.getAudience());

            AuthToken authToken = authTokenService.getByClaims(claims);

            System.out.println(authToken);

            Assert.assertEquals(testUser.getName(), authToken.getUserName());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), authToken.getRequestedPrivileges().getClusterPermissions());
            Assert.assertEquals(Collections.singletonList("r1"), authToken.getBase().getBackendRoles());
            authTokenService.shutdown();

            ConfigHistoryService configHistoryService2 = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                    protectedConfigIndexService, dynamicConfigFactory, Settings.EMPTY);
            
            AuthTokenService authTokenService2 = new AuthTokenService(privilegedConfigClient, configHistoryService2, Settings.EMPTY, threadPool,
                    clusterService, protectedConfigIndexService, config);
            authTokenService2.setSendTokenUpdates(false);
            authTokenService2.waitForInitComplete(20000);

            AuthToken authToken2 = authTokenService2.getByClaims(claims);

            Assert.assertEquals(authToken.getUserName(), authToken2.getUserName());
            Assert.assertEquals(authToken.getRequestedPrivileges().getClusterPermissions(),
                    authToken2.getRequestedPrivileges().getClusterPermissions());
            Assert.assertEquals(authToken.getBase().getBackendRoles(), authToken2.getBase().getBackendRoles());
        } finally {
            authTokenService.shutdown();
        }
    }

    @Test
    public void expiryTest() throws Exception {
        User testUser = User.forUser("test_user").backendRoles("r1", "r2", "r3").build();
        AuthTokenServiceConfig config = new AuthTokenServiceConfig();

        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);
        config.setJwtAud("_test_aud");

        Settings authTokenServiceSettings = Settings.builder().put(AuthTokenService.CLEANUP_INTERVAL.getKey(), TimeValue.timeValueSeconds(1)).build();

        ConfigHistoryService configHistoryService = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                protectedConfigIndexService, dynamicConfigFactory, Settings.EMPTY);
        AuthTokenService authTokenService = new AuthTokenService(privilegedConfigClient, configHistoryService, authTokenServiceSettings, threadPool,
                clusterService, protectedConfigIndexService, config);
        try {
            authTokenService.setSendTokenUpdates(false);
            authTokenService.waitForInitComplete(10000);

            RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test\nroles:\n- r1\n- r0");
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);
            request.setExpiresAfter(Duration.ofSeconds(5));

            CreateAuthTokenResponse response = authTokenService.createJwt(testUser, request);

            System.out.println(response.getJwt());

            JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K));

            Claims claims = jwtParser.parseClaimsJws(response.getJwt()).getBody();
            String id = claims.get(JwtConstants.CLAIM_JWT_ID).toString();

            System.out.println(claims);

            Assert.assertEquals(testUser.getName(), claims.getSubject());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));
            Assert.assertEquals(Collections.singletonList("r1"), ((Map<?, ?>) claims.get("base")).get("r_be"));
            Assert.assertEquals(config.getJwtAud(), claims.getAudience());
            Assert.assertTrue(
                    response.getAuthToken().getCreationTime().plusSeconds(11) + " <= " + claims.getExpiration().getTime() + "\n" + claims.toString(),
                    response.getAuthToken().getCreationTime().plusSeconds(11).toEpochMilli() > claims.getExpiration().getTime());

            AuthToken authToken = authTokenService.getByIdFromIndex(id);

            System.out.println(authToken);

            Assert.assertEquals(testUser.getName(), authToken.getUserName());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), authToken.getRequestedPrivileges().getClusterPermissions());
            Assert.assertEquals(Collections.singletonList("r1"), authToken.getBase().getBackendRoles());

            Thread.sleep(10000);

            try {
                authToken = authTokenService.getByIdFromIndex(id);

                Assert.fail(authToken + "");
            } catch (NoSuchAuthTokenException e) {
                // Expected
            }

        } finally {
            authTokenService.shutdown();
        }
    }

}
