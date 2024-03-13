/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.authtoken;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import com.floragunn.codova.config.net.CacheConfig;
import org.apache.cxf.rs.security.jose.jwt.JwtConstants;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenResponse;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.StaticSettings;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.io.DeserializationException;
import io.jsonwebtoken.io.Deserializer;

public class AuthTokenServiceTest {

    private static ConfigurationRepository configurationRepository;
    private static PrivilegesEvaluator privilegesEvaluator;
    private static AuthorizationService authorizationService;
    private static ProtectedConfigIndexService protectedConfigIndexService;
    private static ThreadPool threadPool;
    private static PrivilegedConfigClient privilegedConfigClient;
    private static StaticSgConfig staticSgConfig;
    private static ClusterService clusterService;
    private static Deserializer<Map<String, ?>> jsonDeserializer = bytes -> {
        try {
            return DocReader.json().readObject(bytes);
        } catch (DocumentParseException | UnexpectedDocumentStructureException e) {
            throw new DeserializationException(e.getMessage(), e);
        }
    };
    
    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().resources("authtoken").singleNode().enterpriseModulesEnabled().sslEnabled()
            .disableModule(AuthTokenModule.class).embedded().build();

    @BeforeClass
    public static void setupDependencies() {
        configurationRepository = cluster.getInjectable(ConfigurationRepository.class);
        authorizationService = cluster.getInjectable(AuthorizationService.class);
        privilegesEvaluator = cluster.getInjectable(PrivilegesEvaluator.class);
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
        Actions actions = new Actions(null);

        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);
        config.setJwtAud("_test_aud");
        config.setMaxTokensPerUser(100);
        config.setCacheConfig(CacheConfig.DEFAULT);

        ConfigHistoryService configHistoryService = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                protectedConfigIndexService, actions, StaticSettings.EMPTY, privilegesEvaluator);
        AuthTokenService authTokenService = new AuthTokenService(privilegedConfigClient, authorizationService, privilegesEvaluator, configHistoryService, StaticSettings.EMPTY,
                threadPool, clusterService, protectedConfigIndexService, actions, config);
        try {
            authTokenService.setSendTokenUpdates(false);
            authTokenService.waitForInitComplete(10000);

            RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test\nroles:\n- r1\n- r0");
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);

            CreateAuthTokenResponse response = authTokenService.createJwt(testUser, request);

            JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K)).deserializeJsonWith(jsonDeserializer);
            
            Claims claims = jwtParser.parseClaimsJws(response.getJwt()).getBody();

            Assert.assertEquals(testUser.getName(), claims.getSubject());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));
            Assert.assertEquals(Collections.singletonList("r1"), ((Map<?, ?>) claims.get("base")).get("r_be"));
            Assert.assertEquals(config.getJwtAud(), claims.getAudience());

            AuthToken authToken = authTokenService.getByClaims(claims);

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
        Actions actions = new Actions(null);

        AuthTokenServiceConfig config = new AuthTokenServiceConfig();
        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);
        config.setJwtAud("_test_aud");
        config.setMaxTokensPerUser(100);
        config.setCacheConfig(CacheConfig.DEFAULT);

        ConfigHistoryService configHistoryService = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                protectedConfigIndexService, actions, StaticSettings.EMPTY, privilegesEvaluator);
        AuthTokenService authTokenService = new AuthTokenService(privilegedConfigClient, authorizationService, privilegesEvaluator, configHistoryService, StaticSettings.EMPTY,
                threadPool, clusterService, protectedConfigIndexService, actions, config);

        try {
            authTokenService.setSendTokenUpdates(false);
            authTokenService.waitForInitComplete(10000);

            RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test\nroles:\n- r1\n- r0");
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);

            CreateAuthTokenResponse response = authTokenService.createJwt(testUser, request);

            JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K)).deserializeJsonWith(jsonDeserializer);

            Claims claims = jwtParser.parseClaimsJws(response.getJwt()).getBody();

            Assert.assertEquals(testUser.getName(), claims.getSubject());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));
            Assert.assertEquals(Collections.singletonList("r1"), ((Map<?, ?>) claims.get("base")).get("r_be"));
            Assert.assertEquals(config.getJwtAud(), claims.getAudience());

            AuthToken authToken = authTokenService.getByClaims(claims);

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
        Actions actions = new Actions(null);

        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);
        config.setJwtAud("_test_aud");
        config.setMaxTokensPerUser(100);
        config.setCacheConfig(CacheConfig.DEFAULT);

        ConfigHistoryService configHistoryService = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                protectedConfigIndexService, actions, StaticSettings.EMPTY, privilegesEvaluator);
        AuthTokenService authTokenService = new AuthTokenService(privilegedConfigClient,  authorizationService, privilegesEvaluator, configHistoryService, StaticSettings.EMPTY,
                threadPool, clusterService, protectedConfigIndexService, actions, config);

        try {
            authTokenService.setSendTokenUpdates(false);
            authTokenService.waitForInitComplete(20000);

            RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test\nroles:\n- r1\n- r0");
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);
            CreateAuthTokenResponse response = authTokenService.createJwt(testUser, request);

            JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K)).deserializeJsonWith(jsonDeserializer);

            Claims claims = jwtParser.parseClaimsJws(response.getJwt()).getBody();

            Assert.assertEquals(testUser.getName(), claims.getSubject());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));
            Assert.assertEquals(Collections.singletonList("r1"), ((Map<?, ?>) claims.get("base")).get("r_be"));
            Assert.assertEquals(config.getJwtAud(), claims.getAudience());

            AuthToken authToken = authTokenService.getByClaims(claims);

            Assert.assertEquals(testUser.getName(), authToken.getUserName());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), authToken.getRequestedPrivileges().getClusterPermissions());
            Assert.assertEquals(Collections.singletonList("r1"), authToken.getBase().getBackendRoles());
            authTokenService.shutdown();

            ConfigHistoryService configHistoryService2 = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                    protectedConfigIndexService, actions, StaticSettings.EMPTY, privilegesEvaluator);

            AuthTokenService authTokenService2 = new AuthTokenService(privilegedConfigClient,  authorizationService, privilegesEvaluator, configHistoryService2,
                    StaticSettings.EMPTY, threadPool, clusterService, protectedConfigIndexService, actions, config);
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
        Actions actions = new Actions(null);

        config.setEnabled(true);
        config.setJwtSigningKey(TestJwk.OCT_1);
        config.setJwtAud("_test_aud");
        config.setMaxTokensPerUser(100);
        config.setCacheConfig(CacheConfig.DEFAULT);

        Settings authTokenServiceSettings = Settings.builder().put(AuthTokenService.CLEANUP_INTERVAL.name(), TimeValue.timeValueSeconds(1)).build();

        ConfigHistoryService configHistoryService = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                protectedConfigIndexService, actions, StaticSettings.EMPTY, privilegesEvaluator);
        AuthTokenService authTokenService = new AuthTokenService(privilegedConfigClient, authorizationService, privilegesEvaluator, configHistoryService,
               new StaticSettings(authTokenServiceSettings, null), threadPool, clusterService, protectedConfigIndexService, actions, config);
        try {
            authTokenService.setSendTokenUpdates(false);
            authTokenService.waitForInitComplete(10000);

            RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test\nroles:\n- r1\n- r0");
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);
            request.setExpiresAfter(Duration.ofSeconds(5));

            CreateAuthTokenResponse response = authTokenService.createJwt(testUser, request);

            JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K)).deserializeJsonWith(jsonDeserializer);

            Claims claims = jwtParser.parseClaimsJws(response.getJwt()).getBody();
            String id = claims.get(JwtConstants.CLAIM_JWT_ID).toString();

            Assert.assertEquals(testUser.getName(), claims.getSubject());
            Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));
            Assert.assertEquals(Collections.singletonList("r1"), ((Map<?, ?>) claims.get("base")).get("r_be"));
            Assert.assertEquals(config.getJwtAud(), claims.getAudience());
            Assert.assertTrue(
                    response.getAuthToken().getCreationTime().plusSeconds(11) + " <= " + claims.getExpiration().getTime() + "\n" + claims.toString(),
                    response.getAuthToken().getCreationTime().plusSeconds(11).toEpochMilli() > claims.getExpiration().getTime());

            AuthToken authToken = authTokenService.getByIdFromIndex(id);

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

    @Test
    public void authTokenBasedOnAuthTokenTest() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {

            User testUser = User.forUser("test_user").backendRoles("r1", "r2", "r3").build();
            AuthTokenServiceConfig config = new AuthTokenServiceConfig();
            Actions actions = new Actions(null);

            config.setEnabled(true);
            config.setJwtSigningKey(TestJwk.OCT_1);
            config.setJwtAud("_test_aud");
            config.setMaxTokensPerUser(100);
            config.setExcludeClusterPermissions(Collections.emptyList());
            config.setCacheConfig(CacheConfig.DEFAULT);

            ConfigHistoryService configHistoryService = new ConfigHistoryService(configurationRepository, staticSgConfig, privilegedConfigClient,
                    protectedConfigIndexService, actions, StaticSettings.EMPTY, privilegesEvaluator);
            AuthTokenService authTokenService = new AuthTokenService(privilegedConfigClient, authorizationService, privilegesEvaluator, configHistoryService,
                    StaticSettings.EMPTY, threadPool, clusterService, protectedConfigIndexService, actions, config);
            try {
                authTokenService.setSendTokenUpdates(false);
                authTokenService.waitForInitComplete(10000);

                RequestedPrivileges requestedPrivileges = RequestedPrivileges.parseYaml("cluster_permissions:\n- cluster:test");
                CreateAuthTokenRequest request = new CreateAuthTokenRequest(requestedPrivileges);

                CreateAuthTokenResponse createAuthTokenResponse = authTokenService.createJwt(testUser, request);

                JwtParser jwtParser = Jwts.parser().setSigningKey(Decoders.BASE64URL.decode(TestJwk.OCT_1_K)).deserializeJsonWith(jsonDeserializer);

                Claims claims = jwtParser.parseClaimsJws(createAuthTokenResponse.getJwt()).getBody();

                Assert.assertEquals(testUser.getName(), claims.getSubject());
                Assert.assertEquals(requestedPrivileges.getClusterPermissions(), ((Map<?, ?>) claims.get("requested")).get("cluster_permissions"));

                AuthToken baseAuthToken = authTokenService.getByClaims(claims);

                Assert.assertEquals(testUser.getName(), baseAuthToken.getUserName());
                Assert.assertEquals(requestedPrivileges.getClusterPermissions(), baseAuthToken.getRequestedPrivileges().getClusterPermissions());

                HttpResponse roleUpdateResponse = restClient.putJson("/_searchguard/api/roles/new_test_role", "{\"cluster_permissions\": [\"*\"]}");
                Assert.assertEquals(roleUpdateResponse.getBody(), 201, roleUpdateResponse.getStatusCode());

                Thread.sleep(500);

                User authTokenTestUser = User.forUser(testUser.getName()).backendRoles("r1", "r2", "r3").type(AuthTokenService.USER_TYPE)
                        .specialAuthzConfig(baseAuthToken.getId()).build();

                request.setTokenName("auth_token_based_on_auth_token");

                createAuthTokenResponse = authTokenService.createJwt(authTokenTestUser, request);

                claims = jwtParser.parseClaimsJws(createAuthTokenResponse.getJwt()).getBody();

                AuthToken authTokenBasedOnAuthToken = authTokenService.getByClaims(claims);
                Assert.assertEquals(baseAuthToken.getBase(), authTokenBasedOnAuthToken.getBase());

                request.setTokenName("auth_token_with_fresh_base");
                createAuthTokenResponse = authTokenService.createJwt(testUser, request);
                claims = jwtParser.parseClaimsJws(createAuthTokenResponse.getJwt()).getBody();

                AuthToken authTokenWithFreshBase = authTokenService.getByClaims(claims);
                Assert.assertNotEquals(baseAuthToken.getBase(), authTokenWithFreshBase.getBase());

            } finally {
                authTokenService.shutdown();
            }
        }
    }

}
