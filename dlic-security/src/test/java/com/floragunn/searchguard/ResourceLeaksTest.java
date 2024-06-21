package com.floragunn.searchguard;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authc.AuthenticationBackend;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import org.apache.http.HttpStatus;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class ResourceLeaksTest {

    //https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/208
    //https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/merge_requests/470

    private static final TestSgConfig.Authc.Domain MOCK_DOMAIN = new TestSgConfig.Authc.Domain("basic/" + MockAuthBackend.TYPE);
    private static final TestSgConfig.Authc.Domain BASIC_INT_USER_DB_DOMAIN = new TestSgConfig.Authc.Domain("basic/internal_users_db");

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode()
            .authc(new TestSgConfig.Authc(BASIC_INT_USER_DB_DOMAIN))
            .sslEnabled()
            .enterpriseModulesEnabled()
            .embedded().build();

    @BeforeClass
    public static void addMockAuthToTypedComponentRegistry() {
        SearchGuardModulesRegistry searchGuardModulesRegistry = cluster.getInjectable(SearchGuardModulesRegistry.class);
        assertThat("SearchGuardModulesRegistry is not null", searchGuardModulesRegistry, notNullValue());
        TypedComponentRegistry typedComponentRegistry = searchGuardModulesRegistry.getTypedComponentRegistry();
        assertThat("TypedComponentRegistry is not null", typedComponentRegistry, notNullValue());

        typedComponentRegistry.register(MockAuthBackend.INFO);
    }

    @Before
    public void setUpMockAuthDomain() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = restClient.putJson(
                    "/_searchguard/config/authc", DocNode.of("auth_domains", DocNode.array(MOCK_DOMAIN))
            );
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        }
    }

    @Test
    public void getAndUpdateConfig_shouldNotCauseResourceLeaks() throws Exception {
       try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {

           //get
           GenericRestClient.HttpResponse response = restClient.get("/_searchguard/config/authc");
           assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
           assertThat(
                   response.getBody(),
                   response.getBodyAsDocNode().findByJsonPath("content.auth_domains[*].type"),
                   contains(MOCK_DOMAIN.toDocNode().getAsString("type"))
           );

           //put
           DocNode mockAuthDomainConfig = DocNode.of("type", "basic/" + MockAuthBackend.TYPE);
           DocNode authDomainsConfig = DocNode.of("auth_domains", DocNode.array(BASIC_INT_USER_DB_DOMAIN, mockAuthDomainConfig));


           response = restClient.putJson("/_searchguard/config/authc", authDomainsConfig);
           assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));

           //patch
           authDomainsConfig = DocNode.of("auth_domains", DocNode.array(BASIC_INT_USER_DB_DOMAIN, mockAuthDomainConfig.with("test", "test")));

           response = restClient.patchJsonMerge("/_searchguard/config/authc", authDomainsConfig);
           assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));

           Awaitility.await("Number of config instances doesn't increase after getting, putting and patching config")
                   .atMost(Duration.ofSeconds(20))
                   .untilAsserted(() -> {
                       assertThat("Number of MockAuthBackend instances", MockAuthBackend.NUMBER_OF_INSTANCES.get(), equalTo(1));
                   });
       }
    }

    @Test
    public void purgeCache_shouldNotCauseResourceLeaks() throws Exception {
       try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {

           //cache
           GenericRestClient.HttpResponse response = restClient.delete("/_searchguard/api/cache");
           assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));

           Awaitility.await("Number of config instances doesn't increase after purging cache")
                   .atMost(Duration.ofSeconds(20))
                   .untilAsserted(() -> {
                       assertThat("Number of MockAuthBackend instances", MockAuthBackend.NUMBER_OF_INSTANCES.get(), equalTo(1));
                   });
       }
    }

    private static class MockAuthBackend implements AuthenticationBackend, AutoCloseable {

        private static final String TYPE = "mock";
        private static final ComponentState COMPONENT_STATE = new ComponentState(0, "authentication_backend", TYPE, MockAuthBackend.class).initialized();
        private static final AtomicInteger NUMBER_OF_INSTANCES = new AtomicInteger(0);

        public MockAuthBackend(Map<String, Object> config, ConfigurationRepository.Context context) throws ConfigValidationException {
            if (context.isExternalResourceCreationEnabled()) {
                NUMBER_OF_INSTANCES.incrementAndGet();
            }
        }

        @Override
        public CompletableFuture<AuthCredentials> authenticate(AuthCredentials authCredentials, Meter meter) throws AuthenticatorUnavailableException, CredentialsException {
            return null;
        }

        @Override
        public void close() {
            NUMBER_OF_INSTANCES.decrementAndGet();
        }

        @Override
        public ComponentState getComponentState() {
            return COMPONENT_STATE;
        }

        @Override
        public String getType() {
            return TYPE;
        }

        private static final TypedComponent.Info<AuthenticationBackend> INFO = new TypedComponent.Info<AuthenticationBackend>() {

            @Override
            public Class<AuthenticationBackend> getType() {
                return AuthenticationBackend.class;
            }

            @Override
            public String getName() {
                return TYPE;
            }

            @Override
            public TypedComponent.Factory<AuthenticationBackend> getFactory() {
                return MockAuthBackend::new;
            }
        };
    }

}
