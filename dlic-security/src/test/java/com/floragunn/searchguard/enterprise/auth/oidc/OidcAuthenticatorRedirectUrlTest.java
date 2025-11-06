package com.floragunn.searchguard.enterprise.auth.oidc;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.VariableResolvers;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig.AuthMethod;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction.Request;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ConfigurationRepository.Context;
import com.google.common.collect.ImmutableMap;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class OidcAuthenticatorRedirectUrlTest {

    private static final Logger log = LogManager.getLogger(OidcAuthenticatorRedirectUrlTest.class);

    private static final AuthMethod OIDC_AUTH_METHOD = new AuthMethod("oidc", "OIDC", null);
    private OidcAuthenticator authenticator;

    private static MockIpdServer mockIdpServer;

    private final String frontendBaseUrl;
    private final String expectedOidcRedirectUrl;

    @Parameterized.Parameters
    public static Collection<Object[]> testParameters() {
        return Arrays.asList(new Object[][] {
            { "https://search.frontend.com", "https://search.frontend.com/auth/openid/login" },
            { "https://search.frontend.com/", "https://search.frontend.com/auth/openid/login" },
            { "https://search.com/frontend", "https://search.com/frontend/auth/openid/login" },
            { "https://search.com/frontend/", "https://search.com/frontend/auth/openid/login" },
            { "https://search.com/evem/more/hidden/frontend/", "https://search.com/evem/more/hidden/frontend/auth/openid/login" },
            { "https://search.com/evem/more/hidden/frontend", "https://search.com/evem/more/hidden/frontend/auth/openid/login" }
        });
    }

    public OidcAuthenticatorRedirectUrlTest(String frontendBaseUrl, String expectedOidcRedirectUrl) {
        this.frontendBaseUrl = frontendBaseUrl;
        this.expectedOidcRedirectUrl = expectedOidcRedirectUrl;
    }

    @BeforeClass
    public static void startIdpServer() throws IOException {
        mockIdpServer = MockIpdServer.forKeySet(TestJwk.Jwks.ALL).start();
    }

    @AfterClass
    public static void tearDown() {
        if (mockIdpServer != null) {
            try {
                mockIdpServer.close();
            } catch (Exception e) {
                log.error("Cannot stop IdP server", e);
            }
        }
    }

    @Before
    public void before() throws ConfigValidationException {
        Context testContext = new Context(VariableResolvers.ALL, null, null, null, null, null);
        Map<String, Object> basicAuthenticatorSettings = ImmutableMap.of("idp.openid_configuration_url", mockIdpServer.getDiscoverUri().toString(), "client_id",
            "search-guard-client", "client_secret", "s3cret", "pkce", false);
        this.authenticator = new OidcAuthenticator(basicAuthenticatorSettings, testContext);
    }

    @Test
    public void shouldCreateValidOidcRedirectUrl() throws AuthenticatorUnavailableException, MalformedURLException {
        AuthMethod authMethod = authenticator.activateFrontendConfig(OIDC_AUTH_METHOD, new Request(null, null, frontendBaseUrl));
        String redirectUrl = URLEncodedUtils.parse(authMethod.getSsoLocation(), Charset.forName("UTF-8")).stream()//
            .filter(nameValuePair -> "redirect_uri".equals(nameValuePair.getName()))//
            .map(pair -> pair.getValue())//
            .findFirst()//
            .orElseThrow(() -> new IllegalStateException("Redirect url not found"));

        assertThat(redirectUrl, equalTo(expectedOidcRedirectUrl));
    }
}
