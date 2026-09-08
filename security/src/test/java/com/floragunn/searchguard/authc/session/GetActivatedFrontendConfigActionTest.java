/*
 * Copyright 2026 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.floragunn.searchguard.authc.session;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig.AuthMethod;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction.Handler;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction.Request;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction.Response;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.ComponentState;

public class GetActivatedFrontendConfigActionTest {

    @Before
    public void resetActivationCount() {
        CountingApiAuthenticationFrontend.activationCount.set(0);
    }

    @Test
    public void dynamicHostGuardRunsBeforeAuthenticationFrontendActivation() throws Exception {
        FrontendAuthcConfig config = parseConfig(
                DocNode.of("type", CountingApiAuthenticationFrontend.class.getName(), "label", "Tenant 1", "enable_by_host",
                        Arrays.asList("tenant1-*")),
                DocNode.of("type", CountingApiAuthenticationFrontend.class.getName(), "label", "Tenant 2", "enable_by_host",
                        Arrays.asList("tenant2-*")));

        Response response = Handler.createResponse(config, request("tenant1-kibana"));

        assertThat(response.getAuthMethods().stream().map(AuthMethod::getLabel).toList(), contains("Tenant 1"));
        assertThat("Only the matching authentication frontend may be activated",
                CountingApiAuthenticationFrontend.activationCount.get(), is(1));
    }

    @Test
    public void nonMatchingDynamicHostDoesNotActivateAnyAuthenticationFrontend() throws Exception {
        FrontendAuthcConfig config = parseConfig(
                DocNode.of("type", CountingApiAuthenticationFrontend.class.getName(), "label", "Tenant 1", "enable_by_host",
                        Arrays.asList("tenant1-*")),
                DocNode.of("type", CountingApiAuthenticationFrontend.class.getName(), "label", "Tenant 2", "enable_by_host",
                        Arrays.asList("tenant2-*")));

        Response response = Handler.createResponse(config, request("unknown-kibana"));

        assertThat(response.getAuthMethods(), empty());
        assertThat("Non-matching authentication frontends must not be activated",
                CountingApiAuthenticationFrontend.activationCount.get(), is(0));
    }

    @Test
    public void authenticationDomainsWithoutEnableByHostRemainInResponse() throws Exception {
        FrontendAuthcConfig config = parseConfig(
                DocNode.of("type", "basic", "label", "Tenant 1", "enable_by_host", Arrays.asList("tenant1-*")),
                DocNode.of("type", "basic", "label", "Always available"));

        Response response = Handler.createResponse(config, request("unknown-kibana"));

        assertThat(response.getAuthMethods().stream().map(AuthMethod::getLabel).toList(), contains("Always available"));
    }

    @Test
    public void missingDynamicHostLeavesAllAuthenticationDomainsInResponse() throws Exception {
        FrontendAuthcConfig config = parseConfig(
                DocNode.of("type", "basic", "label", "Tenant 1", "enable_by_host", Arrays.asList("tenant1-*")),
                DocNode.of("type", "basic", "label", "Tenant 2", "enable_by_host", Arrays.asList("tenant2-*")));

        Response response = Handler.createResponse(config, request(null));

        assertThat(response.getAuthMethods().stream().map(AuthMethod::getLabel).toList(), contains("Tenant 1", "Tenant 2"));
    }

    private static Request request(String dynamicHost) {
        return new Request(null, null, null, null, dynamicHost);
    }

    private static FrontendAuthcConfig parseConfig(DocNode... domains) throws Exception {
        ConfigurationRepository.Context context = new ConfigurationRepository.Context(null,
                new SearchGuardModulesRegistry(Settings.EMPTY), null, null, null);
        return FrontendAuthcConfig.parse(DocNode.of("auth_domains", Arrays.asList(domains)), context).get();
    }

    public static class CountingApiAuthenticationFrontend implements ApiAuthenticationFrontend {
        private static final AtomicInteger activationCount = new AtomicInteger();
        private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "counting").initialized();

        public CountingApiAuthenticationFrontend(DocNode config, ConfigurationRepository.Context context) {
        }

        @Override
        public AuthMethod activateFrontendConfig(AuthMethod frontendConfig, Request request) {
            activationCount.incrementAndGet();
            return frontendConfig;
        }

        @Override
        public AuthCredentials extractCredentials(Map<String, Object> request)
                throws CredentialsException, ConfigValidationException, AuthenticatorUnavailableException {
            return null;
        }

        @Override
        public String getType() {
            return "counting";
        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }
    }
}
