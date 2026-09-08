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
import static org.hamcrest.Matchers.is;

import java.util.Arrays;

import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.authc.session.FrontendAuthcConfig.FrontendAuthenticationDomain;

public class FrontendAuthcConfigTest {

    @Test
    public void enableByHostMatchesExactAndPatternHostNames() throws Exception {
        FrontendAuthenticationDomain domain = parseDomain(DocNode.of("type", "basic", "enable_by_host",
                Arrays.asList("tenant1-kibana", "tenant1-*.customername.com", "/tenant1-[0-9]+\\.internal/")));

        assertThat(domain.isEnabledForDynamicHost("tenant1-kibana"), is(true));
        assertThat(domain.isEnabledForDynamicHost("tenant1-eu.customername.com"), is(true));
        assertThat(domain.isEnabledForDynamicHost("tenant1-42.internal"), is(true));
        assertThat(domain.isEnabledForDynamicHost("tenant2-kibana"), is(false));
    }

    @Test
    public void enableByHostIsIgnoredWhenDynamicHostIsNotSpecified() throws Exception {
        FrontendAuthenticationDomain domain = parseDomain(
                DocNode.of("type", "basic", "enable_by_host", Arrays.asList("tenant1-kibana")));

        assertThat(domain.isEnabledForDynamicHost(null), is(true));
    }

    @Test
    public void domainWithoutEnableByHostIsAlwaysEnabled() throws Exception {
        FrontendAuthenticationDomain domain = parseDomain(DocNode.of("type", "basic"));

        assertThat(domain.isEnabledForDynamicHost(null), is(true));
        assertThat(domain.isEnabledForDynamicHost("any-host"), is(true));
    }

    private static FrontendAuthenticationDomain parseDomain(DocNode domain) throws Exception {
        FrontendAuthcConfig config = parseConfig(domain);
        return config.getAuthDomains().get(0);
    }

    private static FrontendAuthcConfig parseConfig(DocNode... domains) throws Exception {
        return FrontendAuthcConfig.parse(DocNode.of("auth_domains", Arrays.asList(domains)), null).get();
    }
}
