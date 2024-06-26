/*
 * Copyright 2022 floragunn GmbH
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
 * 
 */

package com.floragunn.searchguard.authc.session.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticationFrontend;
import com.floragunn.searchguard.authc.rest.RestAuthcConfig;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.authc.session.FrontendAuthcConfig;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;

class MergedAuthcConfig {

    private final SgDynamicConfiguration<FrontendAuthcConfig> frontendConfig;
    private final RestAuthcConfig authcConfig;
    private final ImmutableMap<String, ImmutableList<AuthenticationDomain<ApiAuthenticationFrontend>>> configNameToAuthenticationDomainMap;

    public MergedAuthcConfig(SgDynamicConfiguration<FrontendAuthcConfig> frontendConfig, RestAuthcConfig authcConfig) {
        this.frontendConfig = frontendConfig;
        this.authcConfig = authcConfig;
        this.configNameToAuthenticationDomainMap = createDomainMap(frontendConfig, authcConfig);
    }

    ImmutableList<AuthenticationDomain<ApiAuthenticationFrontend>> get(String config) {
        return configNameToAuthenticationDomainMap.get(config);
    }

    boolean isDebugEnabled(String config) {
        if (this.authcConfig != null && this.authcConfig.isDebugEnabled()) {
            return true;
        }

        FrontendAuthcConfig frontendConfig = this.frontendConfig.getCEntry(config);

        if (frontendConfig != null) {
            return frontendConfig.isDebug();
        } else {
            return false;
        }
    }

    private List<AuthenticationDomain<ApiAuthenticationFrontend>> getApiAuthenticationDomains(RestAuthcConfig authcConfig) {
        if (authcConfig == null) {
            return ImmutableList.empty();
        }

        List<AuthenticationDomain<ApiAuthenticationFrontend>> result = new ArrayList<>(authcConfig.getAuthenticators().size());

        for (AuthenticationDomain<? extends AuthenticationFrontend> domain : authcConfig.getAuthenticators()) {
            if (domain.getFrontend() instanceof ApiAuthenticationFrontend && domain.isEnabled()) {
                @SuppressWarnings("unchecked")
                AuthenticationDomain<ApiAuthenticationFrontend> apiAuthenticationDomain = (AuthenticationDomain<ApiAuthenticationFrontend>) domain;
                result.add(apiAuthenticationDomain);
            }
        }

        return result;
    }

    private ImmutableMap<String, ImmutableList<AuthenticationDomain<ApiAuthenticationFrontend>>> createDomainMap(
            SgDynamicConfiguration<FrontendAuthcConfig> frontendConfig, RestAuthcConfig authczConfig) {
        List<AuthenticationDomain<ApiAuthenticationFrontend>> globalDomains = getApiAuthenticationDomains(authczConfig);

        if (frontendConfig.getCEntries().isEmpty()) {
            return ImmutableMap.of("default", ImmutableList.of(globalDomains));
        } else {
            ImmutableMap.Builder<String, ImmutableList<AuthenticationDomain<ApiAuthenticationFrontend>>> result = new ImmutableMap.Builder<>(
                    frontendConfig.getCEntries().size());

            for (Map.Entry<String, FrontendAuthcConfig> entry : frontendConfig.getCEntries().entrySet()) {
                result.with(entry.getKey(),
                        ImmutableList.concat(globalDomains, entry.getValue().getAuthDomains().map(a -> a.getAuthenticationDomain())));
            }

            return result.build();
        }
    }

    @Override
    public String toString() {
        return configNameToAuthenticationDomainMap.keySet().toString();
    }

    Collection<AuthenticationDomain<ApiAuthenticationFrontend>> getAuthenticationDomains() {
        return this.configNameToAuthenticationDomainMap.values().stream().flatMap((l) -> l.stream()).collect(Collectors.toList());
    }
}
