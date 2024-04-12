/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.floragunn.searchguard.enterprise.auth.ldap.LDAPAuthenticationBackend;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;

import com.floragunn.dlic.auth.http.jwt.HTTPJwtAuthenticator;
import com.floragunn.dlic.auth.http.jwt.keybyoidc.HTTPJwtKeyByOpenIdConnectAuthenticator;
import com.floragunn.dlic.auth.http.jwt.keybyoidc.OidcConfigRestAction;
import com.floragunn.dlic.auth.http.saml.AuthTokenProcessorAction;
import com.floragunn.dlic.auth.http.saml.HTTPSamlAuthenticator;
import com.floragunn.dlic.auth.kerberos.HTTPSpnegoAuthenticator;
import com.floragunn.dlic.auth.ldap2.LDAPAuthenticationBackend2;
import com.floragunn.dlic.auth.ldap2.LDAPAuthorizationBackend2;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.TypedComponent.Info;
import com.floragunn.searchguard.support.ConfigConstants;
import com.google.common.collect.ImmutableList;

@Deprecated
public class LegacyEnterpriseSecurityModule implements SearchGuardModule {

    private boolean enterpriseModulesEnabled;

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        enterpriseModulesEnabled = baseDependencies.getSettings().getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true);

        return Collections.emptyList();
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
        return Arrays.asList(new OidcConfigRestAction(), new AuthTokenProcessorAction());
    }

    @Override
    public List<Info<?>> getTypedComponents() {
        if (enterpriseModulesEnabled) {
            return ImmutableList.of(HTTPJwtAuthenticator.INFO, HTTPJwtKeyByOpenIdConnectAuthenticator.INFO, HTTPSamlAuthenticator.INFO,
                    LDAPAuthenticationBackend2.INFO, LDAPAuthorizationBackend2.INFO,
                    HTTPSpnegoAuthenticator.INFO);
        } else {
            return Collections.emptyList();
        }
    }
}
