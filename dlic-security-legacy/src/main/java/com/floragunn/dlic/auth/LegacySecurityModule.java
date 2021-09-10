/*
 * Copyright 2021 by floragunn GmbH - All rights reserved
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
import java.util.List;
import java.util.function.Supplier;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;

import com.floragunn.dlic.auth.http.jwt.keybyoidc.OidcConfigRestAction;
import com.floragunn.dlic.auth.http.saml.AuthTokenProcessorAction;
import com.floragunn.searchguard.modules.SearchGuardModule;

@Deprecated
public class LegacySecurityModule implements SearchGuardModule<Void> {
    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new OidcConfigRestAction(), new AuthTokenProcessorAction());
    }
}
