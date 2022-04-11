/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.legacy;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.action.licenseinfo.LicenseInfoAction;
import com.floragunn.searchguard.action.licenseinfo.SearchGuardLicenseAction;
import com.floragunn.searchguard.action.licenseinfo.TransportLicenseInfoAction;
import com.floragunn.searchguard.authc.internal_users_db.InternalUsersDatabase;
import com.floragunn.searchguard.legacy.auth.HTTPBasicAuthenticator;
import com.floragunn.searchguard.legacy.auth.HTTPClientCertAuthenticator;
import com.floragunn.searchguard.legacy.auth.HTTPProxyAuthenticator;
import com.floragunn.searchguard.legacy.auth.HTTPProxyAuthenticator2;
import com.floragunn.searchguard.legacy.auth.InternalAuthenticationBackend;
import com.floragunn.searchguard.legacy.auth.NoOpAuthenticationBackend;

@Deprecated
public class LegacySecurityModule implements SearchGuardModule {

    private InternalUsersDatabase internalUsersDatabase;

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        this.internalUsersDatabase = baseDependencies.getInternalUsersDatabase();

        return Collections.emptyList();
    }

    @Override
    public List<TypedComponent.Info<?>> getTypedComponents() {
        return ImmutableList.of(//
                new InternalAuthenticationBackend.AuthcBackendInfo(internalUsersDatabase), //
                new InternalAuthenticationBackend.AuthzBackendInfo(internalUsersDatabase), //
                HTTPClientCertAuthenticator.INFO, HTTPProxyAuthenticator.INFO, HTTPProxyAuthenticator2.INFO, NoOpAuthenticationBackend.INFO,
                HTTPBasicAuthenticator.INFO);

    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        return ImmutableList.of(new SearchGuardLicenseAction(settings, restController));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return ImmutableList.of(new ActionHandler<>(LicenseInfoAction.INSTANCE, TransportLicenseInfoAction.class));
    }

}
