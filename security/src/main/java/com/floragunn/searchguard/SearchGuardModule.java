/*
 * Copyright 2020-2022 floragunn GmbH
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

package com.floragunn.searchguard;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptService;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;

public interface SearchGuardModule {
    default List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        return Collections.emptyList();
    }

    default List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.emptyList();
    }

    default List<ScriptContext<?>> getContexts() {
        return Collections.emptyList();
    }

    default Collection<Object> createComponents(BaseDependencies baseDependencies) {
        return Collections.emptyList();
    }

    default List<Setting<?>> getSettings() {
        return Collections.emptyList();
    }

    default List<AuthenticationDomain<HttpAuthenticationFrontend>> getImplicitHttpAuthenticationDomains() {
        return Collections.emptyList();
    }

    default List<TypedComponent.Info<?>> getTypedComponents() {
        return Collections.emptyList();
    }

    default ImmutableSet<String> getCapabilities() {
        return ImmutableSet.empty();
    }

    default ImmutableSet<String> getUiCapabilities() {
        return ImmutableSet.empty();
    }

    default ImmutableSet<String> getPublicCapabilities() {
        return ImmutableSet.empty();
    }

    default void onNodeStarted() {

    }
}
