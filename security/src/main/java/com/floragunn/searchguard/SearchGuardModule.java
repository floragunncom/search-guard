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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.floragunn.searchguard.configuration.validation.ConfigModificationValidator;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchsupport.StaticSettings;

public interface SearchGuardModule {
    default List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
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

    default StaticSettings.AttributeSet getSettings() {
        return StaticSettings.AttributeSet.empty();
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
    
    /**
     * These readers are not executed when an admin certificate user has initiated the operation
     */
    default ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> getDirectoryReaderWrappersForNormalOperations() {
        return ImmutableList.empty();
    }

    /**
     * These readers are also executed when an admin certificate user has initiated the operation
     */
    default ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> getDirectoryReaderWrappersForAllOperations() {
        return ImmutableList.empty();
    }
    
    default ImmutableList<SearchOperationListener> getSearchOperationListeners() {
        return ImmutableList.empty();
    }

    default ImmutableList<IndexingOperationListener> getIndexOperationListeners() {
        return ImmutableList.empty();
    }

    default ImmutableList<SyncAuthorizationFilter> getSyncAuthorizationFilters() {
        return ImmutableList.empty();
    }
    
    default ImmutableList<SyncAuthorizationFilter> getPrePrivilegeEvaluationSyncAuthorizationFilters() {
        return ImmutableList.empty();
    }

    default ImmutableList<Function<String, Predicate<String>>> getFieldFilters() {
        return ImmutableList.empty();
    }

    default ImmutableList<QueryCacheWeightProvider> getQueryCacheWeightProviders() {
        return ImmutableList.empty();
    }

    default AuditLog getAuditLog() {
        return null;
    }

    default void onNodeStarted() {

    }

    default ImmutableList<ActionFilter> getActionFilters() {
        return ImmutableList.empty();
    }

    default ImmutableList<ConfigModificationValidator<?>> getConfigModificationValidators() {
        return ImmutableList.empty();
    }

    @FunctionalInterface
    interface QueryCacheWeightProvider {
        Weight apply(Index index, Weight weight, QueryCachingPolicy policy);
    }
}
