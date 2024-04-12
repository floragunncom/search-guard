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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.floragunn.searchguard.authz.TenantAccessMapper;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.configuration.validation.ConfigModificationValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.SearchGuardModule.QueryCacheWeightProvider;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthenticationBackend;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.authc.rest.authenticators.AnonymousAuthenticationFrontend;
import com.floragunn.searchguard.authc.rest.authenticators.BasicAuthenticationFrontend;
import com.floragunn.searchguard.authc.rest.authenticators.HttpClientCertAuthenticationFrontend;
import com.floragunn.searchguard.authc.rest.authenticators.HttpTrustedOriginAuthenticationFrontend;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.authc.session.LinkApiAuthenticationFrontend;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;

public class SearchGuardModulesRegistry {

    public static final Setting<List<String>> DISABLED_MODULES = Setting.listSetting("searchguard.modules.disabled", Collections.emptyList(),
            Function.identity(), Property.NodeScope);

    private static final Logger log = LogManager.getLogger(SearchGuardModulesRegistry.class);

    private List<SearchGuardModule> modules = new ArrayList<>();
    private List<ComponentStateProvider> componentStateProviders = new ArrayList<>();
    private ImmutableList<SearchOperationListener> searchOperationListeners;
    private ImmutableList<IndexingOperationListener> indexOperationListeners;
    private ImmutableList<ActionFilter> actionFilters;
    private ImmutableList<SyncAuthorizationFilter> syncAuthorizationFilters;
    private ImmutableList<SyncAuthorizationFilter> prePrivilegeEvaluationSyncAuthorizationFilters;
    private ImmutableList<Function<String, Predicate<String>>> fieldFilters;
    private ImmutableList<QueryCacheWeightProvider> queryCacheWeightProviders;
    private ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> directoryReaderWrappersForNormalOperations;
    private ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> directoryReaderWrappersForAllOperations;

    private MultiTenancyConfigurationProvider multiTenancyConfigurationProvider;
    private TenantAccessMapper tenantAccessMapper;
    private Set<String> moduleNames = new HashSet<>();
    private ImmutableList<ConfigModificationValidator<?>> configModificationValidators;
    private final Set<String> disabledModules;
    private final Settings settings;

    private final TypedComponentRegistry typedComponentRegistry = createTypedComponentRegistryWithDefaults();

    public SearchGuardModulesRegistry(Settings settings) {
        this.disabledModules = new HashSet<>(DISABLED_MODULES.get(settings));
        this.settings = settings;
    }

    public void add(String... classes) {
        if (disabledModules.contains("all")) {
            log.info("All Search Guard modules are disabled by configuration");
            return;
        }

        for (String clazz : classes) {
            try {
                if (disabledModules.contains(clazz)) {
                    log.info("Disabled:  " + clazz);
                    continue;
                }

                if (moduleNames.contains(clazz)) {
                    throw new IllegalStateException(clazz + " is already registered");
                }

                moduleNames.add(clazz);

                Object object = createModule(clazz);

                if (object instanceof SearchGuardModule) {
                    modules.add((SearchGuardModule) object);
                } else {
                    log.error(object + " does not implement SearchGuardSubModule");
                }

                if (object instanceof ComponentStateProvider) {
                    componentStateProviders.add((ComponentStateProvider) object);
                }

                log.info("Active:    " + clazz);

            } catch (ClassNotFoundException e) {
                log.warn("Not found: " + clazz);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                    | SecurityException e) {
                log.error("Error:    " + clazz, e);
            }
        }
    }

    public void addComponentStateProvider(ComponentStateProvider componentStateProvider) {
        componentStateProviders.add(componentStateProvider);
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
        List<RestHandler> result = new ArrayList<>();

        for (SearchGuardModule module : modules) {
            result.addAll(module.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings, settingsFilter,
                    indexNameExpressionResolver, scriptService, nodesInCluster, clusterSupportsFeature));
        }

        return result;
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> result = new ArrayList<>();

        for (SearchGuardModule module : modules) {
            result.addAll(module.getActions());
        }

        return result;
    }

    public List<ScriptContext<?>> getContexts() {
        List<ScriptContext<?>> result = new ArrayList<>();

        for (SearchGuardModule module : modules) {
            result.addAll(module.getContexts());
        }

        return result;
    }

    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        List<Object> result = new ArrayList<>();

        for (SearchGuardModule module : modules) {
            result.addAll(module.createComponents(baseDependencies));

            typedComponentRegistry.register(module.getTypedComponents());
        }

        this.multiTenancyConfigurationProvider = getMultiTenancyConfigurationProvider(result);
        this.tenantAccessMapper = getTenantAccessMapper(result);

        return result;
    }

    public ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> getDirectoryReaderWrappersForNormalOperations() {
        ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> result = this.directoryReaderWrappersForNormalOperations;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getDirectoryReaderWrappersForNormalOperations());
            }

            this.directoryReaderWrappersForNormalOperations = result;
        }

        return result;
    }

    public ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> getDirectoryReaderWrappersForAllOperations() {
        ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> result = this.directoryReaderWrappersForAllOperations;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getDirectoryReaderWrappersForAllOperations());
            }

            this.directoryReaderWrappersForAllOperations = result;
        }

        return result;
    }
        
    public ImmutableList<SearchOperationListener> getSearchOperationListeners() {

        ImmutableList<SearchOperationListener> result = this.searchOperationListeners;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getSearchOperationListeners());
            }

            this.searchOperationListeners = result;
        }

        return result;
    }

    public ImmutableList<IndexingOperationListener> getIndexOperationListeners() {

        ImmutableList<IndexingOperationListener> result = this.indexOperationListeners;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getIndexOperationListeners());
            }

            this.indexOperationListeners = result;
        }

        return result;
    }

    public ImmutableList<ActionFilter> getActionFilters() {

        ImmutableList<ActionFilter> result = this.actionFilters;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getActionFilters());
            }

            this.actionFilters = result;
        }

        return result;
    }

    public List<ConfigModificationValidator<?>> getConfigModificationValidators() {
        ImmutableList<ConfigModificationValidator<?>> result = this.configModificationValidators;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getConfigModificationValidators());
            }

            this.configModificationValidators = result;
        }

        return result;
    }
    
    public ImmutableList<SyncAuthorizationFilter> getSyncAuthorizationFilters() {
        ImmutableList<SyncAuthorizationFilter> result = this.syncAuthorizationFilters;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getSyncAuthorizationFilters());
            }
        }

        return result;
    }

    public ImmutableList<SyncAuthorizationFilter> getPrePrivilegeSyncAuthorizationFilters() {
        ImmutableList<SyncAuthorizationFilter> result = this.prePrivilegeEvaluationSyncAuthorizationFilters;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getPrePrivilegeEvaluationSyncAuthorizationFilters());
            }
        }

        return result;
    }
    
    public ImmutableList<Function<String, Predicate<String>>> getFieldFilters() {
        ImmutableList<Function<String, Predicate<String>>> result = this.fieldFilters;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getFieldFilters());
            }
        }

        return result;
    }

    public ImmutableList<QueryCacheWeightProvider> getQueryCacheWeightProviders() {
        ImmutableList<QueryCacheWeightProvider> result = this.queryCacheWeightProviders;

        if (result == null) {
            result = ImmutableList.empty();

            for (SearchGuardModule module : modules) {
                result = result.with(module.getQueryCacheWeightProviders());
            }
        }

        return result;
    }

    public List<Setting<?>> getSettings() {
        List<Setting<?>> result = new ArrayList<>();

        for (SearchGuardModule module : modules) {
            result.addAll(module.getSettings().toPlatform());
        }

        return result;
    }

    public ImmutableList<AuthenticationDomain<HttpAuthenticationFrontend>> getImplicitHttpAuthenticationDomains() {
        ImmutableList.Builder<AuthenticationDomain<HttpAuthenticationFrontend>> result = new ImmutableList.Builder<>();

        for (SearchGuardModule module : modules) {
            result.with(module.getImplicitHttpAuthenticationDomains());
        }

        return result.build();
    }
    
    public AuditLog getAuditLog() {
        for (SearchGuardModule module : modules) {
            AuditLog auditLog = module.getAuditLog();
            
            if (auditLog != null) {
                return auditLog;
            }
        }
        
        return null;
    }

    public void onNodeStarted() {
        for (SearchGuardModule module : modules) {
            module.onNodeStarted();
        }
    }

    public List<ComponentState> getComponentStates() {
        List<ComponentState> result = new ArrayList<>(componentStateProviders.size());

        for (ComponentStateProvider provider : componentStateProviders) {
            try {
                ComponentState componentState = provider.getComponentState();

                if (componentState != null) {
                    componentState.updateStateFromParts();
                    result.add(componentState);
                }
            } catch (Exception e) {
                log.error("Error while retrieving component state from " + provider);
            }
        }

        return result;
    }

    public ComponentState getComponentState(String moduleName) {
        for (ComponentStateProvider provider : componentStateProviders) {
            ComponentState componentState = provider.getComponentState();

            if (componentState != null && componentState.getName().equals(moduleName)) {
                componentState.updateStateFromParts();
                return componentState;
            }

        }

        return null;
    }

    private Object createModule(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        Class<?> clazz = Class.forName(className);

        try {
            Constructor<?> constructor = clazz.getDeclaredConstructor(Settings.class);

            return constructor.newInstance(settings);
        } catch (NoSuchMethodException e) {
            // ignore
        }

        return Class.forName(className).getDeclaredConstructor().newInstance();
    }

    public MultiTenancyConfigurationProvider getMultiTenancyConfigurationProvider() {
        return multiTenancyConfigurationProvider;
    }

    public TenantAccessMapper getTenantAccessMapper() {
        return tenantAccessMapper;
    }

    private static TypedComponentRegistry createTypedComponentRegistryWithDefaults() {
        TypedComponentRegistry typedComponentRegistry = new TypedComponentRegistry();

        typedComponentRegistry.registerInstance(AuthenticationBackend.class, "noop", AuthenticationBackend.NOOP);

        typedComponentRegistry.register(HttpAuthenticationFrontend.class, "basic", BasicAuthenticationFrontend::new);
        typedComponentRegistry.registerInstance(HttpAuthenticationFrontend.class, "anonymous", new AnonymousAuthenticationFrontend());
        typedComponentRegistry.registerInstance(HttpAuthenticationFrontend.class, "trusted_origin",
                new HttpTrustedOriginAuthenticationFrontend(null, null));
        typedComponentRegistry.registerInstance(HttpAuthenticationFrontend.class, "clientcert", new HttpClientCertAuthenticationFrontend(null, null));

        typedComponentRegistry.register(ApiAuthenticationFrontend.class, "basic", BasicAuthenticationFrontend::new);
        typedComponentRegistry.register(ApiAuthenticationFrontend.class, "link", LinkApiAuthenticationFrontend::new);

        return typedComponentRegistry;

    }

    public TypedComponentRegistry getTypedComponentRegistry() {
        return typedComponentRegistry;
    }

    public List<SearchGuardModule> getModules() {
        return Collections.unmodifiableList(modules);
    }

    private MultiTenancyConfigurationProvider getMultiTenancyConfigurationProvider(List<Object> componentsList) {
        List<Object> multiTenancyConfigurationProviders = componentsList.stream()
                .filter(o -> MultiTenancyConfigurationProvider.class.isAssignableFrom(o.getClass())).toList();

        if (!multiTenancyConfigurationProviders.isEmpty()) {
            return (MultiTenancyConfigurationProvider) multiTenancyConfigurationProviders.get(0);
        } else {
            return MultiTenancyConfigurationProvider.DEFAULT;
        }
    }

    private TenantAccessMapper getTenantAccessMapper(List<Object> componentsList) {
        List<Object> tenantAccessMappers = componentsList.stream()
                .filter(o -> TenantAccessMapper.class.isAssignableFrom(o.getClass())).toList();

        if (!tenantAccessMappers.isEmpty()) {
            return (TenantAccessMapper) tenantAccessMappers.get(0);
        } else {
            return TenantAccessMapper.NO_OP;
        }
    }

}
