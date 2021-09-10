/*
 * Copyright 2020-2021 floragunn GmbH
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

package com.floragunn.searchguard.modules;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptService;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.auth.api.AuthorizationBackend;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;

public class SearchGuardModulesRegistry {
    // TODO moduleinfo see reflectionhelper

    public static final Setting<List<String>> DISABLED_MODULES = Setting.listSetting("searchguard.modules.disabled", Collections.emptyList(),
            Function.identity(), Property.NodeScope);

    private static final Logger log = LogManager.getLogger(SearchGuardModulesRegistry.class);

    private List<SearchGuardModule<?>> modules = new ArrayList<>();
    private List<ComponentStateProvider> componentStateProviders = new ArrayList<>();
    private Set<String> moduleNames = new HashSet<>();
    private final Set<String> disabledModules;
    private final Settings settings;

    private SearchGuardComponentRegistry<AuthenticationBackend> authenticationBackends = new SearchGuardComponentRegistry<AuthenticationBackend>(
            AuthenticationBackend.class, (o) -> o.getType()).add(StandardComponents.authcBackends);

    private SearchGuardComponentRegistry<AuthorizationBackend> authorizationBackends = new SearchGuardComponentRegistry<AuthorizationBackend>(
            AuthorizationBackend.class, (o) -> o.getType()).add(StandardComponents.authzBackends);

    private SearchGuardComponentRegistry<HTTPAuthenticator> httpAuthenticators = new SearchGuardComponentRegistry<HTTPAuthenticator>(
            HTTPAuthenticator.class, (o) -> o.getType()).add(StandardComponents.httpAuthenticators);

    private SearchGuardComponentRegistry<ApiAuthenticationFrontend> apiAuthenticationFrontends = new SearchGuardComponentRegistry<ApiAuthenticationFrontend>(
            ApiAuthenticationFrontend.class, (o) -> o.getType()).add(StandardComponents.apiAuthenticationFrontends);

    private SearchGuardComponentRegistry<AuthFailureListener> authFailureListeners = new SearchGuardComponentRegistry<AuthFailureListener>(
            AuthFailureListener.class, (o) -> o.getType()).add(StandardComponents.authFailureListeners);

    public SearchGuardModulesRegistry(Settings settings) {
        this.disabledModules = new HashSet<>(DISABLED_MODULES.get(settings));
        this.settings = settings;
    }

    public void add(String... classes) {
        for (String clazz : classes) {
            try {
                if (disabledModules.contains(clazz)) {
                    log.info(clazz + " is disabled");
                    continue;
                }

                if (moduleNames.contains(clazz)) {
                    throw new IllegalStateException(clazz + " is already registered");
                }

                moduleNames.add(clazz);

                Object object = createModule(clazz);

                if (object instanceof SearchGuardModule) {
                    modules.add((SearchGuardModule<?>) object);
                } else {
                    log.error(object + " does not implement SearchGuardSubModule");
                }

                if (object instanceof ComponentStateProvider) {
                    componentStateProviders.add((ComponentStateProvider) object);
                }
            } catch (ClassNotFoundException e) {
                log.warn("Module class does not exist " + clazz);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                    | SecurityException e) {
                log.error("Error while instantiating " + clazz, e);
            }
        }
    }

    public void addComponentStateProvider(ComponentStateProvider componentStateProvider) {
        componentStateProviders.add(componentStateProvider);
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        List<RestHandler> result = new ArrayList<>();

        for (SearchGuardModule<?> module : modules) {
            result.addAll(module.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings, settingsFilter,
                    indexNameExpressionResolver, scriptService, nodesInCluster));
        }

        return result;
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> result = new ArrayList<>();

        for (SearchGuardModule<?> module : modules) {
            result.addAll(module.getActions());
        }

        return result;
    }

    public List<ScriptContext<?>> getContexts() {
        List<ScriptContext<?>> result = new ArrayList<>();

        for (SearchGuardModule<?> module : modules) {
            result.addAll(module.getContexts());
        }

        return result;
    }

    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        List<Object> result = new ArrayList<>();

        for (SearchGuardModule<?> module : modules) {
            result.addAll(module.createComponents(baseDependencies));

            registerConfigChangeListener(module, baseDependencies.getDynamicConfigFactory());
        }

        authenticationBackends.addComponentsWithMatchingType(result);
        authorizationBackends.addComponentsWithMatchingType(result);
        httpAuthenticators.addComponentsWithMatchingType(result);

        return result;
    }

    public List<Setting<?>> getSettings() {
        List<Setting<?>> result = new ArrayList<>();

        for (SearchGuardModule<?> module : modules) {
            result.addAll(module.getSettings());
        }

        return result;
    }

    public void onNodeStarted() {
        for (SearchGuardModule<?> module : modules) {
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

    @SuppressWarnings("unchecked")
    private void registerConfigChangeListener(SearchGuardModule<?> module, DynamicConfigFactory dynamicConfigFactory) {
        SearchGuardModule.SgConfigMetadata<?> configMetadata = module.getSgConfigMetadata();

        if (configMetadata == null) {
            return;
        }

        dynamicConfigFactory.addConfigChangeListener(configMetadata.getSgConfigType(), (config) -> {
            @SuppressWarnings("rawtypes")
            Consumer consumer = configMetadata.getConfigConsumer();

            try {
                Object convertedConfig = convert(configMetadata, config);

                if (log.isDebugEnabled()) {
                    log.debug("New configuration for " + module + ": " + convertedConfig);
                }
                consumer.accept(convertedConfig);

                if (module instanceof ComponentStateProvider) {
                    ComponentState configComponentState = ((ComponentStateProvider) module).getComponentState().getOrCreatePart("config",
                            "sg_config");

                    configComponentState.setInitialized();
                }

            } catch (ConfigValidationException e) {
                log.error("Error while parsing configuration for " + module + "\n" + e.getValidationErrors(), e);

                if (module instanceof ComponentStateProvider) {
                    ComponentState configComponentState = ((ComponentStateProvider) module).getComponentState().getOrCreatePart("config",
                            "sg_config");

                    configComponentState.setFailed(e.getMessage());
                    configComponentState.setDetailJson(e.getValidationErrors().toJsonString());
                }

                consumer.accept(null);
            } catch (Exception e) {
                log.error("Error while parsing configuration for " + module, e);

                if (module instanceof ComponentStateProvider) {
                    ComponentState configComponentState = ((ComponentStateProvider) module).getComponentState().getOrCreatePart("config",
                            "sg_config");

                    configComponentState.setFailed(e);
                }
            }
        });
    }

    private <T> T convert(SearchGuardModule.SgConfigMetadata<T> configMetadata, SgDynamicConfiguration<?> value) throws ConfigValidationException {
        if (value == null) {
            return null;
        }

        Object entry = value.getCEntry(configMetadata.getEntry());

        if (entry == null) {
            if (log.isDebugEnabled()) {
                log.debug("No config entry " + configMetadata.getEntry() + " in " + value);
            }
            return null;
        }

        JsonNode subNode = DefaultObjectMapper.objectMapper.valueToTree(entry).at(configMetadata.getJsonPointer());

        if (subNode == null || subNode.isMissingNode()) {
            if (log.isDebugEnabled()) {
                log.debug("JsonPointer " + configMetadata.getJsonPointer() + " in " + value + " not found");
            }
            return null;
        }
        
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = DefaultObjectMapper.readTree(subNode, Map.class);

            return configMetadata.getConfigParser().apply(map);
        } catch (ConfigValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
        }
    }

    public SearchGuardComponentRegistry<AuthenticationBackend> getAuthenticationBackends() {
        return authenticationBackends;
    }

    public SearchGuardComponentRegistry<AuthorizationBackend> getAuthorizationBackends() {
        return authorizationBackends;
    }

    public SearchGuardComponentRegistry<HTTPAuthenticator> getHttpAuthenticators() {
        return httpAuthenticators;
    }

    public SearchGuardComponentRegistry<ApiAuthenticationFrontend> getApiAuthenticationFrontends() {
        return apiAuthenticationFrontends;
    }
    
    public SearchGuardComponentRegistry<AuthFailureListener> getAuthFailureListeners() {
        return authFailureListeners;
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

}
