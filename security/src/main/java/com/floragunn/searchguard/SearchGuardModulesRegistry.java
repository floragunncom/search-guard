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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

import com.floragunn.searchguard.authc.AuthenticationBackend;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.rest.authenticators.AnonymousAuthenticator;
import com.floragunn.searchguard.authc.rest.authenticators.BasicAuthenticator;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPClientCertAuthenticator;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPTrustedOriginAuthenticator;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.http.LinkApiAuthenticationFrontend;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.privileges.PrivilegesInterceptor;
import com.floragunn.searchsupport.util.ImmutableList;

public class SearchGuardModulesRegistry {
    // TODO moduleinfo see reflectionhelper

    public static final Setting<List<String>> DISABLED_MODULES = Setting.listSetting("searchguard.modules.disabled", Collections.emptyList(),
            Function.identity(), Property.NodeScope);

    private static final Logger log = LogManager.getLogger(SearchGuardModulesRegistry.class);

    private List<SearchGuardModule> modules = new ArrayList<>();
    private List<ComponentStateProvider> componentStateProviders = new ArrayList<>();
    private PrivilegesInterceptor privilegesInterceptor;
    private Set<String> moduleNames = new HashSet<>();
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
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        List<RestHandler> result = new ArrayList<>();

        for (SearchGuardModule module : modules) {
            result.addAll(module.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings, settingsFilter,
                    indexNameExpressionResolver, scriptService, nodesInCluster));
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

        List<Object> privilegesInterceptors = result.stream().filter(o -> o instanceof PrivilegesInterceptor).collect(Collectors.toList());

        if (privilegesInterceptors.size() != 0) {
            this.privilegesInterceptor = (PrivilegesInterceptor) privilegesInterceptors.get(0);
        }

        return result;
    }

    public List<Setting<?>> getSettings() {
        List<Setting<?>> result = new ArrayList<>();

        for (SearchGuardModule module : modules) {
            result.addAll(module.getSettings());
        }

        return result;
    }

    public List<AuthenticationDomain<HTTPAuthenticator>> getImplicitHttpAuthenticationDomains() {
        ImmutableList.Builder<AuthenticationDomain<HTTPAuthenticator>> result = new ImmutableList.Builder<>();

        for (SearchGuardModule module : modules) {
            result.with(module.getImplicitHttpAuthenticationDomains());
        }

        return result.build();
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

    public PrivilegesInterceptor getPrivilegesInterceptor() {
        return privilegesInterceptor;
    }

    private static TypedComponentRegistry createTypedComponentRegistryWithDefaults() {
        TypedComponentRegistry typedComponentRegistry = new TypedComponentRegistry();

        typedComponentRegistry.registerInstance(AuthenticationBackend.class, "noop", AuthenticationBackend.NOOP);      
        
        typedComponentRegistry.registerInstance(HTTPAuthenticator.class, "basic", new BasicAuthenticator(null, null));
        typedComponentRegistry.registerInstance(HTTPAuthenticator.class, "anonymous", new AnonymousAuthenticator());
        typedComponentRegistry.registerInstance(HTTPAuthenticator.class, "trusted_origin", new HTTPTrustedOriginAuthenticator(null, null));
        typedComponentRegistry.registerInstance(HTTPAuthenticator.class, "clientcert", new HTTPClientCertAuthenticator(null, null));
        
        typedComponentRegistry.registerInstance(ApiAuthenticationFrontend.class, "basic", new BasicAuthenticator(null, null));
        typedComponentRegistry.register(LinkApiAuthenticationFrontend.class, "link", LinkApiAuthenticationFrontend::new);
        
        return typedComponentRegistry;

    }

    public TypedComponentRegistry getTypedComponentRegistry() {
        return typedComponentRegistry;
    }
   
}
