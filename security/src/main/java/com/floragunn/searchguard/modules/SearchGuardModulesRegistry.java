package com.floragunn.searchguard.modules;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.AuthenticationBackend;
import com.floragunn.searchguard.auth.AuthorizationBackend;
import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.modules.SearchGuardModule.BaseDependencies;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;

public class SearchGuardModulesRegistry {
    // TODO moduleinfo see reflectionhelper

    public static final Setting<List<String>> DISABLED_MODULES = Setting.listSetting("searchguard.modules.disabled", Collections.emptyList(),
            Function.identity(), Property.NodeScope);

    private static final Logger log = LogManager.getLogger(SearchGuardModulesRegistry.class);

    private List<SearchGuardModule<?>> subModules = new ArrayList<>();
    private Set<String> moduleNames = new HashSet<>();
    private final Set<String> disabledModules;

    private SearchGuardComponentRegistry<AuthenticationBackend> authenticationBackends = new SearchGuardComponentRegistry<AuthenticationBackend>(
            AuthenticationBackend.class, (o) -> o.getType()).add(StandardComponents.authcBackends);

    private SearchGuardComponentRegistry<AuthorizationBackend> authorizationBackends = new SearchGuardComponentRegistry<AuthorizationBackend>(
            AuthorizationBackend.class, (o) -> o.getType()).add(StandardComponents.authzBackends);

    private SearchGuardComponentRegistry<HTTPAuthenticator> httpAuthenticators = new SearchGuardComponentRegistry<HTTPAuthenticator>(
            HTTPAuthenticator.class, (o) -> o.getType()).add(StandardComponents.httpAuthenticators);
    
    private SearchGuardComponentRegistry<AuthFailureListener> authFailureListeners = new SearchGuardComponentRegistry<AuthFailureListener>(
            AuthFailureListener.class, (o) -> o.getType()).add(StandardComponents.authFailureListeners);

    public SearchGuardModulesRegistry(Settings settings) {
        this.disabledModules = new HashSet<>(DISABLED_MODULES.get(settings));
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

                Object object = Class.forName(clazz).getDeclaredConstructor().newInstance();

                if (object instanceof SearchGuardModule) {
                    subModules.add((SearchGuardModule<?>) object);
                } else {
                    log.error(object + " does not implement SearchGuardSubModule");
                }
            } catch (ClassNotFoundException e) {
                log.warn("Module class does not exist " + clazz);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                    | SecurityException e) {
                log.error("Error while instantiating " + clazz, e);
            }
        }
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        List<RestHandler> result = new ArrayList<>();

        for (SearchGuardModule<?> module : subModules) {
            result.addAll(module.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings, settingsFilter,
                    indexNameExpressionResolver, nodesInCluster));
        }

        return result;
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> result = new ArrayList<>();

        for (SearchGuardModule<?> module : subModules) {
            result.addAll(module.getActions());
        }

        return result;
    }

    public List<ScriptContext<?>> getContexts() {
        List<ScriptContext<?>> result = new ArrayList<>();

        for (SearchGuardModule<?> module : subModules) {
            result.addAll(module.getContexts());
        }

        return result;
    }

    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        List<Object> result = new ArrayList<>();

        for (SearchGuardModule<?> module : subModules) {
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

        for (SearchGuardModule<?> module : subModules) {
            result.addAll(module.getSettings());
        }

        return result;
    }

    public void onNodeStarted() {
        for (SearchGuardModule<?> module : subModules) {
            module.onNodeStarted();
        }
    }

    @SuppressWarnings("unchecked")
    private void registerConfigChangeListener(SearchGuardModule<?> module, DynamicConfigFactory dynamicConfigFactory) {
        SearchGuardModule.SgConfigMetadata<?> configMetadata = module.getSgConfigMetadata();

        if (configMetadata == null) {
            return;
        }

        dynamicConfigFactory.addConfigChangeListener(configMetadata.getSgConfigType(), (config) -> {
            Object convertedConfig = convert(configMetadata, config);

            if (log.isDebugEnabled()) {
                log.debug("New configuration for " + module + ": " + convertedConfig);
            }

            @SuppressWarnings("rawtypes")
            Consumer consumer = configMetadata.getConfigConsumer();

            consumer.accept(convertedConfig);
        });
    }

    private <T> T convert(SearchGuardModule.SgConfigMetadata<T> configMetadata, SgDynamicConfiguration<?> value) {
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
            return configMetadata.getConfigParser().parse(subNode);
        } catch (ConfigValidationException e) {
            log.error("Error while parsing configuration in " + this + "\n" + e.getValidationErrors(), e);
            return null;
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

    public SearchGuardComponentRegistry<AuthFailureListener> getAuthFailureListeners() {
        return authFailureListeners;
    }

}
