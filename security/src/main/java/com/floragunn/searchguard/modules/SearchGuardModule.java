package com.floragunn.searchguard.modules;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import com.fasterxml.jackson.core.JsonPointer;
import com.floragunn.searchguard.SearchGuardPlugin.ProtectedIndices;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchsupport.config.validation.JsonNodeParser;

public interface SearchGuardModule<T> {
    default List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
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

    default SgConfigMetadata<T> getSgConfigMetadata() {
        return null;
    }

    public class SgConfigMetadata<T> {
        private final Class<?> sgConfigType;
        private final String entry;
        private final JsonPointer jsonPointer;
        private final JsonNodeParser<T> configParser;
        private final Consumer<T> configConsumer;

        public SgConfigMetadata(Class<?> sgConfigType, String entry, JsonPointer jsonPointer, JsonNodeParser<T> configParser,
                Consumer<T> configConsumer) {
            super();
            this.sgConfigType = sgConfigType;
            this.entry = entry;
            this.jsonPointer = jsonPointer;
            this.configParser = configParser;
            this.configConsumer = configConsumer;
        }

        public Class<?> getSgConfigType() {
            return sgConfigType;
        }

        public String getEntry() {
            return entry;
        }

        public JsonPointer getJsonPointer() {
            return jsonPointer;
        }

        public JsonNodeParser<T> getConfigParser() {
            return configParser;
        }

        public Consumer<T> getConfigConsumer() {
            return configConsumer;
        }

    }

    public class BaseDependencies {

        private final Settings settings;
        private final Client localClient;
        private final ClusterService clusterService;
        private final ThreadPool threadPool;
        private final ResourceWatcherService resourceWatcherService;
        private final ScriptService scriptService;
        private final NamedXContentRegistry xContentRegistry;
        private final Environment environment;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final DynamicConfigFactory dynamicConfigFactory;
        private final ConfigurationRepository configurationRepository;
        private final ProtectedIndices protectedIndices;
        private final SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry;

        public BaseDependencies(Settings settings, Client localClient, ClusterService clusterService, ThreadPool threadPool,
                ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
                Environment environment, IndexNameExpressionResolver indexNameExpressionResolver, DynamicConfigFactory dynamicConfigFactory,
                ConfigurationRepository configurationRepository, ProtectedIndices protectedIndices,
                SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry) {
            super();
            this.settings = settings;
            this.localClient = localClient;
            this.clusterService = clusterService;
            this.threadPool = threadPool;
            this.resourceWatcherService = resourceWatcherService;
            this.scriptService = scriptService;
            this.xContentRegistry = xContentRegistry;
            this.environment = environment;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
            this.dynamicConfigFactory = dynamicConfigFactory;
            this.configurationRepository = configurationRepository;
            this.protectedIndices = protectedIndices;
            this.specialPrivilegesEvaluationContextProviderRegistry = specialPrivilegesEvaluationContextProviderRegistry;
        }

        public Settings getSettings() {
            return settings;
        }

        public Client getLocalClient() {
            return localClient;
        }

        public ClusterService getClusterService() {
            return clusterService;
        }

        public ThreadPool getThreadPool() {
            return threadPool;
        }

        public ResourceWatcherService getResourceWatcherService() {
            return resourceWatcherService;
        }

        public ScriptService getScriptService() {
            return scriptService;
        }

        public NamedXContentRegistry getxContentRegistry() {
            return xContentRegistry;
        }

        public Environment getEnvironment() {
            return environment;
        }

        public IndexNameExpressionResolver getIndexNameExpressionResolver() {
            return indexNameExpressionResolver;
        }

        public DynamicConfigFactory getDynamicConfigFactory() {
            return dynamicConfigFactory;
        }

        public ConfigurationRepository getConfigurationRepository() {
            return configurationRepository;
        }

        public ProtectedIndices getProtectedIndices() {
            return protectedIndices;
        }

        public SpecialPrivilegesEvaluationContextProviderRegistry getSpecialPrivilegesEvaluationContextProviderRegistry() {
            return specialPrivilegesEvaluationContextProviderRegistry;
        }

    }
}
