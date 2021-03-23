package com.floragunn.searchguard.modules;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import com.fasterxml.jackson.core.JsonPointer;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchsupport.config.validation.JsonNodeParser;

public interface SearchGuardModule<T> {
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

    default SgConfigMetadata<T> getSgConfigMetadata() {
        return null;
    }
    
    default void onNodeStarted() {
        
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
}
