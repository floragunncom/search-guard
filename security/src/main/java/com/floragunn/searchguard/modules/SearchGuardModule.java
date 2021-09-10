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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
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

import com.fasterxml.jackson.core.JsonPointer;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.searchguard.BaseDependencies;

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
        private final ValidatingFunction<Map<String, Object>, T> configParser;
        private final Consumer<T> configConsumer;

        public SgConfigMetadata(Class<?> sgConfigType, String entry, JsonPointer jsonPointer, ValidatingFunction<Map<String, Object>, T> configParser,
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

        public ValidatingFunction<Map<String, Object>, T> getConfigParser() {
            return configParser;
        }

        public Consumer<T> getConfigConsumer() {
            return configConsumer;
        }

    }
}
