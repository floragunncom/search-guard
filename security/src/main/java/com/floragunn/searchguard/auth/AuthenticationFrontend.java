/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.auth;

import java.nio.file.Path;

import org.opensearch.common.settings.Settings;

import com.floragunn.codova.validation.ConfigVariableProviders;


public interface AuthenticationFrontend {
    /**
     * The type (name) of the authenticator. Only for logging.  
     * @return the type
     */
    String getType();

    public static class Context {

        private final Path configPath;
        private final Settings esSettings;
        private final ConfigVariableProviders configVariableProviders;

        public Context(Path configPath, Settings esSettings, ConfigVariableProviders configVariableProviders) {
            this.configPath = configPath;
            this.esSettings = esSettings;
            this.configVariableProviders = configVariableProviders;
        }

        public Path getConfigPath() {
            return configPath;
        }

        public Settings getEsSettings() {
            return esSettings;
        }

        public ConfigVariableProviders getConfigVariableProviders() {
            return configVariableProviders;
        }

    }
}
