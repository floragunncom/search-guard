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

package com.floragunn.searchguard.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.floragunn.codova.validation.VariableResolvers;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;

public class AuthczVariableResolvers {
    private static final Logger log = LoggerFactory.getLogger(AuthczVariableResolvers.class);

    private static VariableResolvers defaultInstance;

    public static VariableResolvers get() {
        if (defaultInstance != null) {
            return defaultInstance;
        } else {
            log.error("AuthczVariableResolvers is not initialized; returning fallback", new Exception());
            return VariableResolvers.ALL_PRIVILEGED;
        }
    }

    public static void init(ConfigVarService secretsService) {
        defaultInstance = VariableResolvers.ALL_PRIVILEGED.with("var", (key) -> secretsService.get(key));
    }
}
