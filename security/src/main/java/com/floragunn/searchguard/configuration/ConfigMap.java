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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;

public class ConfigMap {

    private final Map<CType<?>, SgDynamicConfiguration<?>> map;

    private ConfigMap(Map<CType<?>, SgDynamicConfiguration<?>> map) {
        this.map = map;
    }

    public <T> SgDynamicConfiguration<T> get(CType<T> ctype) {
        @SuppressWarnings("unchecked")
        SgDynamicConfiguration<T> config = (SgDynamicConfiguration<T>) map.get(ctype);

        if (config == null) {
            return null;
        }

        if (!config.getCType().equals(ctype)) {
            throw new RuntimeException("Stored configuration does not match type: " + ctype + "; " + config);
        }

        return config;
    }

    public Collection<SgDynamicConfiguration<?>> getAll() {
        return this.map.values();
    }

    public boolean containsAll(Set<CType<?>> types) {
        return map.keySet().containsAll(types);
    }

    public ConfigMap with(ConfigMap newConfigs) {
        Map<CType<?>, SgDynamicConfiguration<?>> updatedMap = new HashMap<>(this.map);
        updatedMap.putAll(newConfigs.map);
        return new ConfigMap(Collections.unmodifiableMap(updatedMap));
    }

    public static class Builder {
        private final Map<CType<?>, SgDynamicConfiguration<?>> map = new HashMap<>();

        public <T> Builder with(SgDynamicConfiguration<T> config) {
            map.put(config.getCType(), config);

            return this;
        }

        public ConfigMap build() {
            return new ConfigMap(Collections.unmodifiableMap(this.map));
        }
    }
}
