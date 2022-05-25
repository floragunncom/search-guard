/*
 * Copyright 2021-2022 floragunn GmbH
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
import java.util.Objects;
import java.util.Set;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;

public class ConfigMap implements Destroyable {

    private final ImmutableMap<CType<?>, SgDynamicConfiguration<?>> map;
    private final String sourceIndex;

    private ConfigMap(ImmutableMap<CType<?>, SgDynamicConfiguration<?>> map, String sourceIndex) {
        this.map = map;
        this.sourceIndex = sourceIndex;
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
        return new ConfigMap(this.map.with(newConfigs.map),
                Objects.equals(this.sourceIndex, newConfigs.sourceIndex) ? this.sourceIndex : this.sourceIndex + "," + newConfigs.sourceIndex);
    }

    public ConfigMap only(Set<CType<?>> types) {
        return new ConfigMap(this.map.matching((k) -> types.contains(k)), this.sourceIndex);
    }

    public String getVersionsAsString() {
        return ImmutableMap.map(this.map, (k) -> k, (v) -> v.getDocVersion()).toString();
    }

    public ImmutableSet<CType<?>> getTypes() {
        return this.map.keySet();
    }

    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    public static class Builder {
        private ImmutableMap.Builder<CType<?>, SgDynamicConfiguration<?>> map = new ImmutableMap.Builder<>();
        private String sourceIndex;

        public Builder(String sourceIndex) {
            this.sourceIndex = sourceIndex;
        }

        public <T> Builder with(SgDynamicConfiguration<T> config) {
            map.put(config.getCType(), config);

            return this;
        }

        public ConfigMap build() {
            return new ConfigMap(this.map.build(), sourceIndex);
        }
    }

    @Override
    public void destroy() {
        for (SgDynamicConfiguration<?> config : this.map.values()) {
            config.destroy();
        }
    }

    @Override
    public String toString() {
        return map.values().toString();
    }

    public String getSourceIndex() {
        return sourceIndex;
    }
}
