/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.sgconf.history;

import java.util.Map;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;

public class ConfigSnapshot {
    private final Map<CType<?>, SgDynamicConfiguration<?>> configByType;
    private final ConfigVersionSet configVersions;
    private final ConfigVersionSet missingConfigVersions;

    public ConfigSnapshot(Map<CType<?>, SgDynamicConfiguration<?>> configByType) {
        this.configByType = configByType;
        this.configVersions = ConfigVersionSet.from(configByType);
        this.missingConfigVersions = ConfigVersionSet.EMPTY;
    }

    public ConfigSnapshot(Map<CType<?>, SgDynamicConfiguration<?>> configByType, ConfigVersionSet configVersionSet) {
        this.configByType = configByType;
        this.configVersions = configVersionSet;
        this.missingConfigVersions = findMissingVersions();
    }

    private ConfigVersionSet findMissingVersions() {
        ConfigVersionSet.Builder builder = new ConfigVersionSet.Builder();

        for (ConfigVersion configVersion : configVersions) {
            if (!configByType.containsKey(configVersion.getConfigurationType())) {
                builder.add(configVersion);
            }
        }

        return builder.build();
    }

    public ConfigVersionSet getConfigVersions() {
        return configVersions;
    }

    public ConfigVersionSet getMissingConfigVersions() {
        return missingConfigVersions;
    }

    public boolean hasMissingConfigVersions() {
        return missingConfigVersions.size() > 0;
    }

    public SgDynamicConfiguration<?> getConfigByType(CType<?> configType) {
        return configByType.get(configType);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T> SgDynamicConfiguration<T> getConfigByType(Class<T> configType) {
        SgDynamicConfiguration config = getConfigByType(CType.getByClass(configType));
        
        return (SgDynamicConfiguration<T>) config;
    }

    @Override
    public String toString() {
        return "ConfigSnapshot [configByType=" + configByType + ", configVersions=" + configVersions + ", missingConfigVersions="
                + missingConfigVersions + "]";
    }
}
