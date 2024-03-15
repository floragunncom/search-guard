/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchguard.configuration.validation;

import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class ConfigModificationValidator<T> {

    private final CType<T> configType;
    private ConfigMap configMap;

    protected ConfigModificationValidator(CType<T> configType, ConfigurationRepository configurationRepository) {
        this.configType = Objects.requireNonNull(configType, "configType is required");
        Objects.requireNonNull(configurationRepository, "Configuration repository is required");
        configurationRepository.subscribeOnChange(this::setConfigMap);
    }

    public abstract List<ValidationError> validateConfigs(List<SgDynamicConfiguration<?>> newConfigs);
    public abstract List<ValidationError> validateConfig(SgDynamicConfiguration<?> newConfig);
    public abstract <T> List<ValidationError> validateConfigEntry(T newConfigEntry);

    public void setConfigMap(ConfigMap configMap) {
        this.configMap = configMap;
    }

    protected <T> Optional<SgDynamicConfiguration<T>> findCurrentConfiguration(CType<T> typeToLoad) {
        return Optional.ofNullable(configMap)
                .map(confMap -> confMap.get(typeToLoad));
    }

    @SuppressWarnings("unchecked")
    protected  <T> Optional<SgDynamicConfiguration<T>> findConfigOfType(Class<T> type, List<SgDynamicConfiguration<?>> newConfigs) {
        return newConfigs.stream().filter(config -> config.getCType().getType().isAssignableFrom(type))
                .findFirst()
                .map(config -> (SgDynamicConfiguration<T>) config);
    }

    protected ValidationError toValidationError(String configEntryKey, String message) {
        String attribute = Objects.nonNull(configEntryKey)? String.format("%s.%s", configType.toLCString(), configEntryKey) : null;
        return new ValidationError(attribute, message);
    }

}
