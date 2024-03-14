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
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;

import java.util.ArrayList;
import java.util.List;

public class ConfigModificationValidators {

    private final List<ConfigModificationValidator<?>> validators = new ArrayList<>();

    public void register(ConfigModificationValidator<?> validators) {
        this.validators.add(validators);
    }
    public void register(List<ConfigModificationValidator<?>> validators) {
        this.validators.addAll(validators);
    }

    public List<ValidationError> validateConfigs(List<SgDynamicConfiguration<?>> newConfigs) {

        List<ValidationError> validationErrors = new ArrayList<>();

        newConfigs.forEach(config -> {
            validators.forEach(validator -> {
                validationErrors.addAll(validator.validateConfig(config));
            });
        });

        return validationErrors;
    }

    public List<ValidationError> validateConfig(SgDynamicConfiguration<?> newConfig) {

        List<ValidationError> validationErrors = new ArrayList<>();

        validators.forEach(validator -> {
            validationErrors.addAll(validator.validateConfig(newConfig));
        });

        return validationErrors;
    }

    public <T> List<ValidationError> validateConfigEntry(T newConfigEntry) {

        List<ValidationError> validationErrors = new ArrayList<>();

        validators.forEach(validator -> {
            validationErrors.addAll(validator.validateConfigEntry(newConfigEntry));
        });

        return validationErrors;
    }

}
