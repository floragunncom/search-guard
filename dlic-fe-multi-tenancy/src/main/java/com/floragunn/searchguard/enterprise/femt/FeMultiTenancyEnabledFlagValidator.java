/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.configuration.validation.ConfigModificationValidator;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

class FeMultiTenancyEnabledFlagValidator extends ConfigModificationValidator<FeMultiTenancyConfig> {

    private static final String CONFIG_ENTRY_DEFAULT_KEY = "default";

    FeMultiTenancyEnabledFlagValidator(ConfigurationRepository configurationRepository) {
        super(FeMultiTenancyConfig.TYPE, configurationRepository);
    }

    @Override
    public List<ValidationError> validateConfigs(List<SgDynamicConfiguration<?>> newConfigs) {
        List<SgDynamicConfiguration<?>> notNullConfigs = newConfigs.stream().filter(Objects::nonNull).collect(Collectors.toList());

        List<ValidationError> errors = new ArrayList<>();

        Optional<SgDynamicConfiguration<FeMultiTenancyConfig>> newFeMtConfig = findConfigOfType(FeMultiTenancyConfig.class, notNullConfigs);
        newFeMtConfig.flatMap(this::validateMultiTenancyEnabledFlag).ifPresent(errors::add);

        return errors;
    }

    @Override
    public List<ValidationError> validateConfig(SgDynamicConfiguration<?> newConfig) {
        return validateConfigs(Collections.singletonList(newConfig));
    }

    @Override
    public <T> List<ValidationError> validateConfigEntry(T newConfigEntry) {
        if (Objects.nonNull(newConfigEntry)) {
            List<ValidationError> errors = new ArrayList<>();

            if (FeMultiTenancyConfig.class.isAssignableFrom(newConfigEntry.getClass())) {

                FeMultiTenancyConfig currentConfig = findCurrentConfiguration(FeMultiTenancyConfig.TYPE)
                        .map(currentCfg -> currentCfg.getCEntry("default"))
                        .orElse(FeMultiTenancyConfig.DEFAULT);

                validateEntryEnabledFlag(null, (FeMultiTenancyConfig) newConfigEntry, currentConfig);
            }

            return errors;
        }
        return Collections.emptyList();
    }

    private Optional<ValidationError> validateMultiTenancyEnabledFlag(SgDynamicConfiguration<FeMultiTenancyConfig> feMtConfig) {

        FeMultiTenancyConfig currentCfg = findCurrentConfiguration(FeMultiTenancyConfig.TYPE)
                .map(currentConfig -> currentConfig.getCEntry("default"))
                .orElse(FeMultiTenancyConfig.DEFAULT);

        return Optional.of(feMtConfig)
                .filter(config -> FeMultiTenancyConfig.class.isAssignableFrom(config.getImplementingClass()))
                .map(config -> config.getCEntry(CONFIG_ENTRY_DEFAULT_KEY))
                .flatMap(config -> validateEntryEnabledFlag(CONFIG_ENTRY_DEFAULT_KEY, config, currentCfg));
    }

    private Optional<ValidationError> validateEntryEnabledFlag(
            String configEntryKey, FeMultiTenancyConfig newConfig, FeMultiTenancyConfig currentConfig) {

        if (currentConfig.isEnabled() != newConfig.isEnabled() && true) { //todo && any .kibana* index exists
            String msg = String.format(
                    "Cannot change value of `enabled` flag to %s. It may cause data loss since some Kibana indices exist.",
                    newConfig.isEnabled()
            );
            return Optional.of(toValidationError(configEntryKey, msg));
        } else {
            return Optional.empty();
        }
    }
}
