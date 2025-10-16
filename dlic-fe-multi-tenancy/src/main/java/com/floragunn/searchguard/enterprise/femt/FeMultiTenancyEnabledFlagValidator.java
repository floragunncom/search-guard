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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

class FeMultiTenancyEnabledFlagValidator extends ConfigModificationValidator<FeMultiTenancyConfig> {

    private static final Logger log = LogManager.getLogger(FeMultiTenancyEnabledFlagValidator.class);
    private static final String CONFIG_ENTRY_DEFAULT_KEY = "default";

    private final FeMultiTenancyConfigurationProvider feMultiTenancyConfigurationProvider;
    private final ClusterService clusterService;

    FeMultiTenancyEnabledFlagValidator(FeMultiTenancyConfigurationProvider feMultiTenancyConfigurationProvider, ClusterService clusterService, ConfigurationRepository configurationRepository) {
        super(FeMultiTenancyConfig.TYPE, configurationRepository);
        this.feMultiTenancyConfigurationProvider = Objects.requireNonNull(
                feMultiTenancyConfigurationProvider, "Fe multi tenancy configuration provider is required"
        );
        this.clusterService = Objects.requireNonNull(clusterService, "Cluster service is required");
    }

    @Override
    public List<ValidationError> validateConfigs(List<SgDynamicConfiguration<?>> newConfigs) {
        List<SgDynamicConfiguration<?>> notNullConfigs = Optional.ofNullable(newConfigs).orElse(new ArrayList<>())
                .stream().filter(Objects::nonNull).collect(Collectors.toList());

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

                FeMultiTenancyConfig currentConfig = getCurrentConfiguration();

                validateEntryEnabledFlag(null, (FeMultiTenancyConfig) newConfigEntry, currentConfig).ifPresent(errors::add);
            }

            return errors;
        }
        return Collections.emptyList();
    }

    private Optional<ValidationError> validateMultiTenancyEnabledFlag(SgDynamicConfiguration<FeMultiTenancyConfig> feMtConfig) {

        FeMultiTenancyConfig currentCfg = getCurrentConfiguration();

        return Optional.of(feMtConfig)
                .map(config -> config.getCEntry(CONFIG_ENTRY_DEFAULT_KEY))
                .flatMap(config -> validateEntryEnabledFlag(CONFIG_ENTRY_DEFAULT_KEY, config, currentCfg));
    }

    private Optional<ValidationError> validateEntryEnabledFlag(
            String configEntryKey, FeMultiTenancyConfig newConfig, FeMultiTenancyConfig currentConfig) {

        if (currentConfig.isEnabled() && (newConfig.isDisabled()) && anyKibanaIndexExists()) {
            String msg = "Cannot change the value of the 'enabled' flag to 'false'. Multitenancy cannot be disabled, please contact the support team.";
            return Optional.of(toValidationError(configEntryKey, msg));
        }
        if (currentConfig.isDisabled() && (newConfig.isEnabled()) && anyKibanaIndexExists()) {
            String msg = "You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed.";
            return Optional.of(toValidationError(configEntryKey, msg));
        } else {
            return Optional.empty();
        }
    }

    private boolean anyKibanaIndexExists() {
        String kibanaIndexNamePrefix = Objects.requireNonNullElse(
                feMultiTenancyConfigurationProvider.getKibanaIndex(), FeMultiTenancyConfig.DEFAULT.getIndex()
        );
        return clusterService.state().metadata().getProject().getIndicesLookup().keySet()
                .stream().anyMatch(name -> name.startsWith(kibanaIndexNamePrefix));
    }


    private FeMultiTenancyConfig getCurrentConfiguration() {
        return findCurrentConfiguration(FeMultiTenancyConfig.TYPE)
                .map(currentConfig -> currentConfig.getCEntry("default"))
                .orElseGet(() -> {
                    log.warn("{} config is unavailable, default config will be used instead", FeMultiTenancyConfig.TYPE.getName());
                    return FeMultiTenancyConfig.DEFAULT;
                });
    }
}
