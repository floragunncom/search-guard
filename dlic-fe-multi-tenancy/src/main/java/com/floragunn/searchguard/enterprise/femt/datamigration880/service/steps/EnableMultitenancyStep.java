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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.ConcurrentConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static java.util.Objects.requireNonNull;

class EnableMultitenancyStep implements MigrationStep {

    private static final Logger log = LogManager.getLogger(EnableMultitenancyStep.class);

    private final FeMultiTenancyConfigurationProvider configurationProvider;

    private final ConfigurationRepository configRepository;

    public EnableMultitenancyStep(FeMultiTenancyConfigurationProvider configurationProvider, ConfigurationRepository configRepository) {
        this.configurationProvider = requireNonNull(configurationProvider, "Configuration provider is required");
        this.configRepository = requireNonNull(configRepository, "Configuration repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext dataMigrationContext) throws StepException {
        FeMultiTenancyConfig configuration = configurationProvider.getConfig().orElse(FeMultiTenancyConfig.DEFAULT);
        if (configuration.isEnabled()) {
            return new StepResult(OK, "Multitenancy is already enabled", "Nothing to be done");
        } else {
            return enableMultitenancy(configuration);
        }
    }

    private StepResult enableMultitenancy(FeMultiTenancyConfig configuration) {
        var newConfig = configuration.withEnabled(true);
        try (
            var config = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", newConfig)) {
            configRepository.update(FeMultiTenancyConfig.TYPE, config, null, false);
            return new StepResult(OK, "Multitenancy has been enabled.", "New configuration " + newConfig);
        } catch (ConfigUpdateException | ConfigValidationException | ConcurrentConfigUpdateException e) {
            log.error("Cannot enable multitenancy", e);
            throw new StepException("Cannot enable multitenancy", StepExecutionStatus.CANNOT_ENABLE_MULTITENANCY, e.getMessage());
        }
    }

    @Override
    public String name() {
        return "Enable multitenancy";
    }
}
