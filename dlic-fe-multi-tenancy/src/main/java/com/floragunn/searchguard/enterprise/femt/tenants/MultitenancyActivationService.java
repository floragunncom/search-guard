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

package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.ConcurrentConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepException;
import com.floragunn.searchsupport.action.StandardResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.floragunn.searchguard.enterprise.femt.tenants.TenantRepository.REQUIRED_MULTI_TENANCY_ALIASES;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_OK;

public class MultitenancyActivationService {

    private static final Logger log = LogManager.getLogger(MultitenancyActivationService.class);

    private final TenantRepository tenantRepository;
    private final ConfigurationRepository configurationRepository;

    private final FeMultiTenancyConfigurationProvider configProvider;

    public MultitenancyActivationService(
        TenantRepository tenantRepository,
        ConfigurationRepository configurationRepository,
        FeMultiTenancyConfigurationProvider configProvider) {
        this.tenantRepository = Objects.requireNonNull(tenantRepository, "Tenant repository is required");
        this.configurationRepository = Objects.requireNonNull(configurationRepository, "Configuration repository is required");
        this.configProvider = Objects.requireNonNull(configProvider, "Multitenancy configuration provider is required");
    }

    public StandardResponse activate() throws StepException {
        Set<String> multiTenancyAliases = tenantRepository.findMultiTenancyRelatedAliases();
        if(!multiTenancyAliases.isEmpty()) {
            Set<String> missingAliases = findMissingAliases(multiTenancyAliases);
            if (!missingAliases.isEmpty()) {
                String message = "Cannot enable multitenancy, missing required aliases: " + String.join(", ", missingAliases);
                log.info(message);
                return new StandardResponse(SC_BAD_REQUEST, message);
            }
            extendTenantsIndexMappings();
        }
        FeMultiTenancyConfig configuration = configProvider.getConfig().orElse(FeMultiTenancyConfig.DEFAULT);
        if (configuration.isEnabled()) {
            return new StandardResponse(SC_OK, "Multitenancy is already enabled, nothing to be done");
        } else {
            return enableMultitenancy(configuration);
        }
    }

    private Set<String> findMissingAliases(Set<String> existingAliases) {
        Set<String> missingAliases = new HashSet<>(Arrays.asList(REQUIRED_MULTI_TENANCY_ALIASES));
        missingAliases.removeAll(existingAliases);
        return missingAliases;
    }

    private void extendTenantsIndexMappings() {
        tenantRepository.extendTenantsIndexMappings(getSgTenantFieldMapping());
        log.debug("Successfully extended tenants index field mappings");
    }

    public static DocNode getSgTenantFieldMapping() {
        return DocNode.of(RequestResponseTenantData.getSgTenantField(), DocNode.of("type", "keyword"));
    }

    private StandardResponse enableMultitenancy(FeMultiTenancyConfig configuration) {
        var newConfig = configuration.withEnabled(true);
        try (
            var config = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", newConfig)) {
            configurationRepository.update(FeMultiTenancyConfig.TYPE, config, null, false);
            log.info("Multitenancy has been activated");
            return new StandardResponse(SC_OK, "Multitenancy has been enabled");
        } catch (ConfigUpdateException | ConfigValidationException | ConcurrentConfigUpdateException e) {
            log.error("Cannot enable multitenancy", e);
            String message = "Cannot enable multitenancy, unexpected error occurred " + e.getMessage();
            return new StandardResponse(SC_INTERNAL_SERVER_ERROR, message);
        }
    }
}
