/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_RESOLVE_INDEX_BY_ALIAS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.GLOBAL_AND_PRIVATE_TENANT_CONFLICT_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.GLOBAL_TENANT_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INDICES_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MULTI_TENANCY_DISABLED_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static java.util.Objects.requireNonNull;

class PopulateTenantsStep implements MigrationStep {

    private static final Logger log = LogManager.getLogger(PopulateTenantsStep.class);
    private final FeMultiTenancyConfigurationProvider configurationProvider;
    private final StepRepository repository;

    public PopulateTenantsStep(FeMultiTenancyConfigurationProvider configurationProvider, StepRepository repository) {
        this.configurationProvider = requireNonNull(configurationProvider, "Multi-tenancy configuration provider is required");
        this.repository = requireNonNull(repository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext dataMigrationContext) {
        ImmutableSet<String> configuredTenants = configurationProvider.getTenantNames();
        Optional<FeMultiTenancyConfig> optionalConfig = configurationProvider.getConfig();
        return optionalConfig.map(config -> executeWithConfig(config, dataMigrationContext, configuredTenants)) //
            .orElse(new StepResult(MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR, "Cannot load SearchGuard multi tenancy config."));
    }

    private StepResult executeWithConfig(FeMultiTenancyConfig config, DataMigrationContext dataMigrationContext,
        ImmutableSet<String> configuredTenants) {
        log.debug("Searching for tenants, provided configuration '{}' and tenant names '{}'.", config, configuredTenants);
        if(!config.isEnabled()) {
            String details = "Current configuration " + config;
            String message = "Frontend multi-tenancy is not enabled.";
            return new StepResult(MULTI_TENANCY_DISABLED_ERROR, message, details);
        }
        List<TenantAlias> configuredTenantAliases = configuredTenants.stream() //
            .filter(name -> !Tenant.GLOBAL_TENANT_ID.equals(name)) //
            .sorted() //
            .map(name -> new TenantAlias(toInternalIndexName(config, name), name))
            .collect(Collectors.toList());
        GetIndexResponse allExistingIndices = repository.findAllIndicesIncludingHidden();
        log.debug("Tenants found in configuration '{}'.", configuredTenantAliases);
        List<TenantAlias> globalTenantIndexName = extractGlobalTenantIndexName(config.getIndex(), allExistingIndices, config);
        List<TenantIndex> tenants = Stream.concat(globalTenantIndexName.stream(), configuredTenantAliases.stream()) //
            .map(this::resolveIndexAlias) //
            .flatMap(Optional::stream) //
            .collect(Collectors.toList());
        log.debug("Tenants read from configuration with index names resolved: '{}'", tenants);
        Set<String> tenantIndices = tenants.stream().map(TenantIndex::indexName).collect(Collectors.toSet());
        ImmutableList<TenantIndex> privateTenants = findPrivateTenants(allExistingIndices, config.getIndex(), tenantIndices);
        tenants.addAll(privateTenants);
        if(tenants.isEmpty()) {
            return new StepResult(INDICES_NOT_FOUND_ERROR, "Indices related to front-end multi tenancy not found.");
        }
        List<TenantIndex> globalTenants = tenants.stream().filter(TenantIndex::belongsToGlobalTenant).toList();
        if(globalTenants.size() != 1) {
            String message = "Definition of exactly one global tenant is expected, but found '" + globalTenants.size() + "'. "
                + "Please verify that index '" + globalTenantIndexName + "' exists.";
            String globalTenantsString = globalTenants.stream().map(Object::toString).collect(Collectors.joining(", "));
            return new StepResult(GLOBAL_TENANT_NOT_FOUND_ERROR, message, "List of global tenants: " + globalTenantsString);
        }
        dataMigrationContext.setTenantIndices(ImmutableList.of(tenants));
        String stringTenantList = tenants.stream() //
            .map(data -> "tenant " + (data.belongsToUserPrivateTenant() ? "__user__" : data.tenantName()) + " -> " + data.indexName()) //
            .collect(Collectors.joining(", "));
        String details = "Tenants found for migration: " + stringTenantList;
        String message = "Populates " +  tenants.size() + " tenants' data for migration";
        return new StepResult(OK, message, details);
    }

    private static List<TenantAlias> extractGlobalTenantIndexName(String configuredFrontendIndexName, GetIndexResponse allExistingIndices, FeMultiTenancyConfig config) {
        var globalTenantIndexNamePattern = Pattern.compile(Pattern.quote(configuredFrontendIndexName) + "_8\\.7\\.\\d+_\\d{3}\\b");
        return allExistingIndices.aliases()//
                .entrySet()//
                .stream() //
                .filter(entry -> globalTenantIndexNamePattern.matcher(entry.getKey()).matches()) //
                .filter(entry -> entry.getValue().stream().map(alias -> alias.getAlias()).collect(Collectors.toSet()).contains(configuredFrontendIndexName)) //
                .map(entry -> entry.getKey()) //
                .map(indexName -> new TenantAlias(indexName, Tenant.GLOBAL_TENANT_ID)) //
                .toList();
    }

    private Optional<TenantIndex> resolveIndexAlias(TenantAlias tenantAlias) {
        return getIndexNameByAliasName(tenantAlias.aliasName()) //
            .map(indexName -> new TenantIndex(indexName, tenantAlias.tenantName()));
    }

    @Override
    public String name() {
        return "Populate tenants";
    }

    private String toInternalIndexName(FeMultiTenancyConfig config, String tenant) {
        if (tenant == null) {
            throw new ElasticsearchException("tenant must not be null here");
        }
        String tenantInfoPart = "_" + TenantManager.toInternalTenantName(tenant);
        String prefix = config.getIndex();
        StringBuilder result = new StringBuilder(prefix).append(tenantInfoPart);
        return result.toString();
    }

    private Optional<String> getIndexNameByAliasName(String aliasName) {
        Optional<GetIndexResponse> indexResponse = repository.findIndexByNameOrAlias(aliasName);
        if(indexResponse.isEmpty()) {
            log.warn("Index '{}' with front-end data does not exist, its data will be not used during migration.", aliasName);
        }
        return indexResponse.map(response -> {
            String[] indices = response.getIndices();
            if((indices == null) || (indices.length != 1)) {
                String message = "Alias " + aliasName + " should be associated with exactly one index";
                String details = "Alias " + aliasName + " is associated with indices " + Optional.ofNullable(indices)
                    .stream()
                    .flatMap(Arrays::stream) //
                    .map(indexName -> "'" + indexName + "'") //
                    .collect(Collectors.joining(", "));
                throw new StepException(message, CANNOT_RESOLVE_INDEX_BY_ALIAS_ERROR, details);
            }
            String indexName = indices[0];
            log.debug("Alias '{}' is related to index '{}'.", aliasName, indexName);
            return indexName;
        });
    }

    private ImmutableList<TenantIndex> findPrivateTenants(GetIndexResponse allExistingIndices, String validIndexPrefix, Set<String> alreadyFoundIndices) {
        Pattern pattern = Pattern.compile(Pattern.quote(validIndexPrefix) + "_-?\\d+_[a-z0-9]+\\b");
        List<TenantIndex> privateTenants = allExistingIndices.aliases()
            .entrySet()
            .stream()
            .filter(entry -> ! alreadyFoundIndices.contains(entry.getKey()))
            .peek(entry -> log.trace("Checking if index {} is private user tenant index", entry.getKey()))
            .filter(entry -> isPrivateUserTenantIndex(pattern, entry))
            .map(entry -> new TenantIndex(entry.getKey(), null))
            .collect(Collectors.toList());
        log.debug("Private tenant related index found: '{}'", privateTenants);
        List<String> privateTenantsInConflictWithGlobalTenant = privateTenants.stream() //
                .map(TenantIndex::indexName) //
                .filter(name -> name.contains("_sgsglobaltenant_")) //
                .toList();
        if(!privateTenantsInConflictWithGlobalTenant.isEmpty()) {
            String message = "Found user private tenant which is in name conflict with the global tenant";
            String details = "Indices " + privateTenantsInConflictWithGlobalTenant.stream() //
                .map(name -> "'" + name + "'") //
                .collect(Collectors.joining(", "));
            throw new StepException(message, GLOBAL_AND_PRIVATE_TENANT_CONFLICT_ERROR, details);
        }
        return ImmutableList.of(privateTenants);
    }

    private boolean isPrivateUserTenantIndex(Pattern aliasNamePattern, Map.Entry<String, List<AliasMetadata>> entry) {
        for(AliasMetadata aliasMetadata : entry.getValue()) {
            String currentAlias = aliasMetadata.alias();
            if(aliasNamePattern.matcher(currentAlias).matches()) {
                return true;
            }
        }
        return false;
    }
}
