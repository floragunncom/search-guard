package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantData;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.FAILURE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.SUCCESS;
import static java.util.Objects.requireNonNull;

class PopulateTenantsStep implements MigrationStep {

    private static final Logger log = LogManager.getLogger(PopulateTenantsStep.class);

    private final MultiTenancyConfigurationProvider configurationProvider;
    private final PrivilegedConfigClient client;

    public PopulateTenantsStep(MultiTenancyConfigurationProvider configurationProvider, PrivilegedConfigClient client) {
        this.configurationProvider = requireNonNull(configurationProvider, "Multi-tenancy configuration provider is required");
        this.client = requireNonNull(client, "Privileged client is required");
    }

    @Override
    public StepResult execute(DataMigrationContext dataMigrationContext) {
        ImmutableSet<String> configuredTenants = configurationProvider.getTenantNames();
        return configurationProvider.getConfig() //
            .map(config -> executeWithConfig(config, dataMigrationContext, configuredTenants)) //
            .orElse(new StepResult(FAILURE, "Cannot retrieve multi tenancy configuration"));
    }

    public StepResult executeWithConfig(FeMultiTenancyConfig config, DataMigrationContext dataMigrationContext,
        ImmutableSet<String> configuredTenants) {
        log.debug("Searching for tenants, provided configuration '{}' and tenant names '{}'.", config, configuredTenants);
        if(!config.isEnabled()) {
            return new StepResult(FAILURE, "Frontend multi-tenancy is not enabled.", "Current configuration " + config);
        }
        Stream<TenantData> configuredTenantDataStream = configuredTenants.stream() //
            .filter(name -> !Tenant.GLOBAL_TENANT_ID.equals(name)) //
            .sorted() //
            .map(name -> new TenantData(toInternalIndexName(config, name), name));// here we have an index alias
        TenantData globalTenant = new TenantData(".kibana_8.7.0", Tenant.GLOBAL_TENANT_ID);
        List<TenantData> tenants = Stream.concat(Stream.of(globalTenant), configuredTenantDataStream) //
            .map(this::resolveIndexAlias) //
            .filter(Objects::nonNull) //
            .collect(Collectors.toUnmodifiableList());
        log.debug("Tenants read from configuration: '{}'", tenants);
        Set<String> tenantIndices = tenants.stream().map(tenant -> tenant.indexName()).collect(Collectors.toSet());
        ImmutableList<TenantData> privateTenants = findPrivateTenants(tenantIndices);
        tenants.addAll(privateTenants);
        if(tenants.isEmpty()) {
            return new StepResult(FAILURE, "Indices related to front-end multi tenancy not found.");
        }
        List<TenantData> globalTenants = tenants.stream().filter(TenantData::isGlobal).collect(Collectors.toList());
        if(globalTenants.size() != 1) {
            String message = "Definition of exactly one global tenant is expected, but found " + globalTenants.size();
            String globalTenantsString = globalTenants.stream().map(Object::toString).collect(Collectors.joining(", "));
            return new StepResult(FAILURE, message, "List of global tenants: " + globalTenantsString);
            // TODO check if global tenant exist
            // TODO what to do when the global tenant does not exists
        }
        dataMigrationContext.setTenants(ImmutableList.of(tenants));
        String stringTenantList = tenants.stream() //
            .map(data -> "tenant " + data.tenantName() + " -> " + data.indexName()) //
            .collect(Collectors.joining(", "));
        String details = "Tenants found for migration: " + stringTenantList;
        String message = "Populates " +  tenants.size() + " tenants' data for migration";
        return new StepResult(SUCCESS, message, details);
    }

    private TenantData resolveIndexAlias(TenantData tenantData) {
        return getIndexNameByAliasName(tenantData.indexName()) //
            .map(indexName -> new TenantData(indexName, tenantData.tenantName())) //
            .orElse(null);
    }

    @Override
    public String name() {
        return "Populate tenants";
    }

    private String toInternalIndexName(FeMultiTenancyConfig config, String tenant) {
        if (tenant == null) {
            throw new ElasticsearchException("tenant must not be null here");
        }
        String tenantInfoPart = "_" + tenant.hashCode() + "_" + tenant.toLowerCase().replaceAll("[^a-z0-9]+", "");
        String prefix = config.getIndex();
        StringBuilder result = new StringBuilder(prefix).append(tenantInfoPart);
        return result.toString();
    }

    public Optional<String> getIndexNameByAliasName(String aliasName) {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(aliasName);
        try {
            GetIndexResponse response = client.admin().indices().getIndex(request).actionGet();
            String indexName = response.getIndices()[0];
            log.debug("Alias '{}' is related to index '{}'.", aliasName, indexName);
            return Optional.ofNullable(indexName);
        } catch (IndexNotFoundException ex) {
            // TODO maybe this should be treated as error
            log.debug("Cannot find index by alias '{}'", aliasName);
            return Optional.empty();
        }
    }

    private ImmutableList<TenantData> findPrivateTenants(Set<String> alreadyFoundIndices) {
        GetIndexResponse indices = client.admin().indices().getIndex(new GetIndexRequest().indices("*")).actionGet();
        // TODO finish implementation
        return ImmutableList.empty();
    }
}
