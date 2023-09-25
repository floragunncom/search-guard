package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_LOAD_INDEX_MAPPINGS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.NO_GLOBAL_TENANT_SETTINGS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;

class CreateTempIndexStep implements MigrationStep {

    private static final Logger log = LogManager.getLogger(CreateTempIndexStep.class);
    public static final String MIGRATED_DATA_MARKER = "sg_data_migrated_to_8_8_0";
    public static final String MAPPINGS_PROPERTIES = "properties";

    private final StepRepository stepRepository;

    public CreateTempIndexStep(StepRepository stepRepository) {
        this.stepRepository = Objects.requireNonNull(stepRepository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String globalTenantIndexName = context.getGlobalTenantIndexName();
        GetSettingsResponse settingsResponse = stepRepository.getIndexSettings(globalTenantIndexName);
        Settings settings = extractSettings(settingsResponse, globalTenantIndexName);
        int numberOfShards = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        int numberOfReplicas = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        long mappingsTotalFieldsLimit = settings.getAsLong(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1500L);
        Map<String, Object> mappingSources = loadMappingSources(globalTenantIndexName);
        if(log.isInfoEnabled()) {
            String mappingsString = DocNode.wrap(mappingSources).toJsonString();
            log.info("Index '{}' has '{}' shards, '{}' replicas and mapping fields limit '{}', mappings defined for index '{}'",
                globalTenantIndexName,
                numberOfShards,
                numberOfReplicas,
                mappingsTotalFieldsLimit,
                mappingsString);
        }
        extendMappingsWithMigrationMarker(mappingSources);
        String mappingsString = DocNode.wrap(mappingSources).toJsonString();
        log.debug("Mappings extended with migration data marker '{}'.", mappingsString);
        String tempIndexName = context.getTempIndexName();
        stepRepository.createIndex(tempIndexName, numberOfShards, numberOfReplicas, mappingsTotalFieldsLimit, mappingSources);
        String message = "Temporary index '" + tempIndexName + "' created with " + numberOfShards + " shards, replicas "
            + numberOfReplicas + " and total mapping fields limit " + mappingsTotalFieldsLimit;
        String details = "Temp index mappings " + mappingsString;
        return new StepResult(OK, message, details);
    }

    private static void extendMappingsWithMigrationMarker(Map<String, Object> mappingSources) {
        if(!mappingSources.containsKey(MAPPINGS_PROPERTIES)) {
            mappingSources.put(MAPPINGS_PROPERTIES, new HashMap<>());
        }
        Map<String, Object> mappingProperties = (Map<String, Object>) mappingSources.get(MAPPINGS_PROPERTIES);
        mappingProperties.put(MIGRATED_DATA_MARKER, ImmutableMap.of("type", "boolean"));
    }

    private Map<String, Object> loadMappingSources(String indexName) {
        GetMappingsResponse indexMappingsResponse = stepRepository.findIndexMappings(indexName);
        return Optional.ofNullable(indexMappingsResponse) //
            .map(GetMappingsResponse::getMappings) //
            .map(mappingsMap -> mappingsMap.get(indexName)) //
            .map(MappingMetadata::getSourceAsMap) //
            .orElseThrow(() -> new StepException("Cannot load mappings for index " + indexName, CANNOT_LOAD_INDEX_MAPPINGS_ERROR, null));
    }

    private static Settings extractSettings(GetSettingsResponse response, String indexName) {
        return Optional.ofNullable(response) //
            .map(GetSettingsResponse::getIndexToSettings) //
            .map(settingsMap -> settingsMap.get(indexName)) //
            .orElseThrow(() -> new StepException(
                "Cannot find global tenant index settings", NO_GLOBAL_TENANT_SETTINGS_ERROR,
                "Cannot load index '" + indexName + "' settings"));
    }

    @Override
    public String name() {
        return "create temp index";
    }
}
