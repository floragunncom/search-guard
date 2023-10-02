package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_LOAD_INDEX_MAPPINGS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.EMPTY_MAPPINGS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.NO_SOURCE_INDEX_SETTINGS_ERROR;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;

class IndexSettingsDuplicator {

    static final String DUPLICATE_MARKER = "sg_data_migrated_to_8_8_0";
    static final String MAPPINGS_PROPERTIES = "properties";

    private static final Logger log = LogManager.getLogger(IndexSettingsDuplicator.class);

    private final StepRepository stepRepository;

    public IndexSettingsDuplicator(StepRepository stepRepository) {
        this.stepRepository = Objects.requireNonNull(stepRepository, "Repository is required");
    }

    public BasicIndexSettings createIndexWithDuplicatedSettings(String indexSettingsSource, String newIndexForCreation,
        boolean insertDuplicateMarker) {
        Strings.requireNonEmpty(indexSettingsSource, "Index name setting source is required");
        Strings.requireNonEmpty(newIndexForCreation, "New index name for creation is required");
        GetSettingsResponse settingsResponse = stepRepository.getIndexSettings(indexSettingsSource);
        Settings settings = extractSettings(settingsResponse, indexSettingsSource);
        int numberOfShards = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        int numberOfReplicas = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        long mappingsTotalFieldsLimit = settings.getAsLong(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1500L);
        Map<String, Object> mappingSources = loadMappingSources(indexSettingsSource);
        if(log.isInfoEnabled()) {
            String mappingsString = DocNode.wrap(mappingSources).toJsonString();
            log.info("Index '{}' has '{}' shards, '{}' replicas and mapping fields limit '{}', mappings defined for index '{}'",
                indexSettingsSource,
                numberOfShards,
                numberOfReplicas,
                mappingsTotalFieldsLimit,
                mappingsString);
        }
        if(insertDuplicateMarker) {
            extendMappingsWithMigrationMarker(mappingSources);
        }
        String mappingsString = DocNode.wrap(mappingSources).toJsonString();
        log.debug("Final mappings in newly created index '{}'.", mappingsString);
        stepRepository.createIndex(newIndexForCreation, numberOfShards, numberOfReplicas, mappingsTotalFieldsLimit, mappingSources);
        return new BasicIndexSettings(numberOfShards, numberOfReplicas, mappingsTotalFieldsLimit, mappingsString);
    }

    public boolean isDuplicate(String indexName) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        Map<String, Object> mappings = Optional.ofNullable(stepRepository.findIndexMappings(indexName)) //
            .map(GetMappingsResponse::getMappings) //
            .map(mappingsMap -> mappingsMap.get(indexName)) //
            .map(MappingMetadata::getSourceAsMap)
            .orElseThrow(() -> new StepException("Cannot load index mappings", CANNOT_LOAD_INDEX_MAPPINGS_ERROR,
                "Index '" + indexName + "'"));
        log.info("Index '{}' has defined mappings '{}'.", indexName, mappings);
        Map<String, Object> properties = (Map<String, Object>) mappings.get(MAPPINGS_PROPERTIES);
        if(Objects.isNull(properties)) {
            String details = "Is index '" + indexName + "' empty?";
            throw new StepException("Mappings for the index are not defined", EMPTY_MAPPINGS_ERROR, details);
        }
        return properties.containsKey(DUPLICATE_MARKER);
    }

    public void markAsDuplicate(String indexName) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        HashMap<String, Object> sources = new HashMap<>();
        extendMappingsWithMigrationMarker(sources);
        stepRepository.updateMappings(indexName, sources);
    }

    private static Settings extractSettings(GetSettingsResponse response, String indexName) {
        return Optional.ofNullable(response) //
            .map(GetSettingsResponse::getIndexToSettings) //
            .map(settingsMap -> settingsMap.get(indexName)) //
            .orElseThrow(() -> new StepException(
                "Cannot find index settings", NO_SOURCE_INDEX_SETTINGS_ERROR,
                "Cannot load index '" + indexName + "' settings"));
    }

    private Map<String, Object> loadMappingSources(String indexName) {
        GetMappingsResponse indexMappingsResponse = stepRepository.findIndexMappings(indexName);
        return Optional.ofNullable(indexMappingsResponse) //
            .map(GetMappingsResponse::getMappings) //
            .map(mappingsMap -> mappingsMap.get(indexName)) //
            .map(MappingMetadata::getSourceAsMap) //
            .orElseThrow(() -> new StepException("Cannot load mappings for index " + indexName, CANNOT_LOAD_INDEX_MAPPINGS_ERROR, null));
    }

    private static void extendMappingsWithMigrationMarker(Map<String, Object> mappingSources) {
        if(!mappingSources.containsKey(MAPPINGS_PROPERTIES)) {
            mappingSources.put(MAPPINGS_PROPERTIES, new HashMap<>());
        }
        Map<String, Object> mappingProperties = (Map<String, Object>) mappingSources.get(MAPPINGS_PROPERTIES);
        mappingProperties.put(DUPLICATE_MARKER, ImmutableMap.of("type", "boolean"));
    }

    public record BasicIndexSettings(int numberOfShards, int numberOfReplicas, long mappingsTotalFieldsLimit, String mappings) {

    }
}
