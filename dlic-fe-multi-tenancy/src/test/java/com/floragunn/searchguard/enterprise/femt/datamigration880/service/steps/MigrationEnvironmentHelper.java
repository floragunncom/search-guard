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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.IndexNameDataFormatter;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.StaticSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.rules.ExternalResource;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.floragunn.searchguard.support.PrivilegedConfigClient.adapt;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.Strings.requireNonEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class MigrationEnvironmentHelper extends ExternalResource {

    private static final Logger log = LogManager.getLogger(MigrationEnvironmentHelper.class);

    public static final String INDEX_TEMPLATE_NAME = "kibana_test_indices_template";

    public static final String TENANT_MANAGEMENT = "management";

    public static final String MULTITENANCY_INDEX_PREFIX = ".kibana";
    public static final DoubleAliasIndex
        GLOBAL_TENANT_INDEX = new DoubleAliasIndex(".kibana_8.7.0_001", ".kibana_8.7.0",
        MULTITENANCY_INDEX_PREFIX);

    public static final String CREATE_INDEX_ID = "CREATE_INDEX_ID";
    public static final DoubleAliasIndex
        PRIVATE_USER_KIRK_INDEX = new DoubleAliasIndex(".kibana_3292183_kirk_8.7.0_001", ".kibana_3292183_kirk_8.7.0", ".kibana_3292183_kirk" );// kirk
    public static final DoubleAliasIndex
        PRIVATE_USER_LUKASZ_1_INDEX = new DoubleAliasIndex(".kibana_-1091682490_lukasz_8.7.0_001", ".kibana_-1091682490_lukasz_8.7.0", ".kibana_-1091682490_lukasz"); //lukasz
    public static final DoubleAliasIndex
        PRIVATE_USER_LUKASZ_2_INDEX = new DoubleAliasIndex(".kibana_739988528_ukasz_8.7.0_001", ".kibana_739988528_ukasz_8.7.0", ".kibana_739988528_ukasz"); //łukasz
    public static final DoubleAliasIndex
        PRIVATE_USER_LUKASZ_3_INDEX = new DoubleAliasIndex(".kibana_-1091714203_luksz_8.7.0_001", ".kibana_-1091714203_luksz_8.7.0", ".kibana_-1091714203_luksz");//luk@sz

    private final LocalCluster.Embedded cluster;
    private final Clock clock;

    private final List<DeletableIndex> createdIndices = new ArrayList<>();

    private PrivilegedConfigClient privilegedConfigClient;

    public MigrationEnvironmentHelper(LocalCluster.Embedded cluster, Clock clock) {
        this.cluster = Objects.requireNonNull(cluster, "Local cluster is required");
        this.clock =Objects.requireNonNull(clock, "Clock is required");
    }

    @Override
    protected void after() {
        deleteIndex(createdIndices.toArray(DeletableIndex[]::new));
        createdIndices.clear();
        findAndDeleteBackupIndices();
        DataMigrationContext context = new DataMigrationContext(new MigrationConfig(false), clock);
        if(isIndexCreated(context.getTempIndexName())) {
            deleteIndex(context::getTempIndexName);
        }
        deleteIndexTemplateIfExists();
    }

    static String tenantNameToIndexName(String indexNamePrefix, String tenantName) {
        return indexNamePrefix + "_" + tenantName.hashCode() + "_" + tenantName.toLowerCase().replaceAll("[^a-z0-9]+", "");
    }

    public void addCreatedIndex(DeletableIndex indexName) {
        this.createdIndices.add(indexName);
    }

    private void deleteIndexTemplateIfExists() {
        Client client = cluster.getInternalNodeClient();
        GetIndexTemplatesResponse response = client.admin().indices().prepareGetTemplates(StaticSettings.DEFAULT_MASTER_TIMEOUT, INDEX_TEMPLATE_NAME).execute().actionGet();
        if(!response.getIndexTemplates().isEmpty()) {
            var acknowledgedResponse = client.admin().indices().prepareDeleteTemplate(INDEX_TEMPLATE_NAME).execute().actionGet();
            assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
        }
    }

    public boolean isIndexCreated(String...indexOrAlias) {
        try {
            Client client = cluster.getInternalNodeClient();
            for(String index : indexOrAlias) {
                client.admin().indices().getIndex(new GetIndexRequest(StaticSettings.DEFAULT_MASTER_TIMEOUT).indices(index)).actionGet();
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void deleteIndex(DeletableIndex...deletableIndices) {
        if(deletableIndices.length == 0) {
            return;
        }
        String[] indicesForDeletion = Arrays.stream(deletableIndices).map(DeletableIndex::indexForDeletion).toArray(String[]::new);
        DeleteIndexRequest request = new DeleteIndexRequest(indicesForDeletion);
        Client client = cluster.getInternalNodeClient();
        AcknowledgedResponse acknowledgedResponse = client.admin().indices().delete(request).actionGet();
        assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
    }

    public List<DoubleAliasIndex> getIndicesForConfiguredTenantsWithoutGlobal() {
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        return configurationProvider.getTenantNames() //
            .stream() //
            .filter(name -> !Tenant.GLOBAL_TENANT_ID.equals(name)) //
            .map(this::doubleAliasForTenant) //
            .toList();
    }

    public boolean isDocumentInsertionPossible(String indexName) {
        try {
            Client client = cluster.getInternalNodeClient();
            IndexRequest request = new IndexRequest(indexName) //
                .source(ImmutableMap.of("new", "document")) //
                .setRefreshPolicy(IMMEDIATE);
            client.index(request).actionGet();
            return true;
        } catch (ClusterBlockException clusterBlockException) {
            return false;
        }
    }

    public Settings getIndexSettings(String index) {
        Client client = cluster.getInternalNodeClient();
        GetSettingsRequest request = new GetSettingsRequest().indices(index);
        return client.admin().indices().getSettings(request).actionGet().getIndexToSettings().get(index);
    }

    public long countDocumentInIndex(String indexName) {
        Client client = cluster.getInternalNodeClient();
        SearchRequest request = new SearchRequest(indexName);
        request.source(SearchSourceBuilder.searchSource().size(0).trackTotalHits(true).query(QueryBuilders.matchAllQuery()));
        SearchResponse response = client.search(request).actionGet();
        try {
            assertThat(response.getFailedShards(), equalTo(0));
            return response.getHits().getTotalHits().value();
        } finally {
            response.decRef();
        }
    }

    public Optional<String> getDocumentSource(String indexName, String documentId) {
        Client client = cluster.getInternalNodeClient();
        GetResponse response = client.get(new GetRequest(indexName, documentId)).actionGet();
        return Optional.ofNullable(response) //
            .filter(GetResponse::isExists) //
            .map(GetResponse::getSourceAsString);
    }

    public void createIndex(List<DoubleAliasIndex> indices) {
        createIndex(indices.toArray(DoubleAliasIndex[]::new));
    }

    public void createIndex(DoubleAliasIndex...indices) {
        createIndex(MULTITENANCY_INDEX_PREFIX, 0, null, indices);
    }

    public DoubleAliasIndex doubleAliasForTenant(String tenantName) {
        return DoubleAliasIndex.forTenantWithPrefix(getConfiguredIndexPrefix(), tenantName);
    }

    public void createLegacyIndex(LegacyIndex...indices) {
        String configuredIndexNamePrefix = getConfiguredIndexPrefix();

        BulkRequest bulkRequest = new BulkRequest();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(INDEX_TEMPLATE_NAME) //
            .patterns(Collections.singletonList(configuredIndexNamePrefix + "*")) //
            .settings(Settings.builder().put("index.hidden", true));
        Client client = cluster.getInternalNodeClient();
        client.admin() //
            .indices() //
            .putTemplate(templateRequest) //
            .actionGet();
        for (LegacyIndex index : indices) {
            String currentIndexName = index.indexName();
            if (!currentIndexName.startsWith(configuredIndexNamePrefix)) {
                throw new IllegalStateException("All legacy indices names should start with " + configuredIndexNamePrefix);
            }
            bulkRequest.add(new IndexRequest(currentIndexName).source(DocNode.EMPTY));
            createdIndices.add(index);
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index.indexName()).aliases(index.longAlias()));
        }
        BulkResponse response = client.bulk(bulkRequest.setRefreshPolicy(IMMEDIATE)).actionGet();
        if(response.hasFailures()) {
            log.error("Cannot create legacy indices due to '{}'", response.buildFailureMessage());
        }
        assertThat(response.hasFailures(), equalTo(false));
        AcknowledgedResponse acknowledgedResponse = client.admin().indices().aliases(indicesAliasesRequest).actionGet();
        if(!acknowledgedResponse.isAcknowledged()) {
            log.error("Cannot create aliases for legacy indices.");
        }
        assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
    }

    public void createIndex(String indexNamesPrefix, int numberOfReplicas, Settings additionalSettings, DoubleAliasIndex...indices) {
        BulkRequest bulkRequestCreateIndices = new BulkRequest();
        BulkRequest bulkRequestClearIndices = new BulkRequest();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        Settings.Builder settings = Settings.builder()
            .put("index.hidden", true)
            .put("index.number_of_replicas", numberOfReplicas);
        if(additionalSettings != null) {
            settings.put(additionalSettings);
        }
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(INDEX_TEMPLATE_NAME) //
            .patterns(Collections.singletonList(indexNamesPrefix + "*"))
            .settings(settings);
        Client client = cluster.getInternalNodeClient();
        AcknowledgedResponse createTemplateResponse = client.admin() //
            .indices() //
            .putTemplate(templateRequest) //
            .actionGet();
        assertThat(createTemplateResponse.isAcknowledged(), equalTo(true));
        for (DoubleAliasIndex index : indices) {
            String currentIndex = index.indexName();
            if (!currentIndex.startsWith(indexNamesPrefix)) {
                String message = String.join("", "Incorrect name of index ",
                    currentIndex, ". All created indices must have a common prefix '" + indexNamesPrefix, "'.");
                throw new IllegalStateException(message);
            }
            bulkRequestCreateIndices.add(new IndexRequest(currentIndex).id(CREATE_INDEX_ID).source(DocNode.EMPTY));
            createdIndices.add(index);
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index.indexName()).aliases(index.longAlias(), index.shortAlias()));
            bulkRequestClearIndices.add(new DeleteRequest(currentIndex, CREATE_INDEX_ID));
        }
        BulkResponse response = client.bulk(bulkRequestCreateIndices.setRefreshPolicy(IMMEDIATE)).actionGet();
        if (response.hasFailures()) {
            log.error("Create index failure response {}", response.buildFailureMessage());
        }
        assertThat(response.hasFailures(), equalTo(false));
        var acknowledgedResponse = client.admin().indices().aliases(indicesAliasesRequest).actionGet();
        assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
        response = client.bulk(bulkRequestClearIndices).actionGet();
        if (response.hasFailures()) {
            log.error("Cannot clear newly created indices {}", response.buildFailureMessage());
        }
        assertThat(response.hasFailures(), equalTo(false));
    }

    public String getConfiguredIndexPrefix() {
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        return configurationProvider.getConfig().orElseThrow().getIndex();
    }

    public void createIndexInYellowState(String index) {
        Client client = cluster.getInternalNodeClient();
        CreateIndexRequest request = new CreateIndexRequest(index);
        Settings.Builder settings = Settings.builder() //
            .put("index.number_of_replicas", 100); // force index yellow state
        request.settings(settings);
        CreateIndexResponse createIndexResponse = client.admin().indices().create(request).actionGet();
        assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
        this.createdIndices.add(() -> index);
    }

    public void createBackupIndex(BackupIndex...indices) {
        BulkRequest bulkRequestIndex = new BulkRequest();
        BulkRequest bulkRequestDelete = new BulkRequest();
        for(BackupIndex index : indices) {
            bulkRequestIndex.add(new IndexRequest(index.indexName()).id(CREATE_INDEX_ID).source(DocNode.EMPTY));
            createdIndices.add(index);
            bulkRequestDelete.add(new DeleteRequest(index.indexName(), CREATE_INDEX_ID));
        }

        Client client = cluster.getInternalNodeClient();
        BulkResponse response = client.bulk(bulkRequestIndex.setRefreshPolicy(IMMEDIATE)).actionGet();
        if (response.hasFailures()) {
            log.error("Create backup index failure response {}", response.buildFailureMessage());
        }
        assertThat(response.hasFailures(), equalTo(false));
        response = client.bulk(bulkRequestDelete.setRefreshPolicy(IMMEDIATE)).actionGet();
        if (response.hasFailures()) {
            log.error("Create backup index failure, cannot clean index, response {}", response.buildFailureMessage());
        }
        assertThat(response.hasFailures(), equalTo(false));
    }

    public DocNode getIndexMappingsAsDocNode(String indexName) {
        Client client = cluster.getInternalNodeClient();
        GetMappingsResponse response = client.admin().indices().getMappings(new GetMappingsRequest(StaticSettings.DEFAULT_MASTER_TIMEOUT).indices(indexName)).actionGet();
        Map<String, Object> source = Optional.of(response) //
            .map(GetMappingsResponse::getMappings) //
            .map(map -> map.get(indexName)) //
            .map(MappingMetadata::getSourceAsMap) //
            .orElseGet(Collections::emptyMap);
        return DocNode.wrap(source);
    }

    public void addDataMigrationMarkerToTheIndex(String indexName) {
        PrivilegedConfigClient client = getPrivilegedClient();
        StepRepository stepRepository = new StepRepository(client);
        GetMappingsResponse response = stepRepository.findIndexMappings(indexName);
        MappingMetadata mappingMetadata = response.getMappings().get(indexName);
        assertThat(mappingMetadata, notNullValue());
        Map<String, Object> mappingSources = mappingMetadata.getSourceAsMap();
        assertThat(mappingSources, notNullValue());
        Map<String, Object> properties = (Map<String, Object>) mappingSources.get("properties");
        assertThat("Index has not mapping defined, is index empty?", properties, notNullValue());
        properties.put("sg_data_migrated_to_8_8_0", ImmutableMap.of("type", "boolean"));
        PutMappingRequest request = new PutMappingRequest(indexName).source(mappingSources);
        AcknowledgedResponse acknowledgedResponse = client.admin().indices().putMapping(request).actionGet();
        assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
    }

    public PrivilegedConfigClient getPrivilegedClient() {
        if(this.privilegedConfigClient != null) {
            return this.privilegedConfigClient;
        }
        Client client = cluster.getInternalNodeClient();
        this.privilegedConfigClient = adapt(client);
        return this.privilegedConfigClient;
    }

    public static ImmutableList<TenantIndex> doubleAliasIndexToTenantDataWithoutTenantName(ImmutableList<DoubleAliasIndex> indices) {
        return indices.map(i -> new TenantIndex(i.indexName(), GLOBAL_TENANT_INDEX.indexName().equals(i.indexName()) ?
            Tenant.GLOBAL_TENANT_ID : "tenant name is not important here"));
    }

    public static ImmutableList<TenantIndex> doubleAliasIndexToTenantDataWithoutTenantName(DoubleAliasIndex...indices) {
        return doubleAliasIndexToTenantDataWithoutTenantName(ImmutableList.ofArray(indices));
    }

    public List<DoubleAliasIndex> generatePrivateTenantNames(String prefix, int number) {
        return IntStream.range(0, number)
            .mapToObj(index -> "private tenant name - " + index)
            .map(tenantName -> DoubleAliasIndex.forTenantWithPrefix(prefix, tenantName))
            .collect(Collectors.toList());
    }

    public void assertThatDocumentExists(String index, String documentId) {
        Client client = cluster.getInternalNodeClient();
        GetRequest request = new GetRequest(index, documentId);
        GetResponse response = client.get(request).actionGet();
        String reason = "Document with id '" + documentId + "' does not exist in index '" + index + "'.";
        assertThat(reason, response.isExists(), equalTo(true));
    }

    public void assertThatDocumentDoesNotExist(String index, String documentId) {
        Client client = cluster.getInternalNodeClient();
        GetRequest request = new GetRequest(index, documentId);
        GetResponse response = client.get(request).actionGet();
        String reason = "Document with id '" + documentId + "' does not exist in index '" + index + "'.";
        assertThat(reason, response.isExists(), not(equalTo(true)));
    }

    public List<DoubleAliasIndex> findIndicesForTenantsDefinedInConfigurationWithoutGlobal() {
        ConfigurationRepository configRepository = cluster.getInjectable(ConfigurationRepository.class);
        SgDynamicConfiguration<Tenant> configuration = configRepository.getConfiguration(CType.TENANTS);
        return configuration.getCEntries() //
            .keySet() //
            .stream() //
            .filter(tenantName -> !Tenant.GLOBAL_TENANT_ID.equals(tenantName)) //
            .map(this::doubleAliasForTenant) //
            .toList();
    }

    public void findAndDeleteBackupIndices() {
        StepRepository stepRepository = new StepRepository(getPrivilegedClient());
        BackupIndex[] backupIndices = stepRepository.findIndexByNameOrAlias("backup_fe_migration_to_8_8_0_*") //
                .stream() //
                .flatMap(response -> Arrays.stream(response.indices())) //
                .map(indexName -> new BackupIndex(indexName)) //
                .toArray(BackupIndex[]::new);
        deleteIndex(backupIndices);
    }

    public interface DeletableIndex {
        String indexForDeletion();
    }

    public record DoubleAliasIndex(String indexName, String shortAlias, String longAlias) implements DeletableIndex {
        public static final String LAST_VERSION_BEFORE_MIGRATION = "8.7.0";
        public static final String LEGACY_VERSION = "7.17.12";

        public DoubleAliasIndex {
            requireNonEmpty(indexName, "Index name is required");
            requireNonEmpty(shortAlias, "Short alias name is required");
            requireNonEmpty(longAlias, "Long alias name is required");
        }

        public static DoubleAliasIndex forTenantWithPrefix(String indexNamePrefix, String tenantName, String version) {
            String baseAndShortAlias = tenantNameToIndexName(indexNamePrefix, tenantName);
            String fullIndexName = createIndexName(baseAndShortAlias, version);
            String aliasWithVersionAndSeqNo = createLongAlias(baseAndShortAlias, version);
            return new DoubleAliasIndex(fullIndexName, baseAndShortAlias, aliasWithVersionAndSeqNo);
        }

        public static DoubleAliasIndex forTenantWithPrefix(String indexNamePrefix, String tenantName) {
            return forTenantWithPrefix(indexNamePrefix, tenantName, LAST_VERSION_BEFORE_MIGRATION);
        }

        private static String createLongAlias(String baseIndexName, String version) {
            return baseIndexName + "_" + version;
        }

        private static String createIndexName(String baseIndexName, String version) {
            return createLongAlias(baseIndexName, version) +"_001";
        }

        public String getIndexNameInVersion(String version) {
            return createIndexName(shortAlias, version);
        }

        public String getLongAliasInVersion(String version) {
            return createLongAlias(shortAlias, version);
        }

        public LegacyIndex toLegacyIndex(String version) {
            return new LegacyIndex(getIndexNameInVersion(version), getLongAliasInVersion(version));
        }

        @Override
        public String indexForDeletion() {
            return indexName;
        }
    }

    public Optional<GetIndexResponse> findHiddenIndexByName(String indexNameOrAlias) {
        Strings.requireNonEmpty(indexNameOrAlias, "Index or alias name is required");
        try {
            GetIndexRequest request = new GetIndexRequest(StaticSettings.DEFAULT_MASTER_TIMEOUT);
            request.indices(indexNameOrAlias).indicesOptions(IndicesOptions.strictExpandHidden());
            GetIndexResponse response = getPrivilegedClient().admin().indices().getIndex(request).actionGet();
            return Optional.ofNullable(response);
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        }
    }

    public void deleteIndex(String indexName) {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        AcknowledgedResponse response = getPrivilegedClient().admin().indices().delete(request).actionGet();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    public record LegacyIndex(String indexName, String longAlias) implements DeletableIndex {
        public LegacyIndex {
            requireNonEmpty(indexName, "Index name is required");
            requireNonEmpty(longAlias, "Long alias is required");
        }

        @Override
        public String indexForDeletion() {
            return indexName;
        }
    }

    public record BackupIndex(String indexName) implements DeletableIndex {

        public BackupIndex(LocalDateTime backupIndexCreationTime) {
            this("backup_fe_migration_to_8_8_0_" + IndexNameDataFormatter.format(backupIndexCreationTime));
        }

        @Override
        public String indexForDeletion() {
            return indexName;
        }
    }
}
