/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationExecutionSummary;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.IndexAlreadyExistsException;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.OptimisticLock;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.OptimisticLockException;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionSummary;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Optional;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.FAILURE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.IN_PROGRESS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.SUCCESS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class IndexMigrationStateRepositoryTest {

    private static final Logger log = LogManager.getLogger(IndexMigrationStateRepositoryTest.class);
    public static final String ID_1 = "my-id-1";
    public static final String ID_2 = "my-id-2";
    public static final String ID_3 = "my-id-3";
    public static final int STEP_NO_1 = 7;
    public static final int STEP_NO_2 = STEP_NO_1 + 1;
    public static final String STEP_NAME_1 = "the first step";
    public static final String STEP_NAME_2 = "the second step";
    public static final String MESSAGE = "Please wait...";
    public static final String TEMP_INDEX_NAME = "temp index name";
    public static final String BACKUP_INDEX_NAME = "backup index name";

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    private IndexMigrationStateRepository repository;

    @Before
    public void before() {
        Client client = cluster.getInternalNodeClient();
        PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.repository = new IndexMigrationStateRepository(privilegedConfigClient);
        if(repository.isIndexCreated()) {
            client.admin().indices().delete(new DeleteIndexRequest(IndexMigrationStateRepository.INDEX_NAME)).actionGet();
        }
    }

    @Test
    public void shouldDetectThatIndexDoesNotExist() {
        boolean indexCreated = repository.isIndexCreated();

        assertThat(indexCreated, equalTo(false));
    }

    @Test
    public void shouldDetectThatIndexExists() {
        repository.createIndex();

        boolean indexCreated = repository.isIndexCreated();

        assertThat(indexCreated, equalTo(true));
    }

    @Test
    public void shouldNotCreateIndexWhenTheIndexAlreadyExists() {
        repository.createIndex();

        assertThatThrown(() -> repository.createIndex(), instanceOf(IndexAlreadyExistsException.class));
    }

    @Test
    public void shouldCreateMappings() {
        repository.createIndex();

        GetMappingsRequest request = new GetMappingsRequest().indices(IndexMigrationStateRepository.INDEX_NAME);
        GetMappingsResponse response = cluster.getPrivilegedInternalNodeClient().admin().indices().getMappings(request).actionGet();
        MappingMetadata mappingMetadata = response.getMappings().get(IndexMigrationStateRepository.INDEX_NAME);
        assertThat(mappingMetadata, notNullValue());
        DocNode mappings = DocNode.wrap(mappingMetadata.getSourceAsMap());
        log.info("Mapping created for index: '{}'", mappings.toJsonString());
        assertThat(mappings, containsValue("$.properties.start_time.type", "date"));
        assertThat(mappings, containsValue("$.properties.status.type", "keyword"));
        assertThat(mappings, containsValue("$.properties.temp_index_name.type", "keyword"));
        assertThat(mappings, containsValue("$.properties.backup_index_name.type", "keyword"));
        assertThat(mappings, containsValue("$.properties.stages.properties.start_time.type", "date"));
        assertThat(mappings, containsValue("$.properties.stages.properties.status.type", "keyword"));
        assertThat(mappings, containsValue("$.properties.stages.properties.name.type", "keyword"));
        assertThat(mappings, containsValue("$.properties.stages.properties.number.type", "long"));
        assertThat(mappings, containsValue("$.properties.stages.properties.message.type", "text"));
        assertThat(mappings, containsValue("$.properties.stages.properties.message.fields.keyword.type", "keyword"));
        assertThat(mappings, containsValue("$.properties.stages.properties.message.fields.keyword.ignore_above", 250));
        assertThat(mappings, containsValue("$.properties.stages.properties.details.fields.keyword.type", "keyword"));
        assertThat(mappings, containsValue("$.properties.stages.properties.details.fields.keyword.ignore_above", 250));
    }

    @Test
    public void shouldCreateDocumentWithIdOne() {
        repository.createIndex();
        LocalDateTime startTime = LocalDateTime.of(2004, 5, 1, 0, 0);
        StepExecutionSummary stepSummary = new StepExecutionSummary(STEP_NO_1, startTime, STEP_NAME_1, OK, MESSAGE);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
        var migrationSummary = new MigrationExecutionSummary(startTime, SUCCESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);

        repository.create(ID_1, migrationSummary);

        MigrationExecutionSummary loadedSummary = repository.findById(ID_1).orElseThrow();
        assertThat(loadedSummary, equalTo(migrationSummary));
    }

    @Test
    public void shouldCreateTwoDocuments() {
        repository.createIndex();
        LocalDateTime startTime = LocalDateTime.of(2004, 5, 1, 0, 0);
        StepExecutionSummary stepSummary = new StepExecutionSummary(STEP_NO_1, startTime, STEP_NAME_1, OK, MESSAGE);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
        var migrationSummaryOne = new MigrationExecutionSummary(startTime, SUCCESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);
        var migrationSummaryTwo = new MigrationExecutionSummary(startTime, FAILURE, null, null, stages);
        repository.create(ID_1, migrationSummaryOne);

        repository.create(ID_2, migrationSummaryTwo);

        MigrationExecutionSummary loadedSummary = repository.findById(ID_2).orElseThrow();
        assertThat(loadedSummary, equalTo(migrationSummaryTwo));
        loadedSummary = repository.findById(ID_1).orElseThrow();
        assertThat(loadedSummary, equalTo(migrationSummaryOne));
    }

    @Test
    public void shouldReportErrorWhenCreatedDocumentAlreadyExists() {
        repository.createIndex();
        LocalDateTime startTime = LocalDateTime.of(2004, 5, 1, 0, 0);
        StepExecutionSummary stepSummary = new StepExecutionSummary(STEP_NO_1, startTime, STEP_NAME_1, OK, MESSAGE);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
        var migrationSummary = new MigrationExecutionSummary(startTime, SUCCESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);
        var migrationSummaryTwo = new MigrationExecutionSummary(startTime, FAILURE, null, null, stages);
        repository.upsert(ID_1, migrationSummary);
        OptimisticLock optimisticLock = repository.findById(ID_1).orElseThrow().lockData();

        assertThatThrown(() -> repository.create(ID_1, migrationSummaryTwo), instanceOf(OptimisticLockException.class));

        OptimisticLock lockDataAfterUpdate = repository.findById(ID_1).orElseThrow().lockData();
        assertThat(lockDataAfterUpdate, equalTo(optimisticLock));
    }

    @Test
    public void shouldStoreDocumentWithIdOne() {
        repository.createIndex();
        LocalDateTime startTime = LocalDateTime.of(2004, 5, 1, 0, 0);
        StepExecutionSummary stepSummary = new StepExecutionSummary(STEP_NO_1, startTime, STEP_NAME_1, OK, MESSAGE);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
        var migrationSummary = new MigrationExecutionSummary(startTime, SUCCESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);

        repository.upsert(ID_1, migrationSummary);

        MigrationExecutionSummary loadedSummary = repository.findById(ID_1).orElseThrow();
        assertThat(loadedSummary, equalTo(migrationSummary));
    }

    @Test
    public void shouldStoreDocumentWithIdTwo() {
        repository.createIndex();
        LocalDateTime startTime = LocalDateTime.of(2004, 5, 1, 0, 1);
        StepExecutionSummary stepSummary = new StepExecutionSummary(STEP_NO_1, startTime, STEP_NAME_1, OK, MESSAGE);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
        var migrationSummary = new MigrationExecutionSummary(startTime, SUCCESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);

        repository.upsert(ID_2, migrationSummary);

        MigrationExecutionSummary loadedSummary = repository.findById(ID_2).orElseThrow();
        assertThat(loadedSummary, equalTo(migrationSummary));
    }

    @Test
    public void shouldNotFindDocument() {
        repository.createIndex();

        Optional<MigrationExecutionSummary> summary = repository.findById(ID_1);

        assertThat(summary.isPresent(), equalTo(false));
    }

    @Test
    public void shouldLoadPrimaryTermAndSeqNo() {
        repository.createIndex();
        LocalDateTime startTime = LocalDateTime.of(2004, 5, 1, 0, 1);
        StepExecutionSummary stepSummary = new StepExecutionSummary(STEP_NO_1, startTime, STEP_NAME_1, OK, MESSAGE);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
        var migrationSummary = new MigrationExecutionSummary(startTime, SUCCESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);
        repository.upsert(ID_3, migrationSummary);

        MigrationExecutionSummary loadedSummary = repository.findById(ID_3).orElseThrow();

        OptimisticLock lock = loadedSummary.lockData();
        assertThat(lock, notNullValue());
        assertThat(lock.primaryTerm(), notNullValue());
        assertThat(lock.seqNo(), notNullValue());
    }

    @Test
    public void shouldUpdateMigrationSummary() {
        repository.createIndex();
        LocalDateTime startTime = LocalDateTime.of(2004, 5, 1, 0, 1);
        StepExecutionSummary stepSummaryOne = new StepExecutionSummary(STEP_NO_1, startTime, STEP_NAME_1, OK, MESSAGE);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummaryOne);
        var migrationSummary = new MigrationExecutionSummary(startTime, IN_PROGRESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);
        repository.upsert(ID_3, migrationSummary);
        StepExecutionSummary stepSummaryTwo = new StepExecutionSummary(STEP_NO_2, startTime, STEP_NAME_2, OK, MESSAGE);
        stages = ImmutableList.of(stepSummaryOne, stepSummaryTwo);
        migrationSummary = new MigrationExecutionSummary(startTime, SUCCESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);

        repository.upsert(ID_3, migrationSummary);

        MigrationExecutionSummary loadedSummary = repository.findById(ID_3).orElseThrow();
        assertThat(loadedSummary.status(), equalTo(SUCCESS));
        assertThat(loadedSummary.stages(), hasSize(2));
        assertThat(loadedSummary.stages().get(0).name(), equalTo(STEP_NAME_1));
        assertThat(loadedSummary.stages().get(1).name(), equalTo(STEP_NAME_2));
    }

    @Test
    public void shouldUpdateMigrationSummaryWithOptimisticLock() {
        repository.createIndex();
        LocalDateTime startTime = LocalDateTime.of(2004, 5, 1, 0, 1);
        StepExecutionSummary stepSummaryOne = new StepExecutionSummary(STEP_NO_1, startTime, STEP_NAME_1, OK, MESSAGE);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummaryOne);
        var migrationSummary = new MigrationExecutionSummary(startTime, IN_PROGRESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);
        repository.upsert(ID_3, migrationSummary);
        OptimisticLock lock = repository.findById(ID_3).orElseThrow().lockData();
        StepExecutionSummary stepSummaryTwo = new StepExecutionSummary(STEP_NO_2, startTime, STEP_NAME_2, OK, MESSAGE);
        stages = ImmutableList.of(stepSummaryOne, stepSummaryTwo);
        migrationSummary = new MigrationExecutionSummary(startTime, SUCCESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);

        repository.updateWithLock(ID_3, migrationSummary, lock);

        MigrationExecutionSummary loadedSummary = repository.findById(ID_3).orElseThrow();
        assertThat(loadedSummary.status(), equalTo(SUCCESS));
        assertThat(loadedSummary.stages(), hasSize(2));
        assertThat(loadedSummary.stages().get(0).name(), equalTo(STEP_NAME_1));
        assertThat(loadedSummary.stages().get(1).name(), equalTo(STEP_NAME_2));
    }

    @Test
    public void shouldNotUpdateMigrationSummaryWithOptimisticLock() {
        repository.createIndex();
        LocalDateTime startTime = LocalDateTime.of(2004, 5, 1, 0, 1);
        StepExecutionSummary stepSummaryOne = new StepExecutionSummary(STEP_NO_1, startTime, STEP_NAME_1, OK, MESSAGE);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummaryOne);
        var migrationSummary = new MigrationExecutionSummary(startTime, IN_PROGRESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);
        repository.upsert(ID_3, migrationSummary);
        OptimisticLock lock = repository.findById(ID_3).orElseThrow().lockData();
        migrationSummary = new MigrationExecutionSummary(startTime.plusHours(1), IN_PROGRESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);
        repository.upsert(ID_3, migrationSummary);
        StepExecutionSummary stepSummaryTwo = new StepExecutionSummary(STEP_NO_2, startTime, STEP_NAME_2, OK, MESSAGE);
        stages = ImmutableList.of(stepSummaryOne, stepSummaryTwo);
        var updatedSummary = new MigrationExecutionSummary(startTime, SUCCESS, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, stages);

        assertThatThrown(() -> repository.updateWithLock(ID_3, updatedSummary, lock), instanceOf(OptimisticLockException.class));

    }
}