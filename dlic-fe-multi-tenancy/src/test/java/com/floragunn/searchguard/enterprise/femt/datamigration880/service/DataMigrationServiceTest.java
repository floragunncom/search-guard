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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepsFactory;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.util.EsLogging;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.FAILURE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.IN_PROGRESS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.SUCCESS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_CONFLICT;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PRECONDITION_FAILED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataMigrationServiceTest {

    @ClassRule
    public static EsLogging esLogging = new EsLogging();

    private static final Logger log = LogManager.getLogger(DataMigrationServiceTest.class);

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2010, 12, 1, 1, 1), ZoneOffset.UTC);
    public static final String STEP_NAME = "My names is mocked step.";
    public static final String MESSAGE = "I am done!";
    public static final String ERROR_MESSAGE = "Unexpected exception during mock step execution";
    public static final String MIGRATION_ID = "migration_8_8_0";
    public static final MigrationConfig STRICT_CONFIG = new MigrationConfig(false);

    @Mock
    private DataMigrationService dataMigrationService;

    @Mock
    private MigrationStateRepository repository;

    @Mock
    private StepsFactory stepFactory;

    @Mock
    private MigrationStep step;

    @Captor
    private ArgumentCaptor<MigrationExecutionSummary> summaryCaptor;

    @Before
    public void before() {
        this.dataMigrationService = new DataMigrationService(repository, stepFactory, Clock.fixed(NOW.toInstant(), ZoneOffset.UTC));
    }

    @Test
    public void shouldPerformDataMigration() {
        mockOneSuccessfulStep();

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_OK));
        verify(repository).upsert(anyString(), summaryCaptor.capture());
        MigrationExecutionSummary persistedSummary = summaryCaptor.getValue();
        assertThat(persistedSummary.status(), equalTo(SUCCESS));
        assertThat(persistedSummary.stages().get(0).name(), equalTo(STEP_NAME));
    }

    @Test
    public void shouldDetectErrorDuringMigration() {
        when(step.execute(any(DataMigrationContext.class))).thenThrow(new RuntimeException(ERROR_MESSAGE));
        when(step.name()).thenReturn(STEP_NAME);
        when(stepFactory.createSteps()).thenReturn(ImmutableList.of(step));

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_INTERNAL_SERVER_ERROR));
        verify(repository).upsert(anyString(), summaryCaptor.capture());
        MigrationExecutionSummary persistedSummary = summaryCaptor.getValue();
        assertThat(persistedSummary.status(), equalTo(FAILURE));
        assertThat(persistedSummary.stages().get(0).name(), equalTo(STEP_NAME));
        assertThat(persistedSummary.stages().get(0).message(), containsString(ERROR_MESSAGE));
    }

    @Test
    public void shouldCreateIndexWhenTheIndexDoesNotExist() {
        mockOneSuccessfulStep();
        when(repository.isIndexCreated()).thenReturn(false);

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_OK));
        verify(repository).createIndex();
    }

    @Test
    public void shouldNotCreateIndexWhenTheIndexExists() {
        mockOneSuccessfulStep();
        when(repository.isIndexCreated()).thenReturn(true);

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_OK));
        verify(repository, never()).createIndex();
    }

    @Test
    public void shouldReturnErrorResponseWhenIndexWasCreatedByConcurrentMigration() {
        when(repository.isIndexCreated()).thenReturn(false);
        doThrow(new IndexAlreadyExistsException("Test index already exists exception", null)).when(repository).createIndex();

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_CONFLICT));
        // should not store/update migration state in db
        verify(repository, never()).create(anyString(), any(MigrationExecutionSummary.class));
        verify(repository, never()).upsert(anyString(), any(MigrationExecutionSummary.class));
        verify(repository, never()).updateWithLock(anyString(), any(MigrationExecutionSummary.class), any(OptimisticLock.class));
        // should not execute any migration step
        verify(step, never()).execute(any(DataMigrationContext.class));
    }

    @Test
    public void shouldDetectThatAnotherMigrationIsInProgressBasedOnRepositoryContent() {
        LocalDateTime past = NOW.minusMinutes(1).toLocalDateTime();
        MigrationExecutionSummary summary = new MigrationExecutionSummary(past, IN_PROGRESS, null, null, ImmutableList.empty());
        when(repository.findById(MIGRATION_ID)).thenReturn(Optional.of(summary));

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_BAD_REQUEST));
        // should not store/update migration state in db
        verify(repository, never()).create(anyString(), any(MigrationExecutionSummary.class));
        verify(repository, never()).upsert(anyString(), any(MigrationExecutionSummary.class));
        verify(repository, never()).updateWithLock(anyString(), any(MigrationExecutionSummary.class), any(OptimisticLock.class));
        // should not execute any migration step
        verify(step, never()).execute(any(DataMigrationContext.class));
    }

    @Test
    public void shouldStartTheMigrationForTheFirstTime() {
        when(repository.findById(MIGRATION_ID)).thenReturn(Optional.empty());
        mockOneSuccessfulStep();

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_OK));
        verify(repository).create(Mockito.eq(MIGRATION_ID), summaryCaptor.capture());
        verify(step).execute(any(DataMigrationContext.class));
        MigrationExecutionSummary persistedSummary = summaryCaptor.getValue();
        assertThat(persistedSummary.stages(), hasSize(1));
        assertThat(persistedSummary.stages().get(0).name(), equalTo("preconditions check"));
        assertThat(persistedSummary.stages().get(0).message(), containsString("The first start"));
    }

    @Test
    public void shouldDetectThatAnotherProcessTriesToStartDataMigrationForTheFirstTime() {
        when(repository.findById(MIGRATION_ID)).thenReturn(Optional.empty());
        doThrow(new OptimisticLockException("Test optimistic lock exception", null)) //
            .when(repository).create(eq(MIGRATION_ID), any(MigrationExecutionSummary.class));

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_PRECONDITION_FAILED));
        // should not store/update migration state in db
        verify(repository, never()).upsert(anyString(), any(MigrationExecutionSummary.class));
        verify(repository, never()).updateWithLock(anyString(), any(MigrationExecutionSummary.class), any(OptimisticLock.class));
        // should not execute any migration step
        verify(step, never()).execute(any(DataMigrationContext.class));

    }

    @Test
    public void shouldRestartMigrationProcess() {
        LocalDateTime past = NOW.minusDays(1).toLocalDateTime();
        OptimisticLock lockData = new OptimisticLock(7, 1492);
        MigrationExecutionSummary summary = new MigrationExecutionSummary(past, IN_PROGRESS, null, null, ImmutableList.empty(), lockData);
        when(repository.findById(MIGRATION_ID)).thenReturn(Optional.of(summary));
        mockOneSuccessfulStep();

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_OK));
        verify(repository).updateWithLock(eq(MIGRATION_ID), summaryCaptor.capture(), eq(lockData));
        verify(step).execute(any(DataMigrationContext.class));
        MigrationExecutionSummary storedSummary = summaryCaptor.getValue();
        assertThat(storedSummary.status(), equalTo(IN_PROGRESS));
        assertThat(storedSummary.stages(), hasSize(1));
        assertThat(storedSummary.stages().get(0).name(), equalTo("preconditions check"));
        assertThat(storedSummary.stages().get(0).message(), containsString("restarted"));
    }

    @Test
    public void shouldDetectParallelMigrationProcessRestart() {
        LocalDateTime past = NOW.minusDays(1).toLocalDateTime();
        OptimisticLock lockData = new OptimisticLock(7, 1492);
        MigrationExecutionSummary summary = new MigrationExecutionSummary(past, IN_PROGRESS, null, null, ImmutableList.empty(), lockData);
        when(repository.findById(MIGRATION_ID)).thenReturn(Optional.of(summary));
        doThrow(new OptimisticLockException("Test optimistic lock exception", null))//
            .when(repository) //
            .updateWithLock(eq(MIGRATION_ID), any(MigrationExecutionSummary.class), eq(lockData));

        StandardResponse standardResponse = dataMigrationService.migrateData(STRICT_CONFIG);

        log.debug("Data migration response '{}'", standardResponse.toJsonString());
        assertThat(standardResponse.getStatus(), equalTo(SC_CONFLICT));
        // should not store/update migration state in db
        verify(repository, never()).create(anyString(), any(MigrationExecutionSummary.class));
        verify(repository, never()).upsert(anyString(), any(MigrationExecutionSummary.class));
        // should not execute any migration step
        verify(step, never()).execute(any(DataMigrationContext.class));
    }

    private void mockOneSuccessfulStep() {
        when(step.execute(any(DataMigrationContext.class))).thenReturn(new StepResult(OK, MESSAGE));
        when(step.name()).thenReturn(STEP_NAME);
        when(stepFactory.createSteps()).thenReturn(ImmutableList.of(step));
    }
}