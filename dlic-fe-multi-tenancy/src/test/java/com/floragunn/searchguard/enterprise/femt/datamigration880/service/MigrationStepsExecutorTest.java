package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.ThrowExceptionStep;
import org.junit.Before;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.FAILURE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.IN_PROGRESS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.SUCCESS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INDICES_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.UNEXPECTED_ERROR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MigrationStepsExecutorTest {
    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2000, 1, 1, 1, 1), ZoneOffset.UTC);
    public static final String MESSAGE_1 = "I am done!";
    public static final String MESSAGE_2 = "The second step was executed";
    public static final String NAME_1 = "I am the first step";
    public static final String NAME_2 = "I am the second step";
    public static final String MESSAGE_FAILURE_1 = "Step execution failed!";
    public static final String MESSAGE_FAILURE_2 = "Oops, Sth went wrong during migration step execution!";
    public static final MigrationConfig STRICT_CONFIG = new MigrationConfig(false);

    @Captor
    private ArgumentCaptor<MigrationExecutionSummary> summaryCaptor;
    @Mock
    private MigrationStateRepository repository;

    private Clock clock;

    @Before
    public void before() {
        this.clock = Clock.fixed(NOW.toInstant(), ZoneOffset.UTC);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotExecuteMigrationWithoutSteps() {
        new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.empty());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldDetectNullMigrationStepAndReportException() {
        MigrationStep stepOne = stepMockWithResult(NAME_1, OK, MESSAGE_1);
        MigrationStep stepTwo = stepMockWithResult(NAME_2, OK, MESSAGE_2);

        new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(stepOne, stepTwo, null));
    }

    @Test
    public void shouldExecuteAndPersistMigrationStep() {
        // given
        MigrationStep step = stepMockWithResult(NAME_1, OK, MESSAGE_1);
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(step));

        // when
        MigrationExecutionSummary summary = executor.execute();

        //then
        assertThat(summary.status(), equalTo(SUCCESS));
        assertThat(summary.isSuccessful(), equalTo(true));
        assertThat(summary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(summary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(summary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));

        ImmutableList<StepExecutionSummary> stages = summary.stages();
        assertThat(stages, hasSize(1));
        StepExecutionSummary stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));

        verify(repository, times(1)).upsert(eq("migration_8_8_0"), summaryCaptor.capture());

        MigrationExecutionSummary migrationSummary = summaryCaptor.getValue();
        assertThat(migrationSummary.status(), equalTo(SUCCESS));
        assertThat(migrationSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(migrationSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(migrationSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));
        stages = migrationSummary.stages();
        assertThat(stages, hasSize(1));
        stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        assertThat(stepExecutionSummary.status(), equalTo(OK));
    }

    @Test
    public void shouldExecuteAndPersistMigrationMultipleStep() {
        // given
        MigrationStep stepOne = stepMockWithResult(NAME_1, OK, MESSAGE_1);
        MigrationStep stepTwo = stepMockWithResult(NAME_2, OK, MESSAGE_2);
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(stepOne, stepTwo));

        // when
        MigrationExecutionSummary summary = executor.execute();

        //then
        assertThat(summary.status(), equalTo(SUCCESS));
        assertThat(summary.isSuccessful(), equalTo(true));
        assertThat(summary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(summary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(summary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));

        ImmutableList<StepExecutionSummary> stages = summary.stages();
        assertThat(stages, hasSize(2));
        StepExecutionSummary stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.status(), equalTo(OK));
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        stepExecutionSummary = stages.get(1);
        assertThat(stepExecutionSummary.status(), equalTo(OK));
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_2));
        assertThat(stepExecutionSummary.number(), equalTo(1L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_2));

        verify(repository, times(2)).upsert(eq("migration_8_8_0"), summaryCaptor.capture());
        List<MigrationExecutionSummary> persistedSummaries = summaryCaptor.getAllValues();
        assertThat(persistedSummaries, hasSize(2));
        // persist invocation after execution of the first step
        MigrationExecutionSummary stepSummary = persistedSummaries.get(0);
        assertThat(stepSummary.status(), equalTo(IN_PROGRESS));
        assertThat(stepSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(stepSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(stepSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));
        stages = stepSummary.stages();
        assertThat(stages, hasSize(1));
        stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        assertThat(stepExecutionSummary.status(), equalTo(OK));

        // persist invocation after execution of the second step
        stepSummary = persistedSummaries.get(1);
        assertThat(stepSummary.status(), equalTo(SUCCESS));
        assertThat(stepSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(stepSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(stepSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));
        stages = stepSummary.stages();
        assertThat(stages, hasSize(2));
        stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        assertThat(stepExecutionSummary.status(), equalTo(OK));
        stepExecutionSummary = stages.get(1);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_2));
        assertThat(stepExecutionSummary.number(), equalTo(1L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_2));
        assertThat(stepExecutionSummary.status(), equalTo(OK));
    }

    @Test
    public void shouldBreakMigrationProcessInCaseOfExpectedFailure() {
        // given
        MigrationStep stepOne = stepMockWithResult(NAME_1, INDICES_NOT_FOUND_ERROR, MESSAGE_1);
        MigrationStep stepTwo = stepMockWithResult(NAME_2, OK, MESSAGE_2);
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(stepOne, stepTwo));

        // when
        MigrationExecutionSummary restSummary = executor.execute();

        //then
        verify(stepOne).execute(any(DataMigrationContext.class));
        verify(stepTwo, never()).execute(any(DataMigrationContext.class));

        assertThat(restSummary.status(), equalTo(FAILURE));
        assertThat(restSummary.isSuccessful(), equalTo(false));
        assertThat(restSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(restSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(restSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));

        ImmutableList<StepExecutionSummary> stages = restSummary.stages();
        assertThat(stages, hasSize(1));
        StepExecutionSummary stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.status(), equalTo(INDICES_NOT_FOUND_ERROR));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));

        verify(repository, times(1)).upsert(eq("migration_8_8_0"), summaryCaptor.capture());

        MigrationExecutionSummary migrationSummary = summaryCaptor.getValue();
        assertThat(migrationSummary.status(), equalTo(FAILURE));
        assertThat(migrationSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(migrationSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(migrationSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));
        stages = migrationSummary.stages();
        assertThat(stages, hasSize(1));
        stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        assertThat(stepExecutionSummary.status(), equalTo(INDICES_NOT_FOUND_ERROR));
    }

    @Test
    public void shouldBreakMigrationProcessInCaseOfUnexpectedFailure() {
        // given
        MigrationStep stepOne = stepMock(NAME_1);
        when(stepOne.execute(any(DataMigrationContext.class))).thenThrow(new RuntimeException(MESSAGE_FAILURE_1));
        MigrationStep stepTwo = stepMockWithResult(NAME_2, OK, MESSAGE_2);
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(stepOne, stepTwo));

        // when
        MigrationExecutionSummary restSummary = executor.execute();

        //then
        verify(stepOne).execute(any(DataMigrationContext.class));
        verify(stepTwo, never()).execute(any(DataMigrationContext.class));

        assertThat(restSummary.status(), equalTo(FAILURE));
        assertThat(restSummary.isSuccessful(), equalTo(false));
        assertThat(restSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(restSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(restSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));

        ImmutableList<StepExecutionSummary> stages = restSummary.stages();
        assertThat(stages, hasSize(1));
        StepExecutionSummary stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo("Unexpected error: " + MESSAGE_FAILURE_1));
        assertThat(stepExecutionSummary.status(), equalTo(UNEXPECTED_ERROR));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));

        verify(repository, times(1)).upsert(eq("migration_8_8_0"), summaryCaptor.capture());

        MigrationExecutionSummary stepSummary = summaryCaptor.getValue();
        assertThat(stepSummary.status(), equalTo(FAILURE));
        assertThat(stepSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(stepSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(stepSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));
        stages = stepSummary.stages();
        assertThat(stages, hasSize(1));
        stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo("Unexpected error: " + MESSAGE_FAILURE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        assertThat(stepExecutionSummary.status(), equalTo(UNEXPECTED_ERROR));
    }

    @Test
    public void shouldMarkMigrationAsFailedInCaseOfExpectedErrorInTheLastStep() {
        // given
        MigrationStep stepOne = stepMockWithResult(NAME_1, OK, MESSAGE_1);
        MigrationStep stepTwo = stepMockWithResult(NAME_2, INDICES_NOT_FOUND_ERROR, MESSAGE_2);
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(stepOne, stepTwo));

        // when
        MigrationExecutionSummary summary = executor.execute();

        //then
        assertThat(summary.status(), equalTo(FAILURE));
        assertThat(summary.isSuccessful(), equalTo(false));
        assertThat(summary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(summary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(summary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));

        ImmutableList<StepExecutionSummary> stages = summary.stages();
        assertThat(stages, hasSize(2));
        StepExecutionSummary stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.status(), equalTo(OK));
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        stepExecutionSummary = stages.get(1);
        assertThat(stepExecutionSummary.status(), equalTo(INDICES_NOT_FOUND_ERROR));
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_2));
        assertThat(stepExecutionSummary.number(), equalTo(1L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_2));

        verify(repository, times(2)).upsert(eq("migration_8_8_0"), summaryCaptor.capture());
        List<MigrationExecutionSummary> persistedSummaries = summaryCaptor.getAllValues();
        assertThat(persistedSummaries, hasSize(2));
        // persist invocation after execution of the first step
        MigrationExecutionSummary stepSummary = persistedSummaries.get(0);
        assertThat(stepSummary.status(), equalTo(IN_PROGRESS));
        assertThat(stepSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(stepSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(stepSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));
        stages = stepSummary.stages();
        assertThat(stages, hasSize(1));
        stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        assertThat(stepExecutionSummary.status(), equalTo(OK));

        // persist invocation after execution of the second step
        stepSummary = persistedSummaries.get(1);
        assertThat(stepSummary.status(), equalTo(FAILURE));
        assertThat(stepSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(stepSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(stepSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));
        stages = stepSummary.stages();
        assertThat(stages, hasSize(2));
        stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        assertThat(stepExecutionSummary.status(), equalTo(OK));
        stepExecutionSummary = stages.get(1);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_2));
        assertThat(stepExecutionSummary.number(), equalTo(1L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_2));
        assertThat(stepExecutionSummary.status(), equalTo(INDICES_NOT_FOUND_ERROR));
    }

    @Test
    public void shouldMarkMigrationAsFailedInCaseOfUnexpectedErrorInTheLastStep() {
        // given
        MigrationStep stepOne = stepMockWithResult(NAME_1, OK, MESSAGE_1);
        MigrationStep stepTwo = stepMock(NAME_2);
        when(stepTwo.execute(any(DataMigrationContext.class))).thenThrow(new RuntimeException(MESSAGE_FAILURE_1));
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(stepOne, stepTwo));

        // when
        MigrationExecutionSummary summary = executor.execute();

        //then
        assertThat(summary.status(), equalTo(FAILURE));
        assertThat(summary.isSuccessful(), equalTo(false));
        assertThat(summary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(summary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(summary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));

        ImmutableList<StepExecutionSummary> stages = summary.stages();
        assertThat(stages, hasSize(2));
        StepExecutionSummary stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.status(), equalTo(OK));
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        stepExecutionSummary = stages.get(1);
        assertThat(stepExecutionSummary.status(), equalTo(UNEXPECTED_ERROR));
        assertThat(stepExecutionSummary.message(), equalTo("Unexpected error: " + MESSAGE_FAILURE_1));
        assertThat(stepExecutionSummary.number(), equalTo(1L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_2));

        verify(repository, times(2)).upsert(eq("migration_8_8_0"), summaryCaptor.capture());
        List<MigrationExecutionSummary> persistedSummaries = summaryCaptor.getAllValues();
        assertThat(persistedSummaries, hasSize(2));
        // persist invocation after execution of the first step
        MigrationExecutionSummary stepSummary = persistedSummaries.get(0);
        assertThat(stepSummary.status(), equalTo(IN_PROGRESS));
        assertThat(stepSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(stepSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(stepSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));
        stages = stepSummary.stages();
        assertThat(stages, hasSize(1));
        stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        assertThat(stepExecutionSummary.status(), equalTo(OK));

        // persist invocation after execution of the second step
        stepSummary = persistedSummaries.get(1);
        assertThat(stepSummary.status(), equalTo(FAILURE));
        assertThat(stepSummary.startTime(), equalTo(NOW.toLocalDateTime()));
        assertThat(stepSummary.backupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_00"));
        assertThat(stepSummary.tempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_00"));
        stages = stepSummary.stages();
        assertThat(stages, hasSize(2));
        stepExecutionSummary = stages.get(0);
        assertThat(stepExecutionSummary.message(), equalTo(MESSAGE_1));
        assertThat(stepExecutionSummary.number(), equalTo(0L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_1));
        assertThat(stepExecutionSummary.status(), equalTo(OK));
        stepExecutionSummary = stages.get(1);
        assertThat(stepExecutionSummary.message(), equalTo("Unexpected error: " + MESSAGE_FAILURE_1));
        assertThat(stepExecutionSummary.number(), equalTo(1L));
        assertThat(stepExecutionSummary.name(), equalTo(NAME_2));
        assertThat(stepExecutionSummary.status(), equalTo(UNEXPECTED_ERROR));
    }

    @Test
    public void shouldExecuteManySteps() {
        List<MigrationStep> stepMocks = IntStream.range(0, 50)//
                .mapToObj(index -> stepMockWithResult("step_" + index, OK, "Step no. " + index)) //
                .collect(Collectors.toList());
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(stepMocks));

        MigrationExecutionSummary migrationSummary = executor.execute();

        assertThat(migrationSummary.status(), equalTo(SUCCESS));
        assertThat(migrationSummary.stages(), hasSize(stepMocks.size()));
        verify(repository, times(stepMocks.size())).upsert(eq("migration_8_8_0"), any(MigrationExecutionSummary.class));
        stepMocks.forEach(stepMock -> verify(stepMock).execute(any(DataMigrationContext.class)));
    }

    @Test
    public void shouldStopExecutionOfLargeAmountOfStepsAfterFirstExpectedFailure() {
        List<MigrationStep> stepMocks = IntStream.range(0, 49)//
            .mapToObj(index -> stepMockWithResult("step_" + index, OK, "Step no. " + index)) //
            .collect(Collectors.toList());
        final int failureStepIndex = 25;
        stepMocks.add(failureStepIndex, stepMockWithResult("Failure step", INDICES_NOT_FOUND_ERROR, MESSAGE_FAILURE_2));
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(stepMocks));

        MigrationExecutionSummary migrationSummary = executor.execute();

        assertThat(migrationSummary.status(), equalTo(FAILURE));
        assertThat(migrationSummary.stages(), hasSize(failureStepIndex + 1));
        StepExecutionSummary failureStepSummary = migrationSummary.stages().get(failureStepIndex);
        assertThat(failureStepSummary.status(),equalTo(INDICES_NOT_FOUND_ERROR));
        assertThat(failureStepSummary.message(),equalTo(MESSAGE_FAILURE_2));
        verify(repository, times(failureStepIndex + 1)).upsert(eq("migration_8_8_0"), any(MigrationExecutionSummary.class));
        stepMocks.stream().limit(failureStepIndex + 1).forEach(stepMock -> verify(stepMock).execute(any(DataMigrationContext.class)));
        stepMocks.stream().skip(failureStepIndex + 1).forEach(stepMock -> verify(stepMock, never()).execute(any(DataMigrationContext.class)));
    }

    @Test
    public void shouldStopExecutionOfLargeAmountOfStepsAfterFirstUnexpectedFailure() {
        List<MigrationStep> stepMocks = IntStream.range(0, 49)//
            .mapToObj(index -> stepMockWithResult("step_" + index, OK, "Step no. " + index)) //
            .collect(Collectors.toList());
        final int failureStepIndex = 25;
        MigrationStep failureStep = stepMock("Failure step name");
        when(failureStep.execute(any(DataMigrationContext.class))).thenThrow(new IllegalStateException(MESSAGE_FAILURE_1));
        stepMocks.add(failureStepIndex, failureStep);
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(stepMocks));

        MigrationExecutionSummary migrationSummary = executor.execute();

        assertThat(migrationSummary.status(), equalTo(FAILURE));
        assertThat(migrationSummary.stages(), hasSize(failureStepIndex + 1));
        StepExecutionSummary failureStepSummary = migrationSummary.stages().get(failureStepIndex);
        assertThat(failureStepSummary.status(), equalTo(UNEXPECTED_ERROR));
        assertThat(failureStepSummary.message(), containsString(MESSAGE_FAILURE_1));
        verify(repository, times(failureStepIndex + 1)).upsert(eq("migration_8_8_0"), any(MigrationExecutionSummary.class));
        stepMocks.stream().limit(failureStepIndex + 1).forEach(stepMock -> verify(stepMock).execute(any(DataMigrationContext.class)));
        stepMocks.stream().skip(failureStepIndex + 1).forEach(stepMock -> verify(stepMock, never()).execute(any(DataMigrationContext.class)));
    }

    @Test
    public void shouldReturnAnotherMigrationStartTime() {
        ZonedDateTime now = ZonedDateTime.of(LocalDateTime.of(2023, 5, 25, 12, 1), ZoneOffset.UTC);
        this.clock = Clock.fixed(now.toInstant(), ZoneOffset.UTC);
        MigrationStep step = stepMockWithResult("The only step to take", OK, "I am done");
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(step));

        MigrationExecutionSummary migrationSummary = executor.execute();

        assertThat(migrationSummary.startTime(), equalTo(now.toLocalDateTime()));
        assertThat(migrationSummary.stages().get(0).startTime(), equalTo(now.toLocalDateTime()));
        verify(repository, times(1)).upsert(eq("migration_8_8_0"), summaryCaptor.capture());
        MigrationExecutionSummary persistedSummary = summaryCaptor.getValue();
        assertThat(persistedSummary.startTime(), equalTo(now.toLocalDateTime()));
        assertThat(persistedSummary.stages().get(0).startTime(), equalTo(now.toLocalDateTime()));
    }

    @Test
    public void shouldHandleStepExecutionException() {
        String message = "Sth went wrong so exception is needed";
        String details = "Descriptive message";
        MigrationStep step = new ThrowExceptionStep(message, INDICES_NOT_FOUND_ERROR, details);
        MigrationStepsExecutor executor = new MigrationStepsExecutor(STRICT_CONFIG, repository, clock, ImmutableList.of(step));

        MigrationExecutionSummary summary = executor.execute();

        assertThat(summary.status(), equalTo(FAILURE));
        assertThat(summary.stages(), hasSize(1));
        StepExecutionSummary stepSummary = summary.stages().get(0);
        assertThat(stepSummary.status(), equalTo(INDICES_NOT_FOUND_ERROR));
        assertThat(stepSummary.message(), equalTo(message));
        assertThat(stepSummary.details(), equalTo(details));
        verify(repository, times(1)).upsert(eq("migration_8_8_0"), summaryCaptor.capture());
        MigrationExecutionSummary storedSummary = summaryCaptor.getValue();
        assertThat(storedSummary, equalTo(summary));
    }

    private MigrationStep stepMock(String name) {
        MigrationStep step = Mockito.mock(MigrationStep.class);
        when(step.name()).thenReturn(name);
        return step;
    }

    private MigrationStep stepMockWithResult(String name, StepExecutionStatus status, String message) {
        MigrationStep step = stepMock(name);
        when(step.execute(any(DataMigrationContext.class))).thenReturn(new StepResult(status, message));
        return step;
    }

}