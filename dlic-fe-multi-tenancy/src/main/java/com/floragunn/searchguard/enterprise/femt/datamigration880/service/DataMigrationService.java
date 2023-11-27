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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.FAILURE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_CREATE_STATUS_DOCUMENT_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_UPDATE_STATUS_DOCUMENT_LOCK_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MIGRATION_ALREADY_IN_PROGRESS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.STATUS_INDEX_ALREADY_EXISTS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_CONFLICT;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PRECONDITION_FAILED;

public class DataMigrationService {

    private static final Logger log = LogManager.getLogger(DataMigrationService.class);
    public static final String STAGE_NAME_PRECONDITIONS = "preconditions check";

    private final MigrationStateRepository migrationStateRepository;

    private final StepsFactory stepsFactory;
    private final Clock clock;

    public DataMigrationService(MigrationStateRepository migrationStateRepository, StepsFactory stepsFactory,
        Clock clock) {
        this.migrationStateRepository = Objects.requireNonNull(migrationStateRepository, "Migration state repository is required");
        this.stepsFactory = Objects.requireNonNull(stepsFactory, "Step factory is required");
        this.clock = Objects.requireNonNull(clock, "Clock is required");
    }

    public DataMigrationService(MigrationStateRepository migrationStateRepository, StepsFactory stepsFactory) {
        this(migrationStateRepository, stepsFactory, Clock.systemUTC());
    }

    public StandardResponse migrateData(MigrationConfig config) {
        Objects.requireNonNull(config, "Migration config is required");
        try {
            if (!migrationStateRepository.isIndexCreated()) {
                migrationStateRepository.createIndex();
            }
            return findDataMigrationState() //
                .map(summary -> restartMigration(config, summary)) //
                .orElseGet(() -> performFirstMigrationStart(config));
        } catch (IndexAlreadyExistsException e) {
            String message = """
                    Cannot create index to store migration related data./
                    Possibly another data migration process was started in parallel.
                    """.trim();
            return errorResponse(SC_CONFLICT, STATUS_INDEX_ALREADY_EXISTS_ERROR, message, e);
        }
    }

    public Optional<MigrationExecutionSummary> findDataMigrationState() {
        return migrationStateRepository.findById(MigrationStepsExecutor.MIGRATION_ID);
    }

    private StandardResponse executeMigrationSteps(MigrationConfig config) {
        Objects.requireNonNull(config, "Migration config is required");
        ImmutableList<MigrationStep> steps = stepsFactory.createSteps();
        if(log.isInfoEnabled()) {
            log.info("Front-end data migration step execution order: '{}'", steps.stream() //
                .map(MigrationStep::name) //
                .collect(Collectors.joining(", ")));
        }
        MigrationStepsExecutor executor = new MigrationStepsExecutor(config, migrationStateRepository, clock, steps);
        MigrationExecutionSummary summary = executor.execute();
        int httpStatus = summary.isSuccessful() ? SC_OK : SC_INTERNAL_SERVER_ERROR;
        return new StandardResponse(httpStatus).data(summary);
    }

    private StandardResponse performFirstMigrationStart(MigrationConfig config) {
        Objects.requireNonNull(config, "Migration config is required");
        LocalDateTime now = LocalDateTime.now(clock);
        MigrationExecutionSummary summary = migrationStartedSummary(now, "The first start of data migration process");
        try {
            migrationStateRepository.create(MigrationStepsExecutor.MIGRATION_ID, summary);
            return executeMigrationSteps(config);
        } catch (OptimisticLockException e) {
            String message = "Another migration process has just been started.";
            return errorResponse(SC_PRECONDITION_FAILED, CANNOT_CREATE_STATUS_DOCUMENT_ERROR, message, e);
        }
    }

    private StandardResponse restartMigration(MigrationConfig config, MigrationExecutionSummary migrationSummary) {
        LocalDateTime now = LocalDateTime.now(clock);
        if(migrationSummary.isMigrationInProgress(now)) {
            String message = "Data migration started previously at " + migrationSummary.startTime() +
                " is already in progress. Cannot run more than one migration process at the time.";
            return errorResponse(SC_BAD_REQUEST, MIGRATION_ALREADY_IN_PROGRESS_ERROR, message, null);
        }
        MigrationExecutionSummary restartedMigration = migrationStartedSummary(now, "Migration restarted");
        try {
            migrationStateRepository.updateWithLock(MigrationStepsExecutor.MIGRATION_ID, restartedMigration, migrationSummary.lockData());
            return executeMigrationSteps(config);
        } catch (OptimisticLockException ex) {
            String errorMessage = "Another instance of data migration process is just starting, aborting.";
            return errorResponse(SC_CONFLICT, CANNOT_UPDATE_STATUS_DOCUMENT_LOCK_ERROR, errorMessage, ex);
        }
    }

    private MigrationExecutionSummary migrationStartedSummary(LocalDateTime now, String message) {
        StepExecutionSummary preconditionStage = new StepExecutionSummary(0, now, STAGE_NAME_PRECONDITIONS, OK, message);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(preconditionStage);
        return new MigrationExecutionSummary(now, ExecutionStatus.IN_PROGRESS, null, null, stages);
    }

    private StandardResponse errorResponse(int httpStatus, StepExecutionStatus status, String message, Throwable ex) {
        LocalDateTime now = LocalDateTime.now(clock);
        var stages = ImmutableList.of(new StepExecutionSummary(0, now, STAGE_NAME_PRECONDITIONS, status, message, ex));
        MigrationExecutionSummary summary = new MigrationExecutionSummary(now, FAILURE, null, null, stages);
        return new StandardResponse(httpStatus).data(summary);
    }
}
