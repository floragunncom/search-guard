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

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_CONTAINS_MIGRATED_DATA_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_DOES_NOT_EXIST_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_IS_EMPTY_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;

class VerifyPreviousBackupStep implements MigrationStep {

    private final StepRepository repository;
    private final IndexSettingsManager indexSettingsManager;

    public VerifyPreviousBackupStep(StepRepository repository, IndexSettingsManager settingsManager) {
        this.repository = Objects.requireNonNull(repository, "Step repository is required is required");
        this.indexSettingsManager = Objects.requireNonNull(settingsManager, "Index setting manager is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        if(! context.getBackupCreated()) {
            String backupIndexName = context.getNewestExistingBackupIndex() //
                .orElseThrow(() -> new StepException("Backup index not found", BACKUP_NOT_FOUND_ERROR, null));
            repository.findIndexByNameOrAlias(backupIndexName) //
                .orElseThrow(() -> new StepException("Backup index does not exist", BACKUP_DOES_NOT_EXIST_ERROR, "Index '" + backupIndexName
                    + "' does not exist"));
            long numberOfDocuments = repository.countDocuments(backupIndexName);
            if(numberOfDocuments == 0) {
                return new StepResult(BACKUP_IS_EMPTY_ERROR, "Backup index '" + backupIndexName + "' contains '" + numberOfDocuments
                    + "' documents");
            }
            if(indexSettingsManager.isMigrationMarkerPresent(backupIndexName)) {
                String details = "Index name '" + backupIndexName + "'";
                return new StepResult(BACKUP_CONTAINS_MIGRATED_DATA_ERROR, "Backup index contain migrated data", details);
            }
            String details = "Index '" + backupIndexName + "' contains '" + numberOfDocuments + "' documents";
            return new StepResult(OK, "Backup index exists", details);
        }
        return new StepResult(OK, "A backup was created in the course of the current migration process");
    }

    @Override
    public String name() {
        return "Verify previous backup";
    }
}
