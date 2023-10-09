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

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepException;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.ROLLBACK;

public interface MigrationStep {

    StepResult execute(DataMigrationContext dataMigrationContext) throws StepException;

    String name();

    default StepResult rollback(DataMigrationContext dataMigrationContext) throws StepException {
        return new StepResult(ROLLBACK, "nothing to be rollback");
    }

    default boolean isZero(long...numbers) {
        for(long current : numbers) {
            if(current != 0L) {
                return false;
            }
        }
        return true;
    }

}
