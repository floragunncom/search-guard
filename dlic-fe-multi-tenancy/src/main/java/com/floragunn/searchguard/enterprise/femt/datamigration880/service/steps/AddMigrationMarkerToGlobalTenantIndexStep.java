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

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;

class AddMigrationMarkerToGlobalTenantIndexStep implements MigrationStep {

    private final IndexSettingsManager indexSettingsManager;

    public AddMigrationMarkerToGlobalTenantIndexStep(IndexSettingsManager indexSettingsManager) {
        this.indexSettingsManager = Objects.requireNonNull(indexSettingsManager, "Index settings manager is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String indexName = context.getGlobalTenantIndexName();
        if(indexSettingsManager.isMigrationMarkerPresent(indexName)) {
            return new StepResult(OK, "Migration marker already present in index '" + indexName + "'");
        }
        indexSettingsManager.addMigrationMarker(indexName);
        return new StepResult(OK, "Migration marker added to index", "Migration marker added to index '" + indexName + "'");
    }

    @Override
    public String name() {
        return "Add migration marker to global tenant index";
    }
}
