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
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.UNHEALTHY_INDICES_ERROR;

class CheckIndicesStateStep implements MigrationStep {

    private final StepRepository repository;

    public CheckIndicesStateStep(StepRepository repository) {
        this.repository = Objects.requireNonNull(repository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String[] dataAndBackupIndices = context.getDataIndicesNames()
            .with(context.getBackupIndices())
            .toArray(String[]::new);
        StringBuilder stringBuilder = new StringBuilder();
        IndicesStatsResponse response = repository.findIndexState(dataAndBackupIndices);
        boolean success = true;
        for (String index : dataAndBackupIndices) {
            IndexStats indexStats = response.getIndex(index);
            if(indexStats == null) {
                success = false;
                stringBuilder.append("Cannot retrieve index '").append(index).append("' status. ");
                continue;
            }
            ClusterHealthStatus indexStatsHealth = indexStats.getHealth();
            if(context.areYellowDataIndicesAllowed() && ClusterHealthStatus.YELLOW.equals(indexStatsHealth)) {
                continue;
            }
            if(!ClusterHealthStatus.GREEN.equals(indexStatsHealth)) {
                success = false;
                stringBuilder.append("Index '")
                    .append(index)
                    .append("' status is '")
                    .append(indexStatsHealth)
                    .append("' but GREEN status is required. ");
            }
        }
        if(success) {
            String examinedIndices = Arrays.stream(dataAndBackupIndices).map(name -> "'" + name + "'").collect(Collectors.joining(", "));
            return new StepResult(OK, "Indices are healthy", "Examined indices " + examinedIndices);
        } else {
            return new StepResult(UNHEALTHY_INDICES_ERROR, "Unhealthy indices were found", stringBuilder.toString());
        }
    }
    @Override
    public String name() {
        return "check index state";
    }
}
