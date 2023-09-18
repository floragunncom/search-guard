package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DATA_INDICES_LOCKED_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;

public class CheckIfIndicesAreBlockedStep implements MigrationStep {

    private final StepRepository repository;

    public CheckIfIndicesAreBlockedStep(StepRepository repository) {
        this.repository = Objects.requireNonNull(repository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        try {
            GetSettingsResponse response = repository.getIndexSettings(context.getDataIndicesNames().toArray(String[]::new));
            Map<String, Settings> indexToSettings = response.getIndexToSettings();
            boolean success = true;
            StringBuilder details = new StringBuilder();
            for (String index : context.getDataIndicesNames()) {
                Settings settings = indexToSettings.get(index);
                if (settings == null) {
                    details.append("Settings for index '").append(index).append("' are not available. ");
                    success = false;
                    continue;
                }
                Settings blocks = settings.getAsSettings("index.blocks");
                if (blocks == null) {
                    details.append("Index '").append(index).append("' block settings are not defined. ");
                    continue;
                }
                boolean currentIndexBlocked = false;
                for (String blockType : List.of("read_only", "read_only_allow_delete", "read", "write", "metadata")) {
                    Boolean blocked = blocks.getAsBoolean(blockType, false);
                    if (blocked) {
                        success = false;
                        currentIndexBlocked = true;
                        details.append("Lock '").append(blockType).append("' is active on index '").append(index).append("'. ");
                    }
                }
                if (currentIndexBlocked) {
                    details.append("Index '").append(index).append("' is blocked. ");
                } else {
                    details.append("Index '").append(index).append("' is lock free. ");
                }
            }
            if (success) {
                return new StepResult(OK, "Blocked data index not found", details.toString());
            }
            return new StepResult(DATA_INDICES_LOCKED_ERROR, "Locked indices found", details.toString());
        } catch (ClusterBlockException e) {
            return new StepResult(DATA_INDICES_LOCKED_ERROR, "Locked indices found", e.getMessage());
        }
    }

    @Override
    public String name() {
        return "check if indices are blocked";
    }
}
