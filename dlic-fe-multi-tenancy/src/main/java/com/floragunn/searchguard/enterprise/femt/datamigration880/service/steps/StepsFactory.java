package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.support.PrivilegedConfigClient;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StepsFactory {
    private final PrivilegedConfigClient client;

    public StepsFactory(PrivilegedConfigClient privilegedConfigClient) {
        this.client = Objects.requireNonNull(privilegedConfigClient, "Priviliaged config client is required");
    }

    public ImmutableList<MigrationStep> createSteps() {
        List<MockStep> list = IntStream.range(0, 5) //
            .mapToObj(i -> new MockStep("step_name_" + i, ExecutionStatus.SUCCESS, "Step executed correctly")) //
            .collect(Collectors.toList());
        return ImmutableList.of(list);
    }

    // TODO provide real steps implementation
    private record MockStep(String name, ExecutionStatus status, String message) implements MigrationStep {

        @Override
        public StepResult execute(DataMigrationContext dataMigrationContext) {
            return new StepResult(status, message);
        }
    }
}
