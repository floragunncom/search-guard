package com.floragunn.aim.policy.conditions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;

import static com.floragunn.aim.policy.actions.SnapshotAsyncAction.REPOSITORY_NAME_FIELD;

public final class SnapshotCreatedCondition extends Condition.Async {
    private static final Logger LOG = LogManager.getLogger(SnapshotCreatedCondition.class);
    public static final String TYPE = "snapshot_created";
    public static final String STEP_NAME = "awaiting_snapshot";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Condition parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            String repositoryName = node.get(REPOSITORY_NAME_FIELD).required().asString();
            return new SnapshotCreatedCondition(repositoryName);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateNotConfigurable();
        }
    };

    private final String repositoryName;

    public SnapshotCreatedCondition(String repositoryName) {
        this.repositoryName = repositoryName;
    }

    @Override
    public boolean execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        String snapshotName = state.getSnapshotName();
        if (snapshotName == null || snapshotName.isEmpty()) {
            throw new IllegalStateException("Snapshot name not found");
        }
        SnapshotsStatusRequest request = new SnapshotsStatusRequest(repositoryName, new String[] { snapshotName });
        SnapshotsStatusResponse response = executionContext.getClient().admin().cluster().execute(SnapshotsStatusAction.INSTANCE, request)
                .actionGet();
        if (response.getSnapshots().isEmpty()) {
            throw new IllegalStateException("Could not retrieve snapshot status");
        }
        if (response.getSnapshots().size() > 1) {
            LOG.warn("Found multiple snapshots for index '" + index + "' matching request params. Choosing the first");
        }
        SnapshotStatus status = response.getSnapshots().get(0);
        if (status == null) {
            throw new IllegalStateException("Snapshot status is null");
        }
        switch (status.getState()) {
        case INIT:
        case STARTED:
            return false;
        case SUCCESS:
            return true;
        default:
            throw new IllegalStateException("Snapshot creation failed with status '" + status.getState() + "'");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SnapshotCreatedCondition)) {
            return false;
        }
        SnapshotCreatedCondition condition = (SnapshotCreatedCondition) other;
        return condition.repositoryName.equals(repositoryName);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        return ImmutableMap.of(REPOSITORY_NAME_FIELD, repositoryName);
    }

    @Override
    public String getStepName() {
        return STEP_NAME;
    }
}
