package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.conditions.SnapshotCreatedCondition;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;

import static org.elasticsearch.rest.RestStatus.ACCEPTED;
import static org.elasticsearch.rest.RestStatus.OK;

public final class SnapshotAsyncAction extends Action.Async<SnapshotCreatedCondition> {
    private static final Logger LOG = LogManager.getLogger(SnapshotAsyncAction.class);
    public static final String TYPE = "snapshot";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            String snapshotNamePrefix = node.get(SNAPSHOT_NAME_PREFIX_FIELD).asString();
            String repositoryName = node.get(REPOSITORY_NAME_FIELD).required().asString();
            return new SnapshotAsyncAction(snapshotNamePrefix, repositoryName);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
            typeValidator.validateOnlyOnceInPolicy();
        }
    };
    public static final String SNAPSHOT_NAME_PREFIX_FIELD = "name_prefix";
    public static final String REPOSITORY_NAME_FIELD = "repository";

    private final String snapshotNamePrefix;
    private final String repositoryName;

    public SnapshotAsyncAction(String snapshotNamePrefix, String repositoryName) {
        this.snapshotNamePrefix = snapshotNamePrefix;
        this.repositoryName = repositoryName;
    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        String snapshotNameExpression = "<" + (snapshotNamePrefix == null ? "" : snapshotNamePrefix + "_") + index + "_{now/d}>";
        String snapshotName = IndexNameExpressionResolver.resolveDateMathExpression(snapshotNameExpression);
        state.setSnapshotName(snapshotName);
        CreateSnapshotRequest request = new CreateSnapshotRequest(repositoryName, snapshotName).indices(index).waitForCompletion(false);
        CreateSnapshotResponse createSnapshotResponse = executionContext.getClient().admin().cluster().execute(CreateSnapshotAction.INSTANCE, request)
                .actionGet();
        if (createSnapshotResponse.status() == OK || createSnapshotResponse.status() == ACCEPTED) {
            LOG.debug("Starting snapshot creation for index '" + index + "' successful");
        } else {
            LOG.debug("Starting snapshot creation for index '" + index + "' failed");
            throw new IllegalStateException("Snapshot creation finally failed");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SnapshotAsyncAction)) {
            return false;
        }
        SnapshotAsyncAction otherSnapshotAsyncAction = (SnapshotAsyncAction) other;
        if (!snapshotNamePrefix.equals(otherSnapshotAsyncAction.snapshotNamePrefix)) {
            return false;
        }
        return repositoryName.equals(otherSnapshotAsyncAction.repositoryName);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        ImmutableMap<String, Object> res = ImmutableMap.of(REPOSITORY_NAME_FIELD, repositoryName);
        if (snapshotNamePrefix != null) {
            res = res.with(SNAPSHOT_NAME_PREFIX_FIELD, snapshotNamePrefix);
        }
        return res;
    }

    @Override
    public SnapshotCreatedCondition createCondition() {
        return new SnapshotCreatedCondition(repositoryName);
    }
}
