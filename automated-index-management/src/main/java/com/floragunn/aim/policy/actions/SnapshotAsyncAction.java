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
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;

import java.util.Objects;

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
            String snapshotNameKey = node.get(SNAPSHOT_NAME_KEY_FIELD).asString();
            return new SnapshotAsyncAction(snapshotNamePrefix, repositoryName, snapshotNameKey);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
            typeValidator.validateOnlyOnceInPolicy();
        }
    };
    public static final String DEFAULT_SNAPSHOT_NAME_KEY = "snapshot_name";

    public static final String SNAPSHOT_NAME_PREFIX_FIELD = "name_prefix";
    public static final String REPOSITORY_NAME_FIELD = "repository";
    public static final String SNAPSHOT_NAME_KEY_FIELD = "snapshot_name_key";

    private final String snapshotNamePrefix;
    private final String repositoryName;
    private final String snapshotNameKey;

    public SnapshotAsyncAction(String snapshotNamePrefix, String repositoryName) {
        this(snapshotNamePrefix, repositoryName, null);
    }

    public SnapshotAsyncAction(String snapshotNamePrefix, String repositoryName, String snapshotNameKey) {
        this.snapshotNamePrefix = snapshotNamePrefix;
        this.repositoryName = repositoryName;
        this.snapshotNameKey = snapshotNameKey;
    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        String snapshotNameExpression = "<" + (snapshotNamePrefix == null ? "" : snapshotNamePrefix + "_") + index + "_{now/d}>";
        String snapshotName = IndexNameExpressionResolver.resolveDateMathExpression(snapshotNameExpression);
        String snapshotNameKey = this.snapshotNameKey != null && !this.snapshotNameKey.isEmpty() ? this.snapshotNameKey : DEFAULT_SNAPSHOT_NAME_KEY;
        CreateSnapshotResponse createSnapshotResponse = executionContext.getClient().admin().cluster()
                .prepareCreateSnapshot(repositoryName, snapshotName).setIndices(index).setWaitForCompletion(false).get();
        if (createSnapshotResponse.status() == OK || createSnapshotResponse.status() == ACCEPTED) {
            LOG.debug("Starting snapshot creation for index '{}' successful", index);
        } else {
            LOG.debug("Starting snapshot creation for index '{}' failed with response: \n{}", index, createSnapshotResponse.toString());
            throw new IllegalStateException("Snapshot creation finally failed");
        }
        state.addCreatedSnapshotName(snapshotNameKey, snapshotName);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SnapshotAsyncAction)) {
            return false;
        }
        SnapshotAsyncAction otherSnapshotAsyncAction = (SnapshotAsyncAction) other;
        return Objects.equals(repositoryName, otherSnapshotAsyncAction.repositoryName)
                && Objects.equals(snapshotNamePrefix, otherSnapshotAsyncAction.snapshotNamePrefix);
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
        return new SnapshotCreatedCondition(repositoryName, snapshotNameKey);
    }
}
