package com.floragunn.aim.policy.actions;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.cluster.metadata.IndexAbstraction;

import java.util.Objects;

public final class RolloverAction extends Action {
    public static final String TYPE = "rollover";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            String aliasKey = node.get(ALIAS_KEY_FIELD).asString();
            return new RolloverAction(aliasKey);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
            typeValidator.validateOnlyOnceInPolicy();
        }
    };
    public static final String ALIAS_KEY_FIELD = "alias_key";

    private final String aliasKey;

    public RolloverAction() {
        aliasKey = null;
    }

    public RolloverAction(String aliasKey) {
        this.aliasKey = aliasKey;
    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        IndexAbstraction abstraction = executionContext.getClusterService().state().metadata().getIndicesLookup().get(index);
        String target;
        if (abstraction != null && abstraction.getParentDataStream() != null) {
            if (abstraction.getWriteIndex() == null || !abstraction.getWriteIndex().getName().equals(index)) {
                throw new IllegalStateException("Index is not the write index of the data stream. Index might be already rolled over");
            }
            target = abstraction.getParentDataStream().getName();
        } else {
            String aliasKey = this.aliasKey != null && !this.aliasKey.isEmpty() ? this.aliasKey
                    : AutomatedIndexManagementSettings.Index.DEFAULT_ROLLOVER_ALIAS_KEY;
            AutomatedIndexManagementSettings.Index indexSettings = new AutomatedIndexManagementSettings.Index(
                    executionContext.getClusterService().state().metadata().index(index).getSettings());
            target = indexSettings.getAlias(aliasKey);
            if (target == null || target.isEmpty()) {
                throw new IllegalStateException("No rollover alias configured in index settings");
            }
            if (!executionContext.getClusterService().state().metadata().index(index).getAliases().containsKey(target)) {
                throw new IllegalStateException("Index does not have the rollover alias assigned. Index might be already rolled over");
            }
        }
        RolloverRequest request = new RolloverRequest(target, null);
        RolloverResponse rolloverResponse = executionContext.getClient().admin().indices().rolloverIndex(request).get();
        if (!rolloverResponse.isRolledOver()) {
            throw new IllegalStateException("Rollover finally failed");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RolloverAction)) {
            return false;
        }
        RolloverAction otherRolloverAction = (RolloverAction) other;
        return Objects.equals(aliasKey, otherRolloverAction.aliasKey);
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        return aliasKey != null ? ImmutableMap.of(ALIAS_KEY_FIELD, aliasKey) : ImmutableMap.empty();
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
