package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;

public final class RolloverAction extends Action {
    public static final String TYPE = "rollover";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            return new RolloverAction();
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
            typeValidator.validateOnlyOnceInPolicy();
        }
    };

    public RolloverAction() {

    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        String rolloverAliasSettingKey = executionContext.getAimSettings().getStatic().getRolloverAliasFieldName();
        String alias = executionContext.getClusterService().state().metadata().index(index).getSettings().get(rolloverAliasSettingKey);
        if (alias == null || alias.isEmpty()) {
            throw new IllegalStateException("No rollover alias configured in index settings");
        }
        if (!executionContext.getClusterService().state().metadata().index(index).getAliases().containsKey(alias)) {
            throw new IllegalStateException("Index does not have the rollover alias assigned. Index might be already rolled over");
        }
        RolloverRequest request = new RolloverRequest(alias, null);
        RolloverResponse response = executionContext.getClient().admin().indices()
                .execute(org.elasticsearch.action.admin.indices.rollover.RolloverAction.INSTANCE, request).actionGet();
        if (!response.isRolledOver()) {
            throw new IllegalStateException("Rollover finally failed");
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RolloverAction;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
