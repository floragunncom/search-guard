package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;

public final class CloseAction extends Action {
    public static final String TYPE = "close";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            if (validationContext != null) {
                validationContext.isWriteBlocked(true);
            }
            return new CloseAction();
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
            typeValidator.validateOnlyOnceInPolicy();
        }
    };

    public CloseAction() {

    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        CloseIndexRequest request = new CloseIndexRequest(index);
        CloseIndexResponse response = executionContext.getClient().admin().indices().execute(CloseIndexAction.INSTANCE, request).actionGet();
        if (!response.isAcknowledged()) {
            throw new IllegalStateException("Could not close index. Response was unacknowledged");
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof CloseAction;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
