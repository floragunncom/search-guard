package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public final class DeleteAction extends Action {
    public static final String TYPE = "delete";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            if (validationContext != null) {
                validationContext.isWriteBlocked(true);
                validationContext.isDeleted(true);
            }
            return new DeleteAction();
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
            typeValidator.validateOnlyOnceInPolicy();
        }
    };

    public DeleteAction() {

    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        AcknowledgedResponse response = executionContext.getClient().admin().indices().execute(DeleteIndexAction.INSTANCE, request).actionGet();
        if (!response.isAcknowledged()) {
            throw new IllegalStateException("Could not delete index. Response was unacknowledged");
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DeleteAction;
    }
}
