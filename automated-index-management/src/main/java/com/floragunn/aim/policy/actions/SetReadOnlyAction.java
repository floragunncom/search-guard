package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;

public final class SetReadOnlyAction extends Action {
    public static final String TYPE = "set_read_only";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            if (validationContext != null) {
                validationContext.isWriteBlocked(true);
            }
            return new SetReadOnlyAction();
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
            typeValidator.validateOnlyOnceInPolicy();
        }
    };

    public SetReadOnlyAction() {

    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        if (!executionContext.updateIndexSetting(index, SETTING_BLOCKS_WRITE, true)) {
            throw new IllegalStateException("Failed to set index read only. Response was not acknowledged");
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SetReadOnlyAction;
    }
}
