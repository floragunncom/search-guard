package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_PRIORITY;

public final class SetPriorityAction extends Action {
    public static final String TYPE = "set_priority";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            int priority = node.get(PRIORITY_FIELD).required().asInt();
            return new SetPriorityAction(priority);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
        }
    };
    public static final String PRIORITY_FIELD = "priority";

    private final int priority;

    public SetPriorityAction(int priority) {
        this.priority = priority;
    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        Settings.Builder builder = Settings.builder().put(SETTING_PRIORITY, priority);
        setIndexSetting(index, executionContext, builder);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SetPriorityAction)) {
            return false;
        }
        SetPriorityAction setPriorityAction = (SetPriorityAction) other;
        return setPriorityAction.priority == priority;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        return ImmutableMap.of(PRIORITY_FIELD, priority);
    }
}
