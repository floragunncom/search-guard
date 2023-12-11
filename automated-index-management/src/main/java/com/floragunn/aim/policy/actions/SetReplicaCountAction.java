package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;

public final class SetReplicaCountAction extends Action {
    public static final String TYPE = "set_replica_count";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            int replicaCount = node.get(REPLICA_COUNT_FIELD).required().asInt();
            if (validationContext != null) {
                //todo validate replica count
            }
            return new SetReplicaCountAction(replicaCount);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
        }
    };
    public static final String REPLICA_COUNT_FIELD = "replica_count";

    private final int replicaCount;

    public SetReplicaCountAction(int replicaCount) {
        this.replicaCount = replicaCount;
    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        Settings.Builder builder = Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, replicaCount);
        setIndexSetting(index, executionContext, builder);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SetReplicaCountAction)) {
            return false;
        }
        SetReplicaCountAction setReplicaCountAction = (SetReplicaCountAction) other;
        return setReplicaCountAction.replicaCount == replicaCount;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        return ImmutableMap.of(REPLICA_COUNT_FIELD, replicaCount);
    }
}
