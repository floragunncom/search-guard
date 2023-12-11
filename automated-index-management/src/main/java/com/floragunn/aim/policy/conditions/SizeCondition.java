package com.floragunn.aim.policy.conditions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.aim.support.ParsingSupport;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.DocsStats;

public final class SizeCondition extends Condition {
    public static final String TYPE = "size";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Condition parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            ByteSizeValue maxSize = node.get(MAX_SIZE_FIELD).required().byString(ParsingSupport::byteSizeValueParser);
            return new SizeCondition(maxSize);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateIndexWritable();
        }
    };
    public static final String MAX_SIZE_FIELD = "max_size";

    private final ByteSizeValue maxSize;

    public SizeCondition(ByteSizeValue maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public boolean execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        DocsStats docsStats = getIndexStats(index, executionContext).getPrimaries().getDocs();
        return docsStats != null && docsStats.getTotalSizeInBytes() > maxSize.getBytes();
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        return ImmutableMap.of(MAX_SIZE_FIELD, maxSize.getStringRep());
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SizeCondition)) {
            return false;
        }
        SizeCondition otherSizeCondition = (SizeCondition) other;
        return maxSize.equals(otherSizeCondition.maxSize);
    }
}
