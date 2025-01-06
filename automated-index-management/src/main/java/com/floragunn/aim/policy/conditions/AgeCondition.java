package com.floragunn.aim.policy.conditions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.aim.support.ParsingSupport;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.core.TimeValue;

import java.time.Instant;

public final class AgeCondition extends Condition {
    public static final String TYPE = "age";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Condition parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            TimeValue maxAge = node.get(MAX_AGE_FIELD).required().byString(ParsingSupport::timeValueParser);
            return new AgeCondition(maxAge);
        }

        @Override
        public void validateType(Validator.TypedValidator typedValidator) {
            typedValidator.validateIndexNotDeleted();
        }
    };
    public static final String MAX_AGE_FIELD = "max_age";

    private final TimeValue maxAge;

    public AgeCondition(TimeValue maxAge) {
        this.maxAge = maxAge;
    }

    @Override
    public boolean execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        long creationDate = executionContext.getClusterService().state().metadata().index(index).getCreationDate();
        long elapsedTime = Instant.now().toEpochMilli() - creationDate;
        return elapsedTime > maxAge.millis();
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        return ImmutableMap.of(MAX_AGE_FIELD, maxAge.getStringRep());
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AgeCondition)) {
            return false;
        }
        AgeCondition otherAgeCondition = (AgeCondition) other;
        return maxAge.equals(otherAgeCondition.maxAge);
    }
}
