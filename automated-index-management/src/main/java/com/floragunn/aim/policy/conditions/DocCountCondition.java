package com.floragunn.aim.policy.conditions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.index.shard.DocsStats;

public final class DocCountCondition extends Condition {
    public static final String TYPE = "doc_count";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Condition parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            long maxCount = node.get(MAX_DOC_COUNT_FIELD).required().asPrimitiveLong();
            if (validationContext != null && maxCount < 0) {
                errors.add(new InvalidAttributeValue(MAX_DOC_COUNT_FIELD, maxCount, "value >= 0", node));
            }
            return new DocCountCondition(maxCount);
        }

        @Override
        public void validateType(Validator.TypedValidator typedValidator) {
            typedValidator.validateIndexNotDeleted();
        }
    };
    public static final String MAX_DOC_COUNT_FIELD = "max_doc_count";

    private final long maxCount;

    public DocCountCondition(long maxCount) {
        this.maxCount = maxCount;
    }

    @Override
    public boolean execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        IndexStats indexStats = executionContext.getIndexStats(index);
        if (indexStats == null || indexStats.getPrimaries() == null) {
            return false;
        }
        DocsStats docsStats = indexStats.getPrimaries().getDocs();
        return docsStats != null && docsStats.getCount() > maxCount;
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        return ImmutableMap.of(MAX_DOC_COUNT_FIELD, maxCount);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DocCountCondition)) {
            return false;
        }
        DocCountCondition otherDocCountCondition = (DocCountCondition) other;
        return maxCount == otherDocCountCondition.maxCount;
    }
}
