package com.floragunn.aim.policy.conditions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Comparator;

public class IndexCountCondition extends Condition {
    public static final String TYPE = "index_count";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Condition parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            String aliasKey = node.get(ALIAS_KEY_FIELD).required().asString();
            int maxCount = node.get(MAX_INDEX_COUNT_FIELD).required().asInt();
            if (validationContext != null && maxCount < 0) {
                errors.add(new InvalidAttributeValue(MAX_INDEX_COUNT_FIELD, maxCount, "value >= 0", node));
            }
            return new IndexCountCondition(aliasKey, maxCount);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
        }
    };

    public static final String ALIAS_KEY_FIELD = "alias_key";
    public static final String MAX_INDEX_COUNT_FIELD = "max_index_count";

    private final String aliasKey;
    private final int maxCount;

    public IndexCountCondition(String aliasKey, int maxCount) {
        this.aliasKey = aliasKey;
        this.maxCount = maxCount;
    }

    @Override
    public boolean execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        Metadata metadata = executionContext.getClusterService().state().metadata();
        Map<String, String> aliases = executionContext.getAimSettings().getStatic().getAliases(metadata.index(index).getSettings());
        String alias = aliases.get(aliasKey);
        if (alias == null || alias.isEmpty()) {
            throw new IllegalStateException("No alias found for key '" + aliasKey + "'");
        }
        Set<Index> aliasedIndices = metadata.aliasedIndices(alias);
        int overlap = aliasedIndices.size() - maxCount;
        if (overlap > 0) {
            List<Index> oldestIndices = aliasedIndices.stream().sorted(Comparator.comparingLong(index1 -> metadata.index(index1).getCreationDate()))
                    .limit(overlap).toList();
            return oldestIndices.stream().anyMatch(index1 -> index.equals(index1.getName()));
        }
        return false;
    }

    @Override
    protected ImmutableMap<String, Object> configToBasicMap() {
        return ImmutableMap.of(ALIAS_KEY_FIELD, aliasKey, MAX_INDEX_COUNT_FIELD, maxCount);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IndexCountCondition)) {
            return false;
        }
        IndexCountCondition otherCondition = (IndexCountCondition) other;
        return Objects.equals(otherCondition.aliasKey, aliasKey) && otherCondition.maxCount == maxCount;
    }
}
