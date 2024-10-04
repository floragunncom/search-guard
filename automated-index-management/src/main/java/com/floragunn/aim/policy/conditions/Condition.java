package com.floragunn.aim.policy.conditions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.rest.RestStatus.OK;

public abstract class Condition implements Document<Object> {
    public static final String TYPE_FIELD = "type";

    public abstract boolean execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception;

    public abstract boolean equals(Object other);

    public abstract String getType();

    protected ImmutableMap<String, Object> configToBasicMap() {
        return ImmutableMap.empty();
    }

    @Override
    public final Object toBasicObject() {
        ImmutableMap<String, Object> res = ImmutableMap.of(TYPE_FIELD, getType());
        return res.with(configToBasicMap());
    }

    public IndicesStatsResponse getIndexStats(String index, PolicyInstance.ExecutionContext executionContext) {
        IndicesStatsResponse response = executionContext.getClient().admin().indices().prepareStats(index).clear().setDocs(true).get();
        if (response.getStatus() != OK) {
            throw new IllegalStateException("Failed to get index settings. Response was not OK, shards failed");
        }
        return response;
    }

    public static abstract class Async extends Condition {
        public abstract String getStepName();
    }

    protected static class Validator {
        private final DocNode docNode;
        private final ValidationErrors errors;
        private final Policy.ValidationContext validationContext;

        private Validator(DocNode docNode, ValidationErrors errors, Policy.ValidationContext validationContext) {
            this.docNode = docNode;
            this.errors = errors;
            this.validationContext = validationContext;
        }

        public void validateType(String type, Condition.ValidatingParser validatingParser) {
            if (validationContext != null && type != null) {
                validatingParser.validateType(new TypeValidator(type));
                validationContext.addExecutable(type);
            }
        }

        public class TypeValidator {
            private final String type;

            private TypeValidator(String type) {
                this.type = type;
            }

            public void validateIndexNotDeleted() {
                if (validationContext.isDeleted()) {
                    errors.add(new InvalidAttributeValue(Condition.TYPE_FIELD, type, "No actions or conditions after delete", docNode));
                }
            }

            public void validateIndexWritable() {
                if (validationContext.isWriteBlocked()) {
                    errors.add(new InvalidAttributeValue(Condition.TYPE_FIELD, type, "No write blocking actions before", docNode));
                }
            }

            public void validateNotConfigurable() {
                errors.add(new InvalidAttributeValue(Condition.TYPE_FIELD, type, "Condition type", docNode));
            }
        }
    }

    public interface ValidatingParser {
        Condition parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext);

        void validateType(Validator.TypeValidator typeValidator);
    }

    public static class Factory {
        public static Factory defaultFactory() {
            Factory factory = new Factory();
            factory.register(AgeCondition.TYPE, AgeCondition.VALIDATING_PARSER);
            factory.register(DocCountCondition.TYPE, DocCountCondition.VALIDATING_PARSER);
            factory.register(ForceMergeDoneCondition.TYPE, ForceMergeDoneCondition.VALIDATING_PARSER);
            factory.register(IndexCountCondition.TYPE, IndexCountCondition.VALIDATING_PARSER);
            factory.register(SizeCondition.TYPE, SizeCondition.VALIDATING_PARSER);
            factory.register(SnapshotCreatedCondition.TYPE, SnapshotCreatedCondition.VALIDATING_PARSER);
            return factory;
        }

        private final Map<String, ValidatingParser> registry;

        private Factory() {
            registry = new ConcurrentHashMap<>();
        }

        public void register(String type, ValidatingParser parser) {
            if (registry.containsKey(type)) {
                throw new IllegalArgumentException("Action of type '" + type + "' is already registered");
            }
            registry.put(type, parser);
        }

        public Condition parse(DocNode docNode, Policy.ValidationContext validationContext) throws ConfigValidationException {
            ValidationErrors errors = new ValidationErrors();
            Validator validator = new Validator(docNode, errors, validationContext);
            ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
            String type = node.get(TYPE_FIELD).required().asString();
            Condition result = null;
            if (!registry.containsKey(type)) {
                errors.add(new InvalidAttributeValue(TYPE_FIELD, type, "condition type", docNode));
            } else {
                ValidatingParser validatingParser = registry.get(type);
                validator.validateType(type, validatingParser);
                result = validatingParser.parse(node, errors, validationContext);
            }
            node.checkForUnusedAttributes();
            errors.throwExceptionForPresentErrors();
            return result;
        }

        public boolean containsType(String type) {
            return registry.containsKey(type);
        }
    }
}
