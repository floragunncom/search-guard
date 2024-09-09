package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Action implements Document<Object> {
    public static final String TYPE_FIELD = "type";

    protected static void setIndexSetting(String index, PolicyInstance.ExecutionContext executionContext, Settings.Builder settingsBuilder) {
        AcknowledgedResponse acknowledgedResponse = executionContext.getClient().admin().indices().prepareUpdateSettings(index)
                .setSettings(settingsBuilder).get();
        if (!acknowledgedResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to execute index settings update. Response was not acknowledged");
        }
    }

    public abstract void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception;

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

    public static abstract class Async<ConditionType extends Condition.Async> extends Action {
        public abstract ConditionType createCondition();
    }

    protected static class Validator {
        private static final List<String> ASYNC_ACTION_NAMES = ImmutableList.of(ForceMergeAsyncAction.TYPE, SnapshotAsyncAction.TYPE);
        private final DocNode docNode;
        private final ValidationErrors errors;
        private final Policy.ValidationContext validationContext;

        private Validator(DocNode docNode, ValidationErrors errors, Policy.ValidationContext validationContext) {
            this.docNode = docNode;
            this.errors = errors;
            this.validationContext = validationContext;
        }

        public void validateType(String type, ValidatingParser validatingParser) {
            if (validationContext != null && type != null) {
                validatingParser.validateType(new TypeValidator(type));
                if (ASYNC_ACTION_NAMES.contains(type)) {
                    validationContext.stepContext().hasAsyncAction(true);
                }
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
                    errors.add(new InvalidAttributeValue(Action.TYPE_FIELD, type, "No actions or conditions after delete", docNode));
                }
            }

            public void validateNoReadOnlyActionBeforeInState() {
                if (validationContext.stepContext().hasAsyncAction()) {
                    errors.add(new InvalidAttributeValue(Action.TYPE_FIELD, type, "No action after async action in state", docNode));
                }
            }

            public void validateOnlyOnceInPolicy() {
                if (validationContext.containsExecutable(type)) {
                    errors.add(new InvalidAttributeValue(Action.TYPE_FIELD, type, "Action only once in policy", docNode));
                }
            }

            public void validateIndexBlocked() {
                if (!validationContext.isWriteBlocked()) {
                    errors.add(new InvalidAttributeValue(Action.TYPE_FIELD, type, "Read only action before", docNode));
                }
            }
        }
    }

    public interface ValidatingParser {
        Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext);

        void validateType(Validator.TypeValidator typeValidator);
    }

    public static class Factory {
        public static Factory defaultFactory() {
            Factory factory = new Factory();
            factory.register(AllocationAction.TYPE, AllocationAction.VALIDATING_PARSER);
            factory.register(CloseAction.TYPE, CloseAction.VALIDATING_PARSER);
            factory.register(DeleteAction.TYPE, DeleteAction.VALIDATING_PARSER);
            factory.register(ForceMergeAsyncAction.TYPE, ForceMergeAsyncAction.VALIDATING_PARSER);
            factory.register(RolloverAction.TYPE, RolloverAction.VALIDATING_PARSER);
            factory.register(SetPriorityAction.TYPE, SetPriorityAction.VALIDATING_PARSER);
            factory.register(SetReadOnlyAction.TYPE, SetReadOnlyAction.VALIDATING_PARSER);
            factory.register(SetReplicaCountAction.TYPE, SetReplicaCountAction.VALIDATING_PARSER);
            factory.register(SnapshotAsyncAction.TYPE, SnapshotAsyncAction.VALIDATING_PARSER);
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

        public Action parse(DocNode docNode, Policy.ValidationContext validationContext) throws ConfigValidationException {
            ValidationErrors errors = new ValidationErrors();
            Validator validator = new Validator(docNode, errors, validationContext);
            ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
            String type = node.get(TYPE_FIELD).required().asString();
            Action result = null;
            if (!registry.containsKey(type)) {
                errors.add(new ValidationError(type, "unknown action type"));
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
