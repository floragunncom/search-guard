package com.floragunn.aim.policy;

import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.aim.policy.conditions.ForceMergeDoneCondition;
import com.floragunn.aim.policy.conditions.SnapshotCreatedCondition;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class Policy implements Document<Object> {
    public static final String STEPS_FIELD = "steps";

    public static Policy parse(DocNode docNode, ParsingContext parsingContext) throws ConfigValidationException {
        ValidationErrors errors = new ValidationErrors();
        ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
        List<Step> steps = node.get(STEPS_FIELD).required().asList(stepNode -> Step.parse(stepNode, parsingContext));
        if (parsingContext.validationContext() != null) {
            if (steps.isEmpty()) {
                errors.add(new InvalidAttributeValue(STEPS_FIELD, null, "At least one step", docNode));
            }
            List<Step> tmp = new LinkedList<>();
            for (Step step : steps) {
                tmp.add(step);
                Action.Async<?> asyncAction = step.getAsyncAction();
                if (asyncAction != null) {
                    Condition.Async condition = asyncAction.createCondition();
                    tmp.add(new Step(condition.getStepName(), ImmutableList.of(condition), ImmutableList.empty()));
                }
            }
            steps = tmp;
        }
        node.checkForUnusedAttributes();
        errors.throwExceptionForPresentErrors();
        return new Policy(steps);
    }

    private final ImmutableList<Step> steps;

    public Policy(Step... steps) {
        this.steps = ImmutableList.ofArray(steps);
    }

    public Policy(List<Step> steps) {
        this.steps = ImmutableList.of(steps);
    }

    public Step getNextStep(String name) {
        for (int i = 0; i < (steps.size() - 1); i++) {
            if (steps.get(i).getName().equals(name)) {
                return steps.get(i + 1);
            }
        }
        return null;
    }

    public Step getFirstStep() {
        return steps.get(0);
    }

    public Step getStep(String name) {
        return steps.stream().filter(step -> step.getName().equals(name)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Step named '" + name + "' could not be found in policy"));
    }

    public boolean equals(Object other) {
        if (!(other instanceof Policy)) {
            return false;
        }
        Policy otherPolicy = (Policy) other;
        return steps.equals(otherPolicy.steps);
    }

    @Override
    public Object toBasicObject() {
        return ImmutableMap.of(STEPS_FIELD, steps.stream().map(Step::toBasicObject).collect(Collectors.toList()));
    }

    public Object toBasicObjectExcludeInternal() {
        return ImmutableMap.of(STEPS_FIELD, steps.stream()
                .filter(step -> !ImmutableList.of(ForceMergeDoneCondition.STEP_NAME, SnapshotCreatedCondition.STEP_NAME).contains(step.getName()))
                .map(Step::toBasicObject).collect(Collectors.toList()));
    }

    public static class Step implements Document<Object> {
        public final static String NAME_FIELD = "name";
        public static final String CONDITIONS_FIELD = "conditions";
        public static final String ACTIONS_FIELD = "actions";

        public static Step parse(DocNode docNode, ParsingContext parsingContext) throws ConfigValidationException {
            ValidationErrors errors = new ValidationErrors();
            Validator validator = new Validator(docNode, errors, parsingContext.validationContext());
            ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
            String name = node.get(NAME_FIELD).required().asString();
            validator.validateName(name);
            List<Condition> conditions = node.get(CONDITIONS_FIELD).asList(parsingContext::parseCondition);
            List<Action> actions = node.get(ACTIONS_FIELD).asList(parsingContext::parseAction);
            validator.validateNotEmpty(conditions, actions);
            node.checkForUnusedAttributes();
            errors.throwExceptionForPresentErrors();
            return new Step(name, conditions, actions);
        }

        private final String name;
        private final List<Condition> conditions;
        private final List<Action> actions;

        public Step(String name, List<Condition> conditions, List<Action> actions) {
            this.name = name;
            this.conditions = conditions;
            this.actions = actions;
        }

        public String getName() {
            return name;
        }

        public List<Condition> getConditions() {
            return conditions;
        }

        public List<Action> getActions() {
            return actions;
        }

        private Action.Async<?> getAsyncAction() {
            for (Action action : actions) {
                if (action instanceof Action.Async) {
                    return (Action.Async<?>) action;
                }
            }
            return null;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of(NAME_FIELD, name, CONDITIONS_FIELD, conditions.stream().map(Condition::toBasicObject).collect(Collectors.toList()),
                    ACTIONS_FIELD, actions.stream().map(Action::toBasicObject).collect(Collectors.toList()));
        }

        public boolean equals(Object other) {
            if (!(other instanceof Step)) {
                return false;
            }
            Step otherStep = (Step) other;
            if (!name.equals(otherStep.name)) {
                return false;
            }
            if (!conditions.equals(otherStep.conditions)) {
                return false;
            }
            return actions.equals(otherStep.actions);
        }

        private static class Validator {
            private static final List<String> ILLEGAL_STEP_NAMES = ImmutableList.of("", ForceMergeDoneCondition.STEP_NAME,
                    SnapshotCreatedCondition.STEP_NAME);
            private final DocNode docNode;
            private final ValidationErrors errors;
            private final ValidationContext validationContext;

            public Validator(DocNode docNode, ValidationErrors errors, ValidationContext validationContext) {
                this.docNode = docNode;
                this.errors = errors;
                this.validationContext = validationContext;
                if (validationContext != null) {
                    validationContext.stepContext(new ValidationContext.StepContext());
                }
            }

            public void validateName(String name) {
                if (validationContext != null && name != null) {
                    if (validationContext.containsStepName(name)) {
                        errors.add(new InvalidAttributeValue(Step.NAME_FIELD, name, "No duplicates", docNode));
                    }
                    if (ILLEGAL_STEP_NAMES.contains(name)) {
                        errors.add(new InvalidAttributeValue(Step.NAME_FIELD, name, "Legal name", docNode));
                    }
                    validationContext.addStepName(name);
                }
            }

            public void validateNotEmpty(List<Condition> conditions, List<Action> actions) {
                if (validationContext != null && (conditions != null && actions != null)) {
                    if (conditions.isEmpty() && actions.isEmpty()) {
                        errors.add(new InvalidAttributeValue(Step.CONDITIONS_FIELD + "|" + Step.ACTIONS_FIELD, null,
                                "At least one condition or action", docNode));
                    }
                }
            }
        }
    }

    public static class ParsingContext {
        public static ParsingContext lenient(Condition.Factory conditionFactory, Action.Factory actionFactory) {
            return new ParsingContext(null, conditionFactory, actionFactory);
        }

        public static ParsingContext strict(Condition.Factory conditionFactory, Action.Factory actionFactory) {
            return new ParsingContext(new ValidationContext(), conditionFactory, actionFactory);
        }

        private final ValidationContext validationContext;
        private final Condition.Factory conditionFactory;
        private final Action.Factory actionFactory;

        private ParsingContext(ValidationContext validationContext, Condition.Factory conditionFactory, Action.Factory actionFactory) {
            this.validationContext = validationContext;
            this.conditionFactory = conditionFactory;
            this.actionFactory = actionFactory;
        }

        public ValidationContext validationContext() {
            return validationContext;
        }

        public Condition parseCondition(DocNode docNode) throws ConfigValidationException {
            return conditionFactory.parse(docNode, validationContext);
        }

        public Action parseAction(DocNode docNode) throws ConfigValidationException {
            return actionFactory.parse(docNode, validationContext);
        }
    }

    public static class ValidationContext {
        private final List<String> stepNames = new LinkedList<>();
        private final List<String> executables = new LinkedList<>();
        private boolean writeBlocked = false;
        private boolean deleted = false;
        private StepContext stepContext;

        public StepContext stepContext() {
            return stepContext;
        }

        public void stepContext(StepContext stepContext) {
            this.stepContext = stepContext;
        }

        public boolean isWriteBlocked() {
            return writeBlocked;
        }

        public void isWriteBlocked(boolean writeBlocked) {
            this.writeBlocked = writeBlocked;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public void isDeleted(boolean deleted) {
            this.deleted = deleted;
        }

        public boolean containsStepName(String name) {
            return stepNames.contains(name);
        }

        public void addStepName(String name) {
            stepNames.add(name);
        }

        public boolean containsExecutable(String executable) {
            return executables.contains(executable);
        }

        public void addExecutable(String executable) {
            executables.add(executable);
        }

        public static class StepContext {
            private boolean hasAsyncAction;

            public boolean hasAsyncAction() {
                return hasAsyncAction;
            }

            public void hasAsyncAction(boolean hasAsyncAction) {
                this.hasAsyncAction = hasAsyncAction;
            }
        }
    }
}
