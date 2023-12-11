package com.floragunn.aim.policy.instance;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;

import java.time.Instant;
import java.util.Objects;

public final class PolicyInstanceState implements Document<Object> {

    public static final String POLICY_NAME_FIELD = "policy_name";
    public static final String STATUS_FIELD = "status";
    public static final String CURRENT_STEP_FIELD = "current_step";
    public static final String LAST_EXECUTED_STEP_FIELD = "last_executed_step";
    public static final String LAST_EXECUTED_CONDITION_FIELD = "last_executed_condition";
    public static final String LAST_EXECUTED_ACTION_FIELD = "last_executed_action";
    public static final String SNAPSHOT_NAME = "snapshot_name";

    private final String policyName;
    private Status status = Status.NOT_STARTED;
    private String currentStepName = "none";
    private StepState lastExecutedStepState = null;
    private ConditionState lastExecutedConditionState = null;
    private ActionState lastExecutedActionState = null;
    private String snapshotName = null;

    public PolicyInstanceState(String policyName) {
        this.policyName = policyName;
    }

    public PolicyInstanceState(DocNode docNode) throws ConfigValidationException {
        ValidationErrors errors = new ValidationErrors();
        ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
        policyName = node.get(POLICY_NAME_FIELD).required().asString();
        status = node.get(STATUS_FIELD).required().asEnum(Status.class);
        currentStepName = node.get(CURRENT_STEP_FIELD).required().asString();
        lastExecutedStepState = node.get(LAST_EXECUTED_STEP_FIELD)
                .by((Parser<StepState, Parser.Context>) (docNode1, context) -> new StepState(docNode1));
        lastExecutedConditionState = node.get(LAST_EXECUTED_CONDITION_FIELD)
                .by((Parser<ConditionState, Parser.Context>) (docNode1, context) -> new ConditionState(docNode1));
        lastExecutedActionState = node.get(LAST_EXECUTED_ACTION_FIELD)
                .by((Parser<ActionState, Parser.Context>) (docNode1, context) -> new ActionState(docNode1));
        snapshotName = node.get(SNAPSHOT_NAME).asString();
    }

    @Override
    public Object toBasicObject() {
        ImmutableMap<String, Object> res = ImmutableMap.of(POLICY_NAME_FIELD, policyName, STATUS_FIELD, status.name(), CURRENT_STEP_FIELD,
                currentStepName);
        if (snapshotName != null) {
            res = res.with(SNAPSHOT_NAME, snapshotName);
        }
        if (lastExecutedStepState != null) {
            res = res.with(LAST_EXECUTED_STEP_FIELD, lastExecutedStepState.toBasicObject());
        }
        if (lastExecutedConditionState != null) {
            res = res.with(LAST_EXECUTED_CONDITION_FIELD, lastExecutedConditionState.toBasicObject());
        }
        if (lastExecutedActionState != null) {
            res = res.with(LAST_EXECUTED_ACTION_FIELD, lastExecutedActionState.toBasicObject());
        }
        return res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PolicyInstanceState state = (PolicyInstanceState) o;
        return Objects.equals(policyName, state.policyName) && status == state.status && Objects.equals(currentStepName, state.currentStepName)
                && Objects.equals(lastExecutedStepState, state.lastExecutedStepState)
                && Objects.equals(lastExecutedConditionState, state.lastExecutedConditionState)
                && Objects.equals(lastExecutedActionState, state.lastExecutedActionState) && Objects.equals(snapshotName, state.snapshotName);
    }

    @Override
    public String toString() {
        return "PolicyInstanceState{" + "policyName='" + policyName + '\'' + ", status=" + status + ", currentStepName='" + currentStepName + '\''
                + ", lastExecutedStepState=" + lastExecutedStepState + ", lastExecutedConditionState=" + lastExecutedConditionState
                + ", lastExecutedActionState=" + lastExecutedActionState + ", snapshotName='" + snapshotName + '\'' + '}';
    }

    public String getPolicyName() {
        return policyName;
    }

    public Status getStatus() {
        return status;
    }

    public String getCurrentStepName() {
        return currentStepName;
    }

    public StepState getLastExecutedStepState() {
        return lastExecutedStepState;
    }

    public ConditionState getLastExecutedConditionState() {
        return lastExecutedConditionState;
    }

    public ActionState getLastExecutedActionState() {
        return lastExecutedActionState;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public PolicyInstanceState setStatus(Status status) {
        this.status = status;
        return this;
    }

    public PolicyInstanceState setCurrentStepName(String currentStepName) {
        this.currentStepName = currentStepName;
        return this;
    }

    public PolicyInstanceState setLastExecutedStepState(StepState lastExecutedStepState) {
        this.lastExecutedStepState = lastExecutedStepState;
        return this;
    }

    public PolicyInstanceState setLastExecutedConditionState(ConditionState conditionState) {
        lastExecutedConditionState = conditionState;
        return this;
    }

    public PolicyInstanceState setLastExecutedActionState(ActionState lastExecutedActionState) {
        this.lastExecutedActionState = lastExecutedActionState;
        return this;
    }

    public PolicyInstanceState setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
        return this;
    }

    public enum Status {
        NOT_STARTED, RUNNING, WAITING, FINISHED, FAILED, DELETED
    }

    public static class StepState implements Document<Object> {
        public static final String NAME_FIELD = "name";
        public static final String START_TIME_FIELD = "start_time";
        public static final String RETRY_COUNT_FIELD = "retry_count";
        public static final String ERROR_FIELD = "error";

        private final String name;
        private final Instant startTime;
        private final int retries;
        private final Error error;

        public StepState(String name, Instant startTime, int retries, Exception exception) {
            this.name = name;
            this.startTime = startTime;
            this.retries = retries;
            error = Error.from(exception);
        }

        public StepState(DocNode docNode) throws ConfigValidationException {
            ValidationErrors errors = new ValidationErrors();
            ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
            name = node.get(NAME_FIELD).required().asString();
            startTime = node.get(START_TIME_FIELD).required().byString(Instant::parse);
            retries = node.get(RETRY_COUNT_FIELD).required().asInt();
            error = node.get(ERROR_FIELD).by((Parser<Error, Parser.Context>) (errorNode, context) -> new Error(errorNode));
            node.checkForUnusedAttributes();
            errors.throwExceptionForPresentErrors();
        }

        @Override
        public Object toBasicObject() {
            ImmutableMap<String, Object> res = ImmutableMap.of(NAME_FIELD, name, START_TIME_FIELD, startTime.toString(), RETRY_COUNT_FIELD, retries);
            if (error != null) {
                res = res.with(ERROR_FIELD, error.toBasicObject());
            }
            return res;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            StepState stepState = (StepState) o;
            return retries == stepState.retries && Objects.equals(name, stepState.name) && Objects.equals(startTime, stepState.startTime)
                    && Objects.equals(error, stepState.error);
        }

        @Override
        public String toString() {
            return "StepState{" + "stepName='" + name + '\'' + ", startTime=" + startTime + ", retries=" + retries + ", error=" + error + '}';
        }

        public String getName() {
            return name;
        }

        public Instant getStartTime() {
            return startTime;
        }

        public int getRetries() {
            return retries;
        }

        public boolean hasError() {
            return error != null;
        }
    }

    public static class ConditionState implements Document<Object> {
        public static final String TYPE_FIELD = "type";
        public static final String START_TIME_FIELD = "start_time";
        public static final String RESULT_FIELD = "result";
        public static final String ERROR_FIELD = "error";

        private final String type;
        private final Instant startTime;
        private final Boolean result;
        private final PolicyInstanceState.Error error;

        public ConditionState(String type, Instant startTime, Boolean result, Exception exception) {
            this.type = type;
            this.startTime = startTime;
            this.result = result;
            this.error = Error.from(exception);
        }

        public ConditionState(DocNode docNode) throws ConfigValidationException {
            ValidationErrors errors = new ValidationErrors();
            ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
            type = node.get(TYPE_FIELD).required().asString();
            startTime = node.get(START_TIME_FIELD).required().byString(Instant::parse);
            result = node.get(RESULT_FIELD).asBoolean();
            error = node.get(ERROR_FIELD)
                    .by((Parser<PolicyInstanceState.Error, Parser.Context>) (docNode1, context) -> new PolicyInstanceState.Error(docNode1));
            node.checkForUnusedAttributes();
            errors.throwExceptionForPresentErrors();
        }

        @Override
        public Object toBasicObject() {
            ImmutableMap<String, Object> res = ImmutableMap.of(TYPE_FIELD, type, START_TIME_FIELD, startTime.toString(), RESULT_FIELD, result);
            if (error != null) {
                res = res.with(ERROR_FIELD, error.toBasicObject());
            }
            return res;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ConditionState that = (ConditionState) o;
            return Objects.equals(type, that.type) && Objects.equals(startTime, that.startTime) && Objects.equals(result, that.result)
                    && Objects.equals(error, that.error);
        }

        @Override
        public String toString() {
            return "ConditionState{" + "type='" + type + '\'' + ", startTime=" + startTime + ", result=" + result + ", error=" + error + '}';
        }

        public boolean hasError() {
            return error != null;
        }

        public Instant getStartTime() {
            return startTime;
        }

        public String getType() {
            return type;
        }
    }

    public static class ActionState implements Document<Object> {
        public static final String TYPE_FIELD = "type";
        public static final String START_TIME_FIELD = "start_time";
        public static final String RETRIES_FIELD = "retries";
        public static final String ERROR_FIELD = "error";

        private final String type;
        private final Instant startTime;
        private final int retries;
        private final Error error;

        public ActionState(String type, Instant startTime, int retries, Exception exception) {
            this.type = type;
            this.startTime = startTime;
            this.retries = retries;
            error = Error.from(exception);
        }

        public ActionState(DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors errors = new ValidationErrors();
            ValidatingDocNode node = new ValidatingDocNode(jsonNode, errors);
            type = node.get(TYPE_FIELD).required().asString();
            startTime = node.get(START_TIME_FIELD).required().byString(Instant::parse);
            retries = node.get(RETRIES_FIELD).required().asInt();
            error = node.get(ERROR_FIELD).by((Parser<Error, Parser.Context>) (errorNode, context) -> new Error(errorNode));
            node.checkForUnusedAttributes();
            errors.throwExceptionForPresentErrors();
        }

        @Override
        public Object toBasicObject() {
            ImmutableMap<String, Object> res = ImmutableMap.of(TYPE_FIELD, type, START_TIME_FIELD, startTime.toString(), RETRIES_FIELD, retries);
            if (error != null) {
                res = res.with(ERROR_FIELD, error.toBasicObject());
            }
            return res;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ActionState that = (ActionState) o;
            return retries == that.retries && Objects.equals(type, that.type) && Objects.equals(startTime, that.startTime)
                    && Objects.equals(error, that.error);
        }

        @Override
        public String toString() {
            return "ActionState{" + "type='" + type + '\'' + ", startTime=" + startTime + ", retries=" + retries + ", error=" + error + '}';
        }

        public boolean hasError() {
            return error != null;
        }

        public Instant getStartTime() {
            return startTime;
        }

        public String getType() {
            return type;
        }

        public int getRetries() {
            return retries;
        }
    }

    public static class Error implements Document<Object> {
        public static Error from(Exception exception) {
            return exception == null ? null : new Error(exception);
        }

        public static final String TYPE_FIELD = "type";
        public static final String MESSAGE_FIELD = "message";

        private final String type;
        private final String message;

        public Error(Exception exception) {
            type = exception.getClass().getSimpleName();
            message = exception.getMessage();
        }

        public Error(DocNode docNode) throws ConfigValidationException {
            ValidationErrors errors = new ValidationErrors();
            ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
            type = node.get(TYPE_FIELD).required().asString();
            message = node.get(MESSAGE_FIELD).required().asString();
            node.checkForUnusedAttributes();
            errors.throwExceptionForPresentErrors();
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of(TYPE_FIELD, type, MESSAGE_FIELD, message);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Error error = (Error) o;
            return Objects.equals(type, error.type) && Objects.equals(message, error.message);
        }

        @Override
        public String toString() {
            return "Error{" + "type='" + type + '\'' + ", message='" + message + '\'' + '}';
        }
    }
}
