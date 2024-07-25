package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;

import java.time.Instant;

import static com.floragunn.aim.policy.instance.PolicyInstanceState.Status.*;

public class PolicyInstance implements Runnable {
    private static final Logger LOG = LogManager.getLogger(PolicyInstance.class);

    private final String index;
    private final Policy policy;
    private final PolicyInstanceState state;
    private final ExecutionContext executionContext;

    private volatile boolean retry = false;

    public PolicyInstance(String index, Policy policy, PolicyInstanceState state, ExecutionContext executionContext) {
        this.index = index;
        this.policy = policy;
        this.state = state;
        this.executionContext = executionContext;
    }

    @Override
    public synchronized void run() {
        LOG.trace("Running policy instance for index '{}' with policy '{}' and status '{}'", index, index, state.getStatus());
        switch (state.getStatus()) {
        case FINISHED:
            LOG.debug("Skipping execution because policy instance is finished");
            break;
        case DELETED:
            LOG.debug("Skipping policy instance execution for index '{}' because index is deleted", index);
            break;
        case NOT_STARTED:
            state.setCurrentStepName(policy.getFirstStep().getName());
        case WAITING:
            execute();
            break;
        case FAILED:
            if (isRetry()) {
                isRetry(false);
                retry();
            }
            break;
        case RUNNING:
            LOG.debug("Policy instance for index '{}' with policy '{}' could not start because it is still running", index, index);
            break;
        default:
            LOG.warn("Policy instance for index '{}' is in no step", index);
            break;
        }
    }

    private void retry() {
        try {
            state.setStatus(RUNNING);
            updateState();
            Policy.Step currentStep = policy.getStep(state.getCurrentStepName());
            Action failedAction = null;
            if (state.getLastExecutedActionState() != null && state.getLastExecutedActionState().hasError()) {
                failedAction = currentStep.getActions().stream()
                        .filter(action -> action.getType().equals(state.getLastExecutedActionState().getType())).findFirst().orElse(null);
            }
            if (failedAction != null) {
                retryStep(currentStep, state.getLastExecutedStepState().getRetries() + 1, currentStep.getActions().indexOf(failedAction),
                        state.getLastExecutedActionState().getRetries() + 1);
            } else {
                executeStep(currentStep, state.getLastExecutedStepState().getRetries() + 1);
            }
            if (state.getStatus() == RUNNING) {
                Policy.Step nextStep = policy.getNextStep(currentStep.getName());
                if (nextStep != null) {
                    state.setCurrentStepName(nextStep.getName());
                    execute();
                } else {
                    state.setStatus(FINISHED);
                    updateState();
                }
            } else {
                updateState();
            }
        } catch (Exception e) {
            LOG.warn("Unexpected error while retrying policy instance for index '{}'", index, e);
            state.setLastExecutedStepState(new PolicyInstanceState.StepState("unknown", Instant.now(), 0, e));
            state.setStatus(FAILED);
            updateState();
        }
    }

    private void execute() {
        try {
            state.setStatus(RUNNING);
            updateState();
            while (state.getStatus() == RUNNING) {
                Policy.Step currentStep = policy.getStep(state.getCurrentStepName());
                if (currentStep != null) {
                    executeStep(currentStep, 0);
                } else {
                    LOG.warn("Could not find step to execute for index '{}'", index);
                    state.setLastExecutedStepState(new PolicyInstanceState.StepState("unknown", Instant.now(), 0,
                            new IllegalStateException("Could not find step to execute")));
                    state.setStatus(FAILED);
                    break;
                }
                if (state.getStatus() == RUNNING) {
                    Policy.Step nextStep = policy.getNextStep(currentStep.getName());
                    if (nextStep != null) {
                        state.setCurrentStepName(nextStep.getName());
                    } else {
                        state.setStatus(FINISHED);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Unexpected error while executing policy instance for index '{}'", index, e);
            state.setLastExecutedStepState(new PolicyInstanceState.StepState("unknown", Instant.now(), 0, e));
            state.setStatus(FAILED);
        } finally {
            updateState();
        }
    }

    private void retryStep(Policy.Step step, int retryCount, int actionIndex, int actionRetryCount) {
        Instant startTime = Instant.now();
        try {
            for (Action action : step.getActions().subList(actionIndex, step.getActions().size())) {
                executeAction(action, actionRetryCount);
            }
            state.setLastExecutedStepState(new PolicyInstanceState.StepState(step.getName(), startTime, retryCount, null));
        } catch (Exception e) {
            state.setLastExecutedStepState(new PolicyInstanceState.StepState(step.getName(), startTime, retryCount, e));
            state.setStatus(FAILED);
        }
    }

    private void executeStep(Policy.Step step, int retryCount) {
        Instant startTime = Instant.now();
        try {
            boolean conditionsResult = false;
            if (!step.getConditions().isEmpty()) {
                for (Condition condition : step.getConditions()) {
                    if (executeCondition(condition)) {
                        conditionsResult = true;
                        break;
                    }
                }
            } else {
                conditionsResult = true;
            }
            if (!conditionsResult) {
                state.setStatus(WAITING);
            } else if (!step.getActions().isEmpty()) {
                for (Action action : step.getActions()) {
                    executeAction(action, 0);
                }
            }
            state.setLastExecutedStepState(new PolicyInstanceState.StepState(step.getName(), startTime, retryCount, null));
        } catch (Exception e) {
            state.setLastExecutedStepState(new PolicyInstanceState.StepState(step.getName(), startTime, retryCount, e));
            state.setStatus(FAILED);
        }
    }

    private void executeAction(Action action, int retryCount) throws ExecutionException {
        Instant startTime = Instant.now();
        try {
            action.execute(index, executionContext, state);
            state.setLastExecutedActionState(new PolicyInstanceState.ActionState(action.getType(), startTime, retryCount, null));
        } catch (Exception e) {
            state.setLastExecutedActionState(new PolicyInstanceState.ActionState(action.getType(), startTime, retryCount, e));
            throw new ExecutionException("Action execution failed for action '" + action.getType() + "'");
        }
    }

    private Boolean executeCondition(Condition condition) throws ExecutionException {
        Instant startTime = Instant.now();
        try {
            boolean result = condition.execute(index, executionContext, state);
            state.setLastExecutedConditionState(new PolicyInstanceState.ConditionState(condition.getType(), startTime, result, null));
            return result;
        } catch (Exception e) {
            state.setLastExecutedConditionState(new PolicyInstanceState.ConditionState(condition.getType(), startTime, null, e));
            throw new ExecutionException("Condition execution failed for condition '" + condition.getType() + "'");
        }
    }

    public synchronized void handleDelete() {
        state.setStatus(DELETED);
        updateState();
    }

    private void updateState() {
        LOG.trace("Updating policy instance state:\n{}", state.toPrettyJsonString());
        executionContext.getStateService().updateState(index, state);
    }

    public synchronized void isRetry(boolean retry) {
        this.retry = retry;
    }

    public boolean isRetry() {
        return retry;
    }

    public String getPolicyName() {
        return state.getPolicyName();
    }

    public static class ExecutionException extends Exception {
        public ExecutionException(String message) {
            super(message);
        }
    }

    public static final class ExecutionContext {
        private final ClusterService clusterService;
        private final Client client;
        private final AutomatedIndexManagementSettings aimSettings;
        private final PolicyInstanceService stateService;

        public ExecutionContext(ClusterService clusterService, Client client, AutomatedIndexManagementSettings aimSettings,
                PolicyInstanceService stateService) {
            this.clusterService = clusterService;
            this.client = client;
            this.aimSettings = aimSettings;
            this.stateService = stateService;
        }

        public ClusterService getClusterService() {
            return clusterService;
        }

        public Client getClient() {
            return client;
        }

        public AutomatedIndexManagementSettings getAimSettings() {
            return aimSettings;
        }

        public PolicyInstanceService getStateService() {
            return stateService;
        }
    }
}
