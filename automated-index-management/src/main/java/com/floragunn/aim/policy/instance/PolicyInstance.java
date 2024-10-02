package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.PolicyService;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.aim.policy.instance.store.InternalJobDetail;
import com.floragunn.aim.policy.instance.store.JobConfigFactory;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobDetailWithBaseConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static com.floragunn.aim.policy.instance.PolicyInstanceState.Status.*;

@DisallowConcurrentExecution
public class PolicyInstance implements Job {
    private static final Logger LOG = LogManager.getLogger(PolicyInstance.class);

    private final Config config;
    private final ExecutionContext executionContext;

    public PolicyInstance(Config config, ExecutionContext executionContext) {
        this.config = config;
        this.executionContext = executionContext;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        LOG.trace("Running policy instance for index '{}' with policy '{}' and status '{}'", config.getIndex(), config.getState().getPolicyName(),
                config.getState().getStatus());
        switch (config.getState().getStatus()) {
        case FINISHED:
            LOG.debug("Skipping execution because policy instance is finished");
            break;
        case DELETED:
            LOG.debug("Skipping policy instance execution for index '{}' because index is deleted", config.getIndex());
            break;
        case NOT_STARTED:
            config.getState().setCurrentStepName(config.getPolicy().getFirstStep().getName());
        case WAITING:
            execute();
            break;
        case FAILED:
            if (config.getState().isRetryOnNextExecution()) {
                LOG.debug("Retrying policy instance '{}'", config.getIndex());
                config.getState().setRetryOnNextExecution(false);
                retry();
            }
            break;
        case RUNNING:
            LOG.debug("Policy instance for index '{}' with policy '{}' could not start because it is still running", config.getIndex(),
                    config.getState().getPolicyName());
            break;
        default:
            LOG.warn("Policy instance for index '{}' is in no step", config.getIndex());
            break;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PolicyInstance) {
            return Objects.equals(config.getIndex(), ((PolicyInstance) obj).config.getIndex());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return config.getIndex().hashCode();
    }

    private void retry() {
        try {
            config.getState().setStatus(RUNNING);
            updateState();
            Policy.Step currentStep = config.getPolicy().getStep(config.getState().getCurrentStepName());
            Action failedAction = null;
            if (config.getState().getLastExecutedActionState() != null && config.getState().getLastExecutedActionState().hasError()) {
                failedAction = currentStep.getActions().stream()
                        .filter(action -> action.getType().equals(config.getState().getLastExecutedActionState().getType())).findFirst().orElse(null);
            }
            if (failedAction != null) {
                retryStep(currentStep, config.getState().getLastExecutedStepState().getRetries() + 1, currentStep.getActions().indexOf(failedAction),
                        config.getState().getLastExecutedActionState().getRetries() + 1);
            } else {
                executeStep(currentStep, config.getState().getLastExecutedStepState().getRetries() + 1);
            }
            if (config.getState().getStatus() == RUNNING) {
                Policy.Step nextStep = config.getPolicy().getNextStep(currentStep.getName());
                if (nextStep != null) {
                    config.getState().setCurrentStepName(nextStep.getName());
                    execute();
                } else {
                    config.getState().setStatus(FINISHED);
                    updateState();
                }
            } else {
                updateState();
            }
        } catch (Exception e) {
            LOG.warn("Unexpected error while retrying policy instance for index '{}'", config.getIndex(), e);
            config.getState().setLastExecutedStepState(new PolicyInstanceState.StepState("unknown", Instant.now(), 0, e));
            config.getState().setStatus(FAILED);
            updateState();
        }
    }

    private void execute() {
        try {
            config.getState().setStatus(RUNNING);
            updateState();
            while (config.getState().getStatus() == RUNNING) {
                Policy.Step currentStep = config.getPolicy().getStep(config.getState().getCurrentStepName());
                if (currentStep != null) {
                    executeStep(currentStep, 0);
                } else {
                    LOG.warn("Could not find step to execute for index '{}'", config.getIndex());
                    config.getState().setLastExecutedStepState(new PolicyInstanceState.StepState("unknown", Instant.now(), 0,
                            new IllegalStateException("Could not find step to execute")));
                    config.getState().setStatus(FAILED);
                    break;
                }
                if (config.getState().getStatus() == RUNNING) {
                    Policy.Step nextStep = config.getPolicy().getNextStep(currentStep.getName());
                    if (nextStep != null) {
                        config.getState().setCurrentStepName(nextStep.getName());
                    } else {
                        config.getState().setStatus(FINISHED);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Unexpected error while executing policy instance for index '{}'", config.getIndex(), e);
            config.getState().setLastExecutedStepState(new PolicyInstanceState.StepState("unknown", Instant.now(), 0, e));
            config.getState().setStatus(FAILED);
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
            config.getState().setLastExecutedStepState(new PolicyInstanceState.StepState(step.getName(), startTime, retryCount, null));
        } catch (Exception e) {
            config.getState().setLastExecutedStepState(new PolicyInstanceState.StepState(step.getName(), startTime, retryCount, e));
            config.getState().setStatus(FAILED);
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
                config.getState().setStatus(WAITING);
            } else if (!step.getActions().isEmpty()) {
                for (Action action : step.getActions()) {
                    executeAction(action, 0);
                }
            }
            config.getState().setLastExecutedStepState(new PolicyInstanceState.StepState(step.getName(), startTime, retryCount, null));
        } catch (Exception e) {
            config.getState().setLastExecutedStepState(new PolicyInstanceState.StepState(step.getName(), startTime, retryCount, e));
            config.getState().setStatus(FAILED);
        }
    }

    private void executeAction(Action action, int retryCount) throws ExecutionException {
        Instant startTime = Instant.now();
        try {
            action.execute(config.getIndex(), executionContext, config.getState());
            config.getState().setLastExecutedActionState(new PolicyInstanceState.ActionState(action.getType(), startTime, retryCount, null));
        } catch (Exception e) {
            config.getState().setLastExecutedActionState(new PolicyInstanceState.ActionState(action.getType(), startTime, retryCount, e));
            throw new ExecutionException("Action execution failed for action '" + action.getType() + "'");
        }
    }

    private Boolean executeCondition(Condition condition) throws ExecutionException {
        Instant startTime = Instant.now();
        try {
            boolean result = condition.execute(config.getIndex(), executionContext, config.getState());
            config.getState().setLastExecutedConditionState(new PolicyInstanceState.ConditionState(condition.getType(), startTime, result, null));
            return result;
        } catch (Exception e) {
            config.getState().setLastExecutedConditionState(new PolicyInstanceState.ConditionState(condition.getType(), startTime, null, e));
            throw new ExecutionException("Condition execution failed for condition '" + condition.getType() + "'");
        }
    }

    private void updateState() {
        config.update(executionContext.getStateService());
    }

    public String getPolicyName() {
        return config.getState().getPolicyName();
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

    public static class Factory implements JobFactory {
        private final ExecutionContext executionContext;

        public Factory(ClusterService clusterService, Client client, AutomatedIndexManagementSettings aimSettings,
                PolicyInstanceService stateService) {
            this.executionContext = new ExecutionContext(clusterService, client, aimSettings, stateService);
        }

        @Override
        public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
            Config config = ((JobDetailWithBaseConfig) bundle.getJobDetail()).getBaseConfig(Config.class);
            LOG.trace("Building new job {} for index {}", config.getJobKey().toString(), config.getJobKey().getName());
            return new PolicyInstance(config, executionContext);
        }
    }

    public static class Config implements JobConfig {
        private final JobKey jobKey;
        private final Collection<Trigger> triggers;
        private final Policy policy;
        private final PolicyInstanceState state;

        private Config(JobKey jobKey, Collection<Trigger> triggers, Policy policy, PolicyInstanceState state) {
            this.jobKey = jobKey;
            this.triggers = triggers;
            this.policy = policy;
            this.state = state;
        }

        @Override
        public long getVersion() {
            return 0;
        }

        @Override
        public String getAuthToken() {
            return null;
        }

        @Override
        public String getSecureAuthTokenAudience() {
            return null;
        }

        @Override
        public JobKey getJobKey() {
            return jobKey;
        }

        @Override
        public String getDescription() {
            return null;
        }

        @Override
        public Class<? extends Job> getJobClass() {
            return PolicyInstance.class;
        }

        @Override
        public Map<String, Object> getJobDataMap() {
            return Map.of();
        }

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public Collection<Trigger> getTriggers() {
            return triggers;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Config config))
                return false;
            return Objects.equals(jobKey, config.jobKey) && Objects.equals(triggers, config.triggers) && Objects.equals(policy, config.policy)
                    && Objects.equals(state, config.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobKey);
        }

        public String getIndex() {
            return jobKey.getName();
        }

        public Policy getPolicy() {
            return policy;
        }

        public synchronized PolicyInstanceState getState() {
            return state;
        }

        public synchronized void setRetryOnNextExecution(boolean retryOnNextExecution, PolicyInstanceService policyInstanceService) {
            state.setRetryOnNextExecution(retryOnNextExecution);
            update(policyInstanceService);
        }

        public synchronized void setDeleted(PolicyInstanceService policyInstanceService) {
            state.setStatus(DELETED);
            LOG.trace("Updating policy instance state for index '{}' to deleted status: \n{}", getIndex(), state.toPrettyJsonString());
            policyInstanceService.updateState(getIndex(), state);
        }

        public synchronized void update(PolicyInstanceService policyInstanceService) {
            if (state.getStatus() == DELETED) {
                LOG.debug("Ignoring policy instance state update for index '{}' because policy instance is deleted", getIndex());
                return;
            }
            LOG.trace("Updating policy instance state for index '{}':\n{}", getIndex(), state.toPrettyJsonString());
            policyInstanceService.updateState(getIndex(), state);
        }

        public static class Factory implements JobConfigFactory<Config> {
            public static Trigger buildDefaultTrigger(JobKey jobKey, AutomatedIndexManagementSettings settings) {
                long executionDelay = settings.getDynamic().getExecutionFixedDelay().getMillis();
                long executionPeriod = settings.getDynamic().getExecutionPeriod().getMillis();
                if (settings.getDynamic().getExecutionRandomDelayEnabled()) {
                    SecureRandom random = new SecureRandom();
                    executionDelay += random.nextLong(executionPeriod);
                }
                return TriggerBuilder.newTrigger().forJob(jobKey)
                        .withIdentity(new TriggerKey(jobKey.getName() + "___" + UUID.randomUUID(), jobKey.getGroup()))
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(executionPeriod).repeatForever()
                                .withMisfireHandlingInstructionFireNow())
                        .startAt(new Date(System.currentTimeMillis() + executionDelay)).build();
            }

            private final AutomatedIndexManagementSettings settings;
            private final PolicyService policyService;
            private final PolicyInstanceService policyInstanceService;

            public Factory(AutomatedIndexManagementSettings settings, PolicyService policyService, PolicyInstanceService policyInstanceService) {
                this.settings = settings;
                this.policyService = policyService;
                this.policyInstanceService = policyInstanceService;
            }

            public JobDetailWithBaseConfig createJobDetailWithBaseConfig(Config config) {
                return new InternalJobDetail(createJobDetail(config), config);
            }

            public Config create(String index, Settings indexSettings) {
                String policyName = indexSettings.get(AutomatedIndexManagementSettings.Static.POLICY_NAME_FIELD.name());
                if (policyName == null || policyName.isEmpty()) {
                    LOG.debug("Policy instance creation failed for index '{}': Invalid index settings: Policy name field missing", index);
                    return null;
                }
                Policy policy = policyService.getPolicyNew(policyName);
                if (policy == null) {
                    LOG.debug("Policy instance creation failed for index '{}': Invalid index settings: Policy '{}' does not exist", index,
                            policyName);
                    return null;
                }
                PolicyInstanceState state = policyInstanceService.getState(index);
                if (state == null || state.getStatus() == PolicyInstanceState.Status.DELETED) {
                    state = new PolicyInstanceState(policyName);
                }
                JobKey jobKey = new JobKey(index, "aim");
                Config config = new Config(jobKey, ImmutableList.of(buildDefaultTrigger(jobKey, settings)), policy, state);
                config.update(policyInstanceService);
                return config;
            }

            @Override
            public JobDetail createJobDetail(Config jobType) {
                JobBuilder jobBuilder = JobBuilder.newJob(jobType.getJobClass());

                jobBuilder.withIdentity(jobType.getJobKey());

                if (jobType.getJobDataMap() != null) {
                    jobBuilder.setJobData(new JobDataMap(jobType.getJobDataMap()));
                }

                jobBuilder.withDescription(jobType.getDescription());
                jobBuilder.storeDurably(jobType.isDurable());
                return jobBuilder.build();
            }
        }
    }
}
