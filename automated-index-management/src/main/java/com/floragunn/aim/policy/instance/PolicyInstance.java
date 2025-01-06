package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.PolicyService;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.aim.policy.schedule.Schedule;
import com.floragunn.aim.scheduler.store.ConfigJobDetail;
import com.floragunn.aim.scheduler.store.JobConfigFactory;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.RestStatus;
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
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static com.floragunn.aim.policy.instance.PolicyInstanceState.Status.*;

@DisallowConcurrentExecution
public class PolicyInstance implements Job {
    public static JobKey jobKeyFromIndexName(String indexName) {
        return new JobKey(indexName, defaultGroupName());
    }

    public static String defaultGroupName() {
        return "_main";
    }

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
            config.getState().setLastResponsibleNode(executionContext.getClusterService().localNode().getName());
            config.getState().setRetryOnNextExecution(false);
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
                    execute0();
                } else {
                    config.getState().setStatus(FINISHED);
                }
            }
        } catch (Exception e) {
            LOG.warn("Unexpected error while retrying policy instance for index '{}'", config.getIndex(), e);
            config.getState().setLastExecutedStepState(new PolicyInstanceState.StepState("unknown", Instant.now(), 0, e));
            config.getState().setStatus(FAILED);
        } finally {
            updateState();
        }
    }

    private void execute() {
        try {
            config.getState().setStatus(RUNNING);
            config.getState().setLastResponsibleNode(executionContext.getClusterService().localNode().getName());
            updateState();
            execute0();
        } catch (Exception e) {
            LOG.warn("Unexpected error while executing policy instance for index '{}'", config.getIndex(), e);
            config.getState().setLastExecutedStepState(new PolicyInstanceState.StepState("unknown", Instant.now(), 0, e));
            config.getState().setStatus(FAILED);
        } finally {
            updateState();
        }
    }

    private void execute0() throws Exception {
        while (config.getState().getStatus() == RUNNING) {
            Policy.Step currentStep = config.getPolicy().getStep(config.getState().getCurrentStepName());
            if (currentStep != null) {
                executeStep(currentStep, 0);
            } else {
                LOG.warn("Could not find step to execute for index '{}'", config.getIndex());
                config.getState().setLastExecutedStepState(
                        new PolicyInstanceState.StepState("unknown", Instant.now(), 0, new IllegalStateException("Could not find step to execute")));
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

        public AutomatedIndexManagementSettings.Index getIndexSettings(String index) {
            return new AutomatedIndexManagementSettings.Index(clusterService.state().metadata().index(index).getSettings());
        }

        public boolean updateIndexSetting(String index, String key, Object value) {
            return updateIndexSettings(index, ImmutableMap.of(key, value));
        }

        public boolean updateIndexSettings(String index, Map<String, Object> settings) {
            AcknowledgedResponse response = client.admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
            if (!response.isAcknowledged()) {
                LOG.warn("Could not update index settings for index '{}'", index);
            }
            return response.isAcknowledged();
        }

        public IndexStats getIndexStats(String index) {
            IndicesStatsResponse response = client.admin().indices().prepareStats(index).clear().setDocs(true).get();
            if (response.getStatus() != RestStatus.OK || response.getIndex(index) == null) {
                LOG.debug("Failed to receive index stats for index '{}'. Response was: {}", index, response);
            }
            return response.getIndex(index);
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
            @SuppressWarnings("unchecked")
            Config config = ((ConfigJobDetail<Config>) bundle.getJobDetail()).getJobConfig();
            LOG.trace("Building new job {} for index {}", config.getJobKey().toString(), config.getJobKey().getName());
            return new PolicyInstance(config, executionContext);
        }
    }

    public static class Config implements JobConfig {
        public static Schedule evaluateSchedule(Policy policy, PolicyInstanceState state, AutomatedIndexManagementSettings settings) {
            String currentStepName = state.getCurrentStepName();
            if ("none".equals(currentStepName) && policy.getFirstStep().getSchedule() != null) {
                return policy.getFirstStep().getSchedule();
            }
            if (!"none".equals(currentStepName) && policy.getStep(currentStepName).getSchedule() != null) {
                return policy.getStep(currentStepName).getSchedule();
            }
            if (policy.getSchedule() != null) {
                return policy.getSchedule();
            }
            return settings.getDynamic().getDefaultSchedule();
        }

        public static Schedule evaluateSchedule(Config config, AutomatedIndexManagementSettings settings) {
            return evaluateSchedule(config.getPolicy(), config.getState(), settings);
        }

        public static boolean isReschedule(Config config, Schedule schedule) {
            TriggerKey currentTriggerKey = config.getCurrentTrigger().getKey();
            TriggerKey newTriggerKey = schedule.getTriggerKey(config.getJobKey());
            if (newTriggerKey.equals(currentTriggerKey)) {
                return false;
            }
            return schedule.getScope() != Schedule.Scope.DEFAULT || !currentTriggerKey.getName().startsWith(Schedule.Scope.DEFAULT.getPrefix());
        }

        private final JobKey jobKey;
        private final Policy policy;
        private final PolicyInstanceState state;

        private Trigger currentTrigger;

        private Config(JobKey jobKey, Policy policy, PolicyInstanceState state, Trigger initialTrigger) {
            this.jobKey = jobKey;
            this.policy = policy;
            this.state = state;

            currentTrigger = initialTrigger;
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
            return ImmutableList.of(currentTrigger);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Config config))
                return false;
            return Objects.equals(jobKey, config.jobKey) && Objects.equals(currentTrigger, config.currentTrigger)
                    && Objects.equals(policy, config.policy) && Objects.equals(state, config.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobKey);
        }

        @Override
        public String toString() {
            return "Config{" + "jobKey=" + jobKey + ", currentTrigger=" + currentTrigger + ", policy=" + policy + ", state=" + state + '}';
        }

        public String getIndex() {
            return jobKey.getName();
        }

        public Trigger getCurrentTrigger() {
            return currentTrigger;
        }

        public Policy getPolicy() {
            return policy;
        }

        public synchronized PolicyInstanceState getState() {
            return state;
        }

        public synchronized void setCurrentTrigger(Trigger trigger) {
            this.currentTrigger = trigger;
        }

        public synchronized void setRetryOnNextExecution(boolean retryOnNextExecution, PolicyInstanceService policyInstanceService) {
            state.setRetryOnNextExecution(retryOnNextExecution);
            update(policyInstanceService);
        }

        public synchronized void setDeleted(PolicyInstanceService policyInstanceService) {
            state.setStatus(DELETED);
            policyInstanceService.updateState(getIndex(), state);
        }

        public synchronized void update(PolicyInstanceService policyInstanceService) {
            if (state.getStatus() == DELETED) {
                LOG.debug("Ignoring policy instance state update for index '{}' because policy instance is deleted", getIndex());
                return;
            }
            policyInstanceService.updateState(getIndex(), state);
        }

        public static class Factory implements JobConfigFactory<Config> {
            private final AutomatedIndexManagementSettings settings;
            private final PolicyService policyService;
            private final PolicyInstanceService policyInstanceService;

            public Factory(AutomatedIndexManagementSettings settings, PolicyService policyService, PolicyInstanceService policyInstanceService) {
                this.settings = settings;
                this.policyService = policyService;
                this.policyInstanceService = policyInstanceService;
            }

            @Override
            public JobDetail createJobDetail(Config config) {
                JobBuilder jobBuilder = JobBuilder.newJob(config.getJobClass()).withIdentity(config.getJobKey()) //
                        .withDescription(config.getDescription()) //
                        .storeDurably(config.isDurable());
                if (config.getJobDataMap() != null) {
                    jobBuilder.setJobData(new JobDataMap(config.getJobDataMap()));
                }
                return jobBuilder.build();
            }

            public Config create(String index, AutomatedIndexManagementSettings.Index indexSettings) {
                String policyName = indexSettings.getPolicyName();
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
                    LOG.trace("Creating new state for index '{}'", index);
                    state = new PolicyInstanceState(policyName);
                    policyInstanceService.updateState(index, state);
                } else {
                    LOG.trace("Found existing state for index '{}': {}", index, state.toPrettyJsonString());
                }
                JobKey jobKey = jobKeyFromIndexName(index);
                Schedule schedule = evaluateSchedule(policy, state, settings);
                return new Config(jobKey, policy, state, schedule.buildTrigger(jobKey));
            }
        }
    }
}
