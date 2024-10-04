package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.policy.PolicyService;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.aim.policy.instance.store.InternalJobDetail;
import com.floragunn.aim.policy.instance.store.InternalOperableTrigger;
import com.floragunn.aim.policy.instance.store.JobStore;
import com.floragunn.aim.policy.instance.store.Store;
import com.floragunn.aim.policy.instance.store.TriggerStore;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.jobs.SchedulerBuilder;
import com.floragunn.searchsupport.jobs.config.JobDetailWithBaseConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class PolicyInstanceManager {
    private static final Logger LOG = LogManager.getLogger(PolicyInstanceManager.class);

    private final Client client;
    private final ClusterService clusterService;
    private final AutomatedIndexManagementSettings settings;
    private final PolicyInstanceService policyInstanceService;
    private final PolicyInstance.Config.Factory jobConfigFactory;
    private final PolicyInstanceStateLogManager policyInstanceStateLogManager;

    private volatile boolean initialized = false;
    private volatile boolean shutdown = false;
    private Scheduler scheduler;
    private ExecutorService maintenanceExecutor;

    public PolicyInstanceManager(AutomatedIndexManagementSettings settings, PolicyService policyService, PolicyInstanceService policyInstanceService,
            Client client, ClusterService clusterService, Condition.Factory conditionFactory, Action.Factory actionFactory) {
        this.client = client;
        this.clusterService = clusterService;
        this.settings = settings;
        this.policyInstanceService = policyInstanceService;

        jobConfigFactory = new PolicyInstance.Config.Factory(settings, policyService, policyInstanceService);
        if (settings.getStatic().stateLog().isEnabled()) {
            policyInstanceStateLogManager = new PolicyInstanceStateLogManager(settings, client, policyService, policyInstanceService,
                    conditionFactory, actionFactory);
        } else {
            policyInstanceStateLogManager = null;
        }
    }

    public synchronized void start() {
        if (initialized) {
            return;
        }
        try {
            PolicyInstance.Factory jobFactory = new PolicyInstance.Factory(clusterService, client, settings, policyInstanceService);
            PolicyInstanceConfigSource jobConfigSource = new PolicyInstanceConfigSource(clusterService, jobConfigFactory);
            TriggerStore<InternalOperableTrigger> triggerStore = new TriggerStore.HeapIndexTriggerStore(
                    AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_TRIGGER_STATES_NAME);
            JobStore<InternalJobDetail> jobStore = new JobStore.HeapJobStore<>(jobConfigFactory, jobConfigSource, Collections.emptyList());
            scheduler = new SchedulerBuilder<PolicyInstance.Config>().name("aim").client(PrivilegedConfigClient.adapt(client))
                    .maxThreads(settings.getStatic().getThreadPoolSize()).threadKeepAlive(Duration.ofMinutes(100))
                    .threadPriority(Thread.NORM_PRIORITY)
                    .jobStore(new Store<>("aim", clusterService.getNodeName(), PrivilegedConfigClient.adapt(client), triggerStore, jobStore))
                    .jobFactory(jobFactory).build();
            maintenanceExecutor = Executors.newSingleThreadExecutor();
            if (policyInstanceStateLogManager != null) {
                policyInstanceStateLogManager.start();
            }
            scheduler.start();
            settings.getDynamic().addChangeListener(this::onSettingsChange);
            clusterService.addListener(this::onClusterChange);
            initialized = true;
        } catch (SchedulerException e) {
            LOG.error("Failed to start policy instance manager", e);
        }
    }

    public synchronized void stop() {
        if (!initialized) {
            return;
        }
        shutdown = true;
        try {
            clusterService.removeListener(this::onClusterChange);
            settings.getDynamic().removeChangeListener(this::onSettingsChange);
            maintenanceExecutor.shutdown();
            try {
                if (maintenanceExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                    maintenanceExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                maintenanceExecutor.shutdownNow();
            }
            scheduler.shutdown(true);
            if (policyInstanceStateLogManager != null) {
                policyInstanceStateLogManager.stop();
            }
            scheduler = null;
            initialized = false;
        } catch (SchedulerException e) {
            LOG.error("Failed to stop policy instance manager", e);
        }
        shutdown = false;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public void handlePoliciesCreate(List<String> policyNames) {
        if (!initialized || shutdown) {
            return;
        }
        LOG.debug("Updating policy instance because of new policies {}", policyNames);
        Map<String, Settings> indices = new HashMap<>();
        for (Map.Entry<String, IndexMetadata> index : clusterService.state().metadata().indices().entrySet()) {
            Settings indexSettings = index.getValue().getSettings();
            String currentPolicyName = settings.getStatic().getPolicyName(indexSettings);
            if (currentPolicyName != null && policyNames.contains(currentPolicyName)) {
                indices.put(index.getKey(), indexSettings);
            }
        }
        if (!indices.isEmpty()) {
            maintenanceExecutor.submit(() -> handleInstanceDeleteCreate(ImmutableList.empty(), indices));
        }
    }

    public void handlePoliciesDelete(List<String> policyNames) {
        if (!initialized || shutdown) {
            return;
        }
        List<String> indices = new ArrayList<>();
        for (Map.Entry<String, IndexMetadata> index : clusterService.state().metadata().indices().entrySet()) {
            String currentPolicyName = settings.getStatic().getPolicyName(index.getValue().getSettings());
            if (currentPolicyName != null && policyNames.contains(currentPolicyName)) {
                indices.add(index.getKey());
            }
        }
        if (!indices.isEmpty()) {
            maintenanceExecutor.submit(() -> handleInstanceDeleteCreate(indices, ImmutableMap.empty()));
        }
    }

    public synchronized boolean executeRetryPolicyInstance(String indexName, boolean execute, boolean retry) {
        if (!initialized || shutdown) {
            return false;
        }
        JobKey key = new JobKey(indexName, "aim");
        try {
            if (!scheduler.checkExists(key) || !policyInstanceService.activeStateExistsForIndex(indexName)) {
                LOG.debug("Policy instance for index '{}' does not exist", indexName);
                return false;
            }
        } catch (SchedulerException e) {
            LOG.warn("Failed to check for policy instance '{}'", indexName, e);
            return false;
        }
        if (retry) {
            try {
                InternalJobDetail jobDetail = (InternalJobDetail) scheduler.getJobDetail(key);
                jobDetail.getBaseConfig(PolicyInstance.Config.class).setRetryOnNextExecution(true, policyInstanceService);
            } catch (Exception e) {
                LOG.warn("Failed to set state reload flag for policy instance '{}'", indexName, e);
            }
        }
        if (execute) {
            try {
                SimpleTrigger trigger = TriggerBuilder.newTrigger()
                        .withIdentity("manual___" + key.getName() + "___" + UUID.randomUUID(), key.getGroup()).forJob(key)
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0).withMisfireHandlingInstructionFireNow())
                        .withPriority(10).startAt(new Date()).build();
                scheduler.scheduleJob(trigger);
                LOG.debug("Policy instance for index '{}' triggered", indexName);
            } catch (SchedulerException e) {
                LOG.warn("Failed to manually trigger policy instance", e);
            }
        }
        return true;
    }

    private synchronized void handleInstanceDeleteCreate(List<String> deleted, Map<String, Settings> created) {
        if (deleted.isEmpty() && created.isEmpty()) {
            return;
        }
        if (!deleted.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Indices to delete policy instances: {}", Arrays.toString(deleted.toArray()));
            }
            for (String index : deleted) {
                try {
                    JobKey jobKey = new JobKey(index, "aim");
                    if (!scheduler.checkExists(jobKey)) {
                        LOG.debug("Could not find job for index '{}' while trying to delete policy instance. Ignoring deletion", index);
                        continue;
                    }
                    InternalJobDetail jobDetail = (InternalJobDetail) scheduler.getJobDetail(jobKey);
                    jobDetail.getBaseConfig(PolicyInstance.Config.class).setDeleted(policyInstanceService);
                    scheduler.deleteJob(jobKey);
                } catch (SchedulerException e) {
                    LOG.warn("Failed to delete policy instance for index {}", index, e);
                }
            }
        }
        if (!created.isEmpty()) {
            for (Map.Entry<String, Settings> entry : created.entrySet()) {
                try {
                    JobKey jobKey = new JobKey(entry.getKey(), "aim");
                    if (scheduler.checkExists(jobKey)) {
                        continue;
                    }
                    PolicyInstance.Config config = jobConfigFactory.create(entry.getKey(), entry.getValue());
                    if (config != null) {
                        LOG.debug("Scheduling policy instance for index '{}'", entry.getKey());
                        JobDetailWithBaseConfig jobDetail = jobConfigFactory.createJobDetailWithBaseConfig(config);
                        scheduler.scheduleJob(jobDetail, new HashSet<>(config.getTriggers()), false);
                    }
                } catch (SchedulerException e) {
                    LOG.error("Could not add policy instance '{}' to scheduler", entry.getKey(), e);
                }
            }
        }
    }

    private void onSettingsChange(List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> changed) {
        if (policyInstanceStateLogManager != null && changed.contains(AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE)) {
            if (settings.getDynamic().getStateLogActive()) {
                policyInstanceStateLogManager.start();
            } else {
                policyInstanceStateLogManager.stop();
            }
        }
        if (changed.contains(AutomatedIndexManagementSettings.Dynamic.EXECUTION_PERIOD)) {
            synchronized (this) {
                try {
                    Set<TriggerKey> keys = scheduler.getTriggerKeys(GroupMatcher.groupEquals("aim"));
                    for (TriggerKey triggerKey : keys) {
                        JobKey jobKey = scheduler.getTrigger(triggerKey).getJobKey();
                        scheduler.rescheduleJob(triggerKey, PolicyInstance.Config.Factory.buildDefaultTrigger(jobKey, settings));
                    }
                } catch (SchedulerException e) {
                    LOG.error("Failed to update policy instance triggers", e);
                }
            }
        }
    }

    private void onClusterChange(ClusterChangedEvent event) {
        if (!event.metadataChanged()) {
            return;
        }
        List<String> deletedIndices = new ArrayList<>();
        Map<String, Settings> createdIndices = new HashMap<>();
        for (Index index : event.indicesDeleted()) {
            IndexMetadata indexMetadata = event.previousState().metadata().index(index);
            String policyName = settings.getStatic().getPolicyName(indexMetadata.getSettings());
            if (!Strings.isNullOrEmpty(policyName)) {
                deletedIndices.add(index.getName());
            }
        }
        for (Map.Entry<String, IndexMetadata> index : event.state().metadata().indices().entrySet()) {
            Settings indexSettings = index.getValue().getSettings();
            String policyName = settings.getStatic().getPolicyName(indexSettings);
            if (!Strings.isNullOrEmpty(policyName)) {
                //LOG.trace("New index '{}' with settings:\n{}", index.getKey(), Strings.toString(index.getValue().getSettings(), true, true));
                createdIndices.put(index.getKey(), indexSettings);
            }
        }
        maintenanceExecutor.submit(() -> handleInstanceDeleteCreate(deletedIndices, createdIndices));
    }
}
