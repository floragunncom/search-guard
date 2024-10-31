package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.policy.PolicyService;
import com.floragunn.aim.policy.schedule.Schedule;
import com.floragunn.aim.scheduler.DynamicJobDistributor;
import com.floragunn.aim.scheduler.SchedulerBuilder;
import com.floragunn.aim.scheduler.store.InternalJobDetail;
import com.floragunn.aim.scheduler.store.JobStore;
import com.floragunn.aim.scheduler.store.Store;
import com.floragunn.aim.scheduler.store.TriggerStore;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.JobPersistenceException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class PolicyInstanceManager {
    private static final Logger LOG = LogManager.getLogger(PolicyInstanceManager.class);

    private final Client client;
    private final ClusterService clusterService;
    private final AutomatedIndexManagementSettings settings;
    private final PolicyService policyService;
    private final PolicyInstanceService policyInstanceService;
    private final PolicyInstance.Config.Factory jobConfigFactory;
    private final DynamicJobDistributor distributor;

    private volatile boolean initialized = false;
    private volatile boolean shutdown = false;
    private Scheduler scheduler;
    private Store<PolicyInstance.Config> store;
    private ExecutorService configUpdateExecutor;

    public PolicyInstanceManager(AutomatedIndexManagementSettings settings, PolicyService policyService, PolicyInstanceService policyInstanceService,
            Client client, ClusterService clusterService, DynamicJobDistributor distributor) {
        this.client = client;
        this.clusterService = clusterService;
        this.settings = settings;
        this.policyService = policyService;
        this.policyInstanceService = policyInstanceService;

        jobConfigFactory = new PolicyInstance.Config.Factory(settings, policyService, policyInstanceService);
        this.distributor = distributor;
    }

    public synchronized void start() throws SchedulerException {
        if (initialized) {
            return;
        }
        PolicyInstance.Config.Factory jobConfigFactory = new PolicyInstance.Config.Factory(settings, policyService, policyInstanceService);
        PolicyInstance.Factory jobFactory = new PolicyInstance.Factory(clusterService, client, settings, policyInstanceService);
        TriggerStore<PolicyInstance.Config> triggerStore = new TriggerStore.HeapIndexTriggerStore<>(
                AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_TRIGGER_STATES_NAME);
        JobStore<PolicyInstance.Config> jobStore = new JobStore.HeapJobStore<>(jobConfigFactory, Collections.emptyList());
        store = new Store<>("aim_main", clusterService.getNodeName(), PrivilegedConfigClient.adapt(client), triggerStore, jobStore, this::getConfigs);
        scheduler = new SchedulerBuilder("aim_main", store, jobFactory, clusterService.getNodeName())
                .maxThreads(settings.getStatic().getThreadPoolSize()) //
                .threadKeepAlive(Duration.ofMinutes(100)) //
                .threadPriority(Thread.NORM_PRIORITY) //
                .build();
        scheduler.getListenerManager().addJobListener(new JobListener() {
            @Override
            public String getName() {
                return "schedule_change_listener";
            }

            @Override
            public void jobToBeExecuted(JobExecutionContext context) {
            }

            @Override
            public void jobExecutionVetoed(JobExecutionContext context) {
            }

            @Override
            public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
                @SuppressWarnings("unchecked")
                InternalJobDetail<PolicyInstance.Config> jobDetail = (InternalJobDetail<PolicyInstance.Config>) context.getJobDetail();
                PolicyInstance.Config config = jobDetail.getJobConfig();
                Schedule evaluatedSchedule = PolicyInstance.Config.evaluateSchedule(config, settings);
                if (PolicyInstance.Config.isReschedule(config, evaluatedSchedule)) {
                    LOG.trace("Rescheduling job '{}' because triggers changed: {}", jobDetail.getKey(), evaluatedSchedule);
                    try {
                        Trigger oldTrigger = config.getCurrentTrigger();
                        Trigger newTrigger = evaluatedSchedule.buildTrigger(config.getJobKey());
                        LOG.trace("Replacing old trigger: {} with new trigger: {}", oldTrigger, newTrigger);
                        Date nextFireTime = scheduler.rescheduleJob(oldTrigger.getKey(), newTrigger);
                        LOG.trace("Next fire time for replaced trigger is: {}", nextFireTime);
                        config.setCurrentTrigger(newTrigger);
                    } catch (SchedulerException e) {
                        LOG.warn("Failed to reschedule job '{}'", jobDetail.getKey(), e);
                    }
                }
            }
        });
        configUpdateExecutor = Executors.newSingleThreadExecutor();
        scheduler.start();
        settings.getDynamic().addChangeListener(this::onSettingsChange);
        clusterService.addListener(this::onClusterStateChange);
        initialized = true;
    }

    public synchronized void stop() {
        if (!initialized) {
            return;
        }
        shutdown = true;
        clusterService.removeListener(this::onClusterStateChange);
        settings.getDynamic().removeChangeListener(this::onSettingsChange);
        configUpdateExecutor.shutdown();
        try {
            if (!configUpdateExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                configUpdateExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            configUpdateExecutor.shutdownNow();
        }
        try {
            scheduler.shutdown(true);
            scheduler = null;
        } catch (SchedulerException e) {
            LOG.error("Failed to stop policy instance manager", e);
        }
        initialized = false;
        shutdown = false;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public void handlePolicyUpdates(List<String> deletedPolicyNames, List<String> createdPolicyNames, List<String> updatedPolicyNames) {
        if (!initialized || shutdown) {
            return;
        }
        List<JobKey> deletedJobs = new ArrayList<>();
        Map<String, AutomatedIndexManagementSettings.Index> newIndices = new HashMap<>();
        for (Map.Entry<String, IndexMetadata> entry : clusterService.state().metadata().indices().entrySet()) {
            if (!AutomatedIndexManagementSettings.Index.isManagedIndex(entry.getValue().getSettings())) {
                continue;
            }
            AutomatedIndexManagementSettings.Index indexSettings = new AutomatedIndexManagementSettings.Index(entry.getValue().getSettings());
            String indexName = entry.getKey();
            String policyName = indexSettings.getPolicyName();
            if (deletedPolicyNames.contains(policyName) || createdPolicyNames.contains(policyName) || updatedPolicyNames.contains(policyName)) {
                JobKey jobKey = PolicyInstance.jobKeyFromIndexName(indexName);
                if (distributor.isJobSelected(jobKey)) {
                    if (deletedPolicyNames.contains(policyName)) {
                        deletedJobs.add(jobKey);
                    } else if (createdPolicyNames.contains(policyName)) {
                        newIndices.put(indexName, indexSettings);
                    } else if (updatedPolicyNames.contains(policyName)) {
                        deletedJobs.add(jobKey);
                        newIndices.put(indexName, indexSettings);
                    }
                }
            }
            LOG.trace("Found indices to drop management after policy updates: {}", deletedJobs);
            LOG.trace("Found indices to manage after policy updates: {}", newIndices.keySet());
            if (!deletedJobs.isEmpty() || !newIndices.isEmpty()) {
                configUpdateExecutor.submit(() -> {
                    store.handleConfigUpdate(deletedJobs, newIndices.isEmpty() ? ImmutableList.empty() : () -> new ConfigIterator(newIndices));
                });
            }
        }
    }

    public void handleReschedule() {
        if (!initialized || shutdown) {
            return;
        }
        configUpdateExecutor.submit(() -> store.reschedule());
    }

    public synchronized boolean executeRetryPolicyInstance(String indexName, boolean execute, boolean retry) {
        JobKey key = PolicyInstance.jobKeyFromIndexName(indexName);
        if (!initialized || shutdown || !distributor.isJobSelected(key)) {
            return false;
        }
        try {
            if (!scheduler.checkExists(key)) {
                LOG.debug("Policy instance for index '{}' does not exist on this node", indexName);
                return false;
            }
        } catch (SchedulerException e) {
            LOG.warn("Failed to check for policy instance '{}'", indexName, e);
            return false;
        }
        if (retry) {
            try {
                @SuppressWarnings("unchecked")
                InternalJobDetail<PolicyInstance.Config> jobDetail = (InternalJobDetail<PolicyInstance.Config>) scheduler.getJobDetail(key);
                jobDetail.getJobConfig().setRetryOnNextExecution(true, policyInstanceService);
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

    private Iterable<PolicyInstance.Config> getConfigs() {
        Map<String, AutomatedIndexManagementSettings.Index> indices = getManagedIndices(clusterService.state());
        return indices.isEmpty() ? ImmutableList.empty() : () -> new ConfigIterator(indices);
    }

    private Map<String, AutomatedIndexManagementSettings.Index> getManagedIndices(ClusterState state) {
        Map<String, AutomatedIndexManagementSettings.Index> createdIndices = new HashMap<>();
        for (Map.Entry<String, IndexMetadata> index : state.metadata().indices().entrySet()) {
            if (AutomatedIndexManagementSettings.Index.isManagedIndex(index.getValue().getSettings())) {
                JobKey jobKey = PolicyInstance.jobKeyFromIndexName(index.getKey());
                if (distributor.isJobSelected(jobKey)) {
                    createdIndices.put(index.getKey(), new AutomatedIndexManagementSettings.Index(index.getValue().getSettings()));
                }
            }
        }
        LOG.debug("Found managed indices: {}", createdIndices);
        return createdIndices;
    }

    private void onSettingsChange(List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> changed) {
        if (changed.contains(AutomatedIndexManagementSettings.Dynamic.DEFAULT_SCHEDULE)) {
            synchronized (this) {
                try {
                    Set<TriggerKey> keys = scheduler.getTriggerKeys(GroupMatcher.groupEquals(PolicyInstance.defaultGroupName())).stream()
                            .filter(triggerKey -> triggerKey.getName().startsWith(Schedule.Scope.DEFAULT.getPrefix())).collect(Collectors.toSet());
                    for (TriggerKey triggerKey : keys) {
                        JobKey jobKey = scheduler.getTrigger(triggerKey).getJobKey();
                        scheduler.rescheduleJob(triggerKey, settings.getDynamic().getDefaultSchedule().buildTrigger(jobKey));
                    }
                } catch (SchedulerException e) {
                    LOG.error("Failed to update policy instance triggers", e);
                }
            }
        }
    }

    private void onClusterStateChange(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlockWithLevel(ClusterBlockLevel.READ)) {
            return;
        }
        if (distributor.isReschedule(event.state())) {
            configUpdateExecutor.submit(() -> store.reschedule());
        } else if (event.metadataChanged()) {
            List<JobKey> deleted = new ArrayList<>();
            for (Index index : event.indicesDeleted()) {
                if (AutomatedIndexManagementSettings.Index.isManagedIndex(event.previousState().metadata().index(index).getSettings())) {
                    deleted.add(PolicyInstance.jobKeyFromIndexName(index.getName()));
                }
            }
            Map<String, AutomatedIndexManagementSettings.Index> createdIndices = new HashMap<>();
            for (Map.Entry<String, IndexMetadata> index : event.state().metadata().indices().entrySet()) {
                if (event.previousState().metadata().hasIndex(index.getKey())) {
                    IndexMetadata previousIndexMetadata = event.previousState().metadata().index(index.getKey());
                    if (index.getValue().getIndexUUID().equals(previousIndexMetadata.getIndexUUID())) {
                        continue;
                    }
                }
                if (AutomatedIndexManagementSettings.Index.isManagedIndex(index.getValue().getSettings())) {
                    JobKey jobKey = PolicyInstance.jobKeyFromIndexName(index.getKey());
                    if (distributor.isJobSelected(jobKey)) {
                        createdIndices.put(index.getKey(), new AutomatedIndexManagementSettings.Index(index.getValue().getSettings()));
                    }
                }
            }
            LOG.trace("Found deleted managed indices: {}", deleted);
            LOG.trace("Found created managed indices: {}", createdIndices.keySet());
            if (!deleted.isEmpty() || !createdIndices.isEmpty()) {
                Iterable<PolicyInstance.Config> created = createdIndices.isEmpty() ? ImmutableList.empty() : () -> new ConfigIterator(createdIndices);
                configUpdateExecutor.submit(() -> {
                    List<InternalJobDetail<PolicyInstance.Config>> deletedConfigs = store.handleConfigUpdate(deleted, created);
                    for (InternalJobDetail<PolicyInstance.Config> deletedConfig : deletedConfigs) {
                        deletedConfig.getJobConfig().setDeleted(policyInstanceService);
                    }
                });
            }
        }
    }

    private class ConfigIterator implements Iterator<PolicyInstance.Config> {
        private final Iterator<Map.Entry<String, AutomatedIndexManagementSettings.Index>> delegate;
        private PolicyInstance.Config peekedValue = null;

        public ConfigIterator(Map<String, AutomatedIndexManagementSettings.Index> created) {
            delegate = created.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            if (peekedValue != null) {
                return true;
            }
            return findNext();
        }

        @Override
        public PolicyInstance.Config next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            PolicyInstance.Config config = peekedValue;
            peekedValue = null;
            return config;
        }

        private boolean findNext() {
            while (delegate.hasNext()) {
                Map.Entry<String, AutomatedIndexManagementSettings.Index> next = delegate.next();
                try {
                    if (store.checkExists(PolicyInstance.jobKeyFromIndexName(next.getKey()))) {
                        continue;
                    }
                } catch (JobPersistenceException e) {
                    LOG.warn("Failed to check for policy instance job existence for index '{}'", next.getKey(), e);
                }
                LOG.trace("Found new managed index '{}'", next.getKey());
                peekedValue = jobConfigFactory.create(next.getKey(), next.getValue());
                return true;
            }
            return false;
        }
    }
}
