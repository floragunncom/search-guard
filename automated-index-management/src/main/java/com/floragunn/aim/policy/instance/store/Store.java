package com.floragunn.aim.policy.instance.store;

import com.floragunn.searchsupport.jobs.cluster.DistributedJobStore;
import com.floragunn.searchsupport.util.SingleElementBlockingQueue;
import com.google.common.collect.MapMaker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.core.TimeValue;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.quartz.utils.Key;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Store<TriggerType extends InternalOperableTrigger, JobType extends InternalJobDetail> implements DistributedJobStore {
    public static boolean INCLUDE_NODE_ID_IN_SCHEDULER_STORE = false;

    public static Store<?, ?> getInstance(String node, String name) {
        return SCHEDULER_STORE.get(getInstanceName(node, name));
    }

    private static final Logger LOG = LogManager.getLogger(Store.class);
    private static final Map<String, Store<?, ?>> SCHEDULER_STORE = new MapMaker().concurrencyLevel(4).weakKeys().makeMap();

    private static String getInstanceName(String node, String name) {
        return INCLUDE_NODE_ID_IN_SCHEDULER_STORE ? node + "::" + name : name;
    }

    private final String schedulerName;
    private final String node;
    private final Client client;
    private final TriggerStore<TriggerType> triggerStore;
    private final JobStore<JobType> jobStore;

    private final ExecutorService configUpdateExecutor;
    private final ScheduledThreadPoolExecutor maintenanceExecutor;

    private volatile boolean shutdown;
    private volatile boolean initialized;

    private SchedulerSignaler signaler;

    public Store(String schedulerName, String node, Client client, TriggerStore<TriggerType> triggerStore, JobStore<JobType> jobStore) {
        this.schedulerName = schedulerName;
        this.node = node;
        this.client = client;
        this.triggerStore = triggerStore;
        this.jobStore = jobStore;

        configUpdateExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new SingleElementBlockingQueue<>());
        maintenanceExecutor = new ScheduledThreadPoolExecutor(1);

        shutdown = false;
        initialized = false;
    }

    @Override
    public void clusterConfigChanged(ClusterChangedEvent event) {
        LOG.debug("Cluster config changed; shutdown: {}", shutdown);
        if (shutdown) {
            return;
        }
        try {
            clearAllSchedulingData();
        } catch (JobPersistenceException e) {
            LOG.error("Could not clear scheduling data on cluster config change", e);
        }
        configUpdateExecutor.submit(() -> {
            try {
                LOG.info("Reinitializing jobs for {}", schedulerName);
                maintenanceExecutor.getQueue().clear();
                load();
                signaler.signalSchedulingChange(0);
                LOG.debug("Finished initializing jobs for {}", schedulerName);
            } catch (Exception e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    LOG.debug(e1);
                }
                if (!shutdown) {
                    LOG.error("Error initializing jobs for {}", schedulerName, e);
                }
            }
        });
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        if (initialized) {
            return;
        }
        LOG.info("Initializing store for '{}'", schedulerName);
        this.signaler = signaler;
        SCHEDULER_STORE.put(getInstanceName(node, schedulerName), this);
        try {
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForYellowStatus()
                    .setWaitForNoInitializingShards(true).get(TimeValue.timeValueSeconds(1));
            LOG.debug("Cluster health before loading store: {}", clusterHealthResponse);
            if (clusterHealthResponse.isTimedOut()) {
                LOG.warn("Timeout while waiting for initialized cluster status. Will continue anyway: {}", clusterHealthResponse);
            }
        } catch (Exception e) {
            LOG.error("Error while checking cluster health", e);
            throw new SchedulerConfigException("Error while checking cluster health", e);
        }
        jobStore.initialize(signaler, client, node, schedulerName, maintenanceExecutor);
        triggerStore.initialize(signaler, client, node, schedulerName, maintenanceExecutor);
        load();
        initialized = true;
        LOG.debug("Store initialization for '{}' finished successfully", schedulerName);
    }

    @Override
    public void schedulerStarted() throws SchedulerException {

    }

    @Override
    public void schedulerPaused() {

    }

    @Override
    public void schedulerResumed() {

    }

    @Override
    public void shutdown() {
        if (!shutdown) {
            LOG.info("Shutting down {}", this);
            shutdown = true;
            jobStore.shutdown();
            triggerStore.shutdown();
            configUpdateExecutor.shutdown();
            try {
                if (configUpdateExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                    configUpdateExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                configUpdateExecutor.shutdownNow();
            }
            maintenanceExecutor.shutdown();
            try {
                if (maintenanceExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                    maintenanceExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                configUpdateExecutor.shutdownNow();
            }
        }
    }

    @Override
    public boolean supportsPersistence() {
        return false;
    }

    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        return 10;
    }

    @Override
    public boolean isClustered() {
        return true;
    }

    @Override
    public synchronized void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
            throws ObjectAlreadyExistsException, JobPersistenceException {
        storeJob(newJob, false);
        storeTrigger(newTrigger, false);
    }

    @Override
    public synchronized void storeJob(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        jobStore.add(newJob, replaceExisting);
    }

    @Override
    public synchronized void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws ObjectAlreadyExistsException, JobPersistenceException {
        LOG.debug("Storing new jobs {}", triggersAndJobs.keySet().stream().map(JobDetail::getKey).collect(Collectors.toSet()));
        for (Map.Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs.entrySet()) {
            storeJob(entry.getKey(), replace);
            for (Trigger trigger : entry.getValue()) {
                storeTrigger((OperableTrigger) trigger, replace);
            }
        }
    }

    @Override
    public synchronized boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        boolean result = triggerStore.removeAll(jobKey);
        return result && jobStore.remove(jobKey);
    }

    @Override
    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        boolean result = true;
        for (JobKey jobKey : jobKeys) {
            result &= removeJob(jobKey);
        }
        return result;
    }

    @Override
    public synchronized JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return jobStore.get(jobKey);
    }

    @Override
    public synchronized void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
            throws ObjectAlreadyExistsException, JobPersistenceException {
        LOG.debug("Storing new trigger {}", newTrigger);
        if (!jobStore.contains(newTrigger.getJobKey())) {
            throw new JobPersistenceException("Trigger " + newTrigger.getKey() + " references non-existing job " + newTrigger.getJobKey());
        }
        triggerStore.add(newTrigger, replaceExisting);
    }

    @Override
    public synchronized boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        InternalOperableTrigger trigger = triggerStore.remove(triggerKey);
        if (trigger == null) {
            return false;
        }
        if (triggerStore.getAll(trigger.getJobKey()).isEmpty()) {
            jobStore.remove(trigger.getJobKey());
        }
        return true;
    }

    @Override
    public synchronized boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        boolean result = true;
        for (TriggerKey triggerKey : triggerKeys) {
            result &= removeTrigger(triggerKey);
        }
        return result;
    }

    @Override
    public synchronized boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        return triggerStore.replace(triggerKey, newTrigger);
    }

    @Override
    public synchronized OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return triggerStore.get(triggerKey);
    }

    @Override
    public synchronized boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return jobStore.contains(jobKey);
    }

    @Override
    public synchronized boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        return triggerStore.contains(triggerKey);
    }

    @Override
    public synchronized void clearAllSchedulingData() throws JobPersistenceException {
        triggerStore.clear();
        jobStore.clear();
    }

    @Override
    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
            throws ObjectAlreadyExistsException, JobPersistenceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeCalendar(String calName) throws JobPersistenceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int getNumberOfJobs() throws JobPersistenceException {
        return jobStore.size();
    }

    @Override
    public synchronized int getNumberOfTriggers() throws JobPersistenceException {
        return triggerStore.size();
    }

    @Override
    public int getNumberOfCalendars() throws JobPersistenceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        return jobStore.keySet().stream().filter(matcher::isMatch).collect(Collectors.toSet());
    }

    @Override
    public synchronized Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return triggerStore.keySet().stream().filter(matcher::isMatch).collect(Collectors.toSet());
    }

    @Override
    public synchronized List<String> getJobGroupNames() throws JobPersistenceException {
        return jobStore.keySet().stream().map(JobKey::getGroup).distinct().toList();
    }

    @Override
    public synchronized List<String> getTriggerGroupNames() throws JobPersistenceException {
        return triggerStore.keySet().stream().map(TriggerKey::getGroup).distinct().toList();
    }

    @Override
    public List<String> getCalendarNames() throws JobPersistenceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        if (!jobStore.contains(jobKey)) {
            return null;
        }
        return triggerStore.getAll(jobKey).stream().map(triggerType -> (OperableTrigger) triggerType).toList();
    }

    @Override
    public synchronized Trigger.TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        if (triggerStore.contains(triggerKey)) {
            return triggerStore.get(triggerKey).getState().getTriggerState();
        }
        return Trigger.TriggerState.NONE;
    }

    @Override
    public synchronized void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
        triggerStore.resetFromErrorState(triggerKey);
    }

    @Override
    public synchronized void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        triggerStore.pause(triggerKey);
    }

    @Override
    public synchronized Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Set<TriggerKey> triggerKeys = triggerStore.keySet().stream().filter(matcher::isMatch).collect(Collectors.toSet());
        triggerStore.pauseAll(triggerKeys);
        return triggerKeys.stream().map(Key::getGroup).collect(Collectors.toSet());
    }

    @Override
    public synchronized void pauseJob(JobKey jobKey) throws JobPersistenceException {
        triggerStore.pauseAll(jobKey);
    }

    @Override
    public synchronized Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        Collection<JobKey> matchedKeys = jobStore.keySet().stream().filter(groupMatcher::isMatch).collect(Collectors.toSet());
        for (JobKey jobKey : matchedKeys) {
            pauseJob(jobKey);
        }
        return matchedKeys.stream().map(Key::getGroup).collect(Collectors.toSet());
    }

    @Override
    public synchronized void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        triggerStore.resume(triggerKey);
    }

    @Override
    public synchronized Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Set<TriggerKey> triggerKeys = triggerStore.keySet().stream().filter(matcher::isMatch).collect(Collectors.toSet());
        triggerStore.resumeAll(triggerKeys);
        return triggerKeys.stream().map(Key::getGroup).collect(Collectors.toSet());
    }

    @Override
    public synchronized Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        return triggerStore.getPausedGroups();
    }

    @Override
    public synchronized void resumeJob(JobKey jobKey) throws JobPersistenceException {
        triggerStore.resumeAll(jobKey);
    }

    @Override
    public synchronized Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        Set<JobKey> jobKeys = jobStore.keySet().stream().filter(matcher::isMatch).collect(Collectors.toSet());
        for (JobKey jobKey : jobKeys) {
            triggerStore.resumeAll(jobKey);
        }
        return jobKeys.stream().map(Key::getGroup).collect(Collectors.toSet());
    }

    @Override
    public synchronized void pauseAll() throws JobPersistenceException {
        triggerStore.pauseAll(triggerStore.keySet());
    }

    @Override
    public synchronized void resumeAll() throws JobPersistenceException {
        triggerStore.resumeAll(triggerStore.keySet());
    }

    @Override
    public synchronized List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException {
        return triggerStore.acquireNextTriggers(noLaterThan, maxCount, timeWindow);
    }

    @Override
    public synchronized void releaseAcquiredTrigger(OperableTrigger trigger) {
        triggerStore.releaseAcquiredTrigger(trigger.getKey());
    }

    @Override
    public synchronized List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        if (triggers.isEmpty()) {
            return List.of();
        }
        Map<JobKey, JobDetail> jobDetails = jobStore.getAllAsMap(triggers.stream().map(Trigger::getJobKey).collect(Collectors.toSet()));
        return triggerStore.triggersFired(triggers, jobDetails);
    }

    @Override
    public synchronized void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
            Trigger.CompletedExecutionInstruction triggerInstCode) {
        jobStore.jobComplete(jobDetail);
        triggerStore.triggeredJobComplete(trigger, jobDetail, triggerInstCode);
    }

    @Override
    public void setInstanceId(String schedInstId) {

    }

    @Override
    public void setInstanceName(String schedName) {

    }

    @Override
    public void setThreadPoolSize(int poolSize) {

    }

    @Override
    public long getAcquireRetryDelay(int failureCount) {
        return 20;
    }

    @Override
    public String toString() {
        return "Store{" + "schedulerName='" + schedulerName + '\'' + ", node='" + node + '\'' + ", triggerStore=" + triggerStore + ", jobStore="
                + jobStore + '}';
    }

    private synchronized void load() {
        Map<JobKey, InternalJobDetail> jobs = jobStore.load();
        triggerStore.load(jobs);
    }
}
