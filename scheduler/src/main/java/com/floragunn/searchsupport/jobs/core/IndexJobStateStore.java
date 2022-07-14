package com.floragunn.searchsupport.jobs.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.quartz.Calendar;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.ScheduleBuilder;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.impl.triggers.DailyTimeIntervalTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;

import com.floragunn.searchsupport.client.Actions;
import com.floragunn.searchsupport.jobs.JobConfigListener;
import com.floragunn.searchsupport.jobs.actions.CheckForExecutingTriggerAction;
import com.floragunn.searchsupport.jobs.actions.CheckForExecutingTriggerRequest;
import com.floragunn.searchsupport.jobs.actions.CheckForExecutingTriggerResponse;
import com.floragunn.searchsupport.jobs.cluster.DistributedJobStore;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobConfigFactory;
import com.floragunn.searchsupport.jobs.config.JobDetailWithBaseConfig;
import com.floragunn.searchsupport.util.SingleElementBlockingQueue;
import com.google.common.base.Objects;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

public class IndexJobStateStore<JobType extends com.floragunn.searchsupport.jobs.config.JobConfig> implements DistributedJobStore {

    /**
     * For unit testing in environments where several nodes run in one JVM.
     */
    public static boolean includeNodeIdInSchedulerToJobStoreMapKeys = false;

    // TODO maybe separate loggers for each scheduler instance?
    private static final Logger log = LogManager.getLogger(IndexJobStateStore.class);

    private final static Map<String, IndexJobStateStore<?>> schedulerToJobStoreMap = new MapMaker().concurrencyLevel(4).weakValues().makeMap();

    public static IndexJobStateStore<?> getInstanceBySchedulerName(String nodeId, String schedulerName) {
        if (includeNodeIdInSchedulerToJobStoreMapKeys) {
            return schedulerToJobStoreMap.get(nodeId + "::" + schedulerName);
        } else {
            return schedulerToJobStoreMap.get(schedulerName);
        }
    }

    private final String schedulerName;
    private final String statusIndexName;
    private final String statusIndexIdPrefix;
    private final String nodeId;
    private final Client client;
    private SchedulerSignaler signaler;
    private final Map<JobKey, InternalJobDetail> keyToJobMap = new HashMap<>();
    private final Map<TriggerKey, InternalOperableTrigger> keyToTriggerMap = new HashMap<>();
    private final Table<String, JobKey, InternalJobDetail> groupAndKeyToJobMap = HashBasedTable.create();
    private final Table<String, TriggerKey, InternalOperableTrigger> groupAndKeyToTriggerMap = HashBasedTable.create();
    private final ActiveTriggerQueue activeTriggers = new ActiveTriggerQueue();
    private final Set<String> pausedTriggerGroups = new HashSet<String>();
    private final Set<String> pausedJobGroups = new HashSet<String>();
    private final Set<JobKey> blockedJobs = new HashSet<JobKey>();
    private final Set<InternalOperableTrigger> triggersStillExecutingOnOtherNodes = new HashSet<>();
    private final Iterable<JobType> jobConfigSource;
    private final JobConfigFactory<JobType> jobFactory;
    private volatile boolean shutdown = false;
    private volatile boolean initialized;
    private long misfireThreshold = 5000l;
    private ThreadLocal<Set<InternalOperableTrigger>> dirtyTriggers = ThreadLocal.withInitial(() -> new HashSet<>());
    private final ExecutorService configChangeExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new SingleElementBlockingQueue<Runnable>());
    private final ScheduledThreadPoolExecutor periodicMaintenanceExecutor = new ScheduledThreadPoolExecutor(1);
    private final ClusterService clusterService;
    private final Collection<JobConfigListener<JobType>> jobConfigListeners;

    public IndexJobStateStore(String schedulerName, String statusIndexName, String statusIndexIdPrefix, String nodeId, Client client,
            Iterable<JobType> jobConfigSource, JobConfigFactory<JobType> jobFactory, ClusterService clusterService,
            Collection<JobConfigListener<JobType>> jobConfigListeners) {
        this.schedulerName = schedulerName;
        this.statusIndexName = statusIndexName;
        this.statusIndexIdPrefix = statusIndexIdPrefix;
        this.nodeId = nodeId;
        this.client = client;
        this.jobConfigSource = jobConfigSource;
        this.jobFactory = jobFactory;
        this.clusterService = clusterService;
        this.jobConfigListeners = new ArrayList<>(jobConfigListeners);
    }

    @Override
    public void clusterConfigChanged(ClusterChangedEvent event) {
        log.debug("Cluster config changed; shutdown: " + shutdown);
        
        if (shutdown) {
            return;
        }

        resetJobs();

        configChangeExecutor.submit(() -> updateAfterClusterConfigChange());
    }

    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        this.signaler = signaler;

        if (includeNodeIdInSchedulerToJobStoreMapKeys) {
            schedulerToJobStoreMap.put(nodeId + "::" + schedulerName, this);
        } else {
            schedulerToJobStoreMap.put(schedulerName, this);
        }

        try {
            this.initJobs();
        } catch (Exception e) {
            if (clusterService != null) {
                log.info("Error while initializing " + this + "\nWill try again during the next cluster change", e);
            } else {
                throw new SchedulerConfigException("Error while initializing " + this, e);
            }
        }
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
            log.info("Shutdown of " + this);
            shutdown = true;
            configChangeExecutor.shutdownNow();
        }
    }

    @Override
    public boolean supportsPersistence() {
        return true;
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
    public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws ObjectAlreadyExistsException, JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            storeJob(newJob, false);
            internalOperableTrigger = storeTriggerInHeap(newTrigger, false);
        }

        setTriggerStatusInIndex(internalOperableTrigger);
    }

    @Override
    public synchronized void storeJob(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        JobConfig baseConfig = null;

        if (newJob instanceof JobDetailWithBaseConfig) {
            baseConfig = ((JobDetailWithBaseConfig) newJob).getBaseConfig();
        }

        InternalJobDetail newJobLocal = new InternalJobDetail(newJob, baseConfig, this);

        addToCollections(newJobLocal);
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws ObjectAlreadyExistsException, JobPersistenceException {

        ArrayList<InternalOperableTrigger> internalOperableTriggers = new ArrayList<>();

        synchronized (this) {
            for (Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs.entrySet()) {
                storeJob(entry.getKey(), true);

                for (Trigger trigger : entry.getValue()) {
                    internalOperableTriggers.add(storeTriggerInHeap((OperableTrigger) trigger, true));
                }
            }
        }

        for (InternalOperableTrigger internalOperableTrigger : internalOperableTriggers) {
            setTriggerStatusInIndex(internalOperableTrigger);
        }
    }

    @Override
    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger = storeTriggerInHeap(newTrigger, replaceExisting);

        setTriggerStatusInIndex(internalOperableTrigger);
    }

    @Override
    public synchronized boolean removeJob(JobKey jobKey) {
        boolean result = false;

        List<OperableTrigger> triggers = getTriggersForJob(jobKey);

        for (OperableTrigger trigger : triggers) {
            this.removeTrigger(trigger.getKey());
            result = true;
        }

        if (keyToJobMap.remove(jobKey) != null) {
            result = true;
        }

        groupAndKeyToJobMap.remove(jobKey.getGroup(), jobKey);

        return result;
    }

    @Override
    public synchronized boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        boolean result = true;

        for (JobKey jobKey : jobKeys) {
            if (!removeJob(jobKey)) {
                result = false;
            }
        }
        return result;
    }

    @Override
    public synchronized boolean removeTrigger(TriggerKey triggerKey) {
        InternalOperableTrigger internalOperableTrigger = this.keyToTriggerMap.remove(triggerKey);

        if (internalOperableTrigger == null) {
            return false;
        }

        this.groupAndKeyToTriggerMap.remove(triggerKey.getGroup(), triggerKey);
        this.activeTriggers.remove(internalOperableTrigger);

        InternalJobDetail internalJobDetail = this.keyToJobMap.get(internalOperableTrigger.getJobKey());

        if (internalJobDetail != null) {
            internalJobDetail.triggers.remove(internalOperableTrigger);

            if (internalJobDetail.triggers.isEmpty()) {
                removeJob(internalJobDetail.getKey());
            }

        }

        return true;
    }

    @Override
    public synchronized boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        boolean result = true;

        for (TriggerKey triggerKey : triggerKeys) {
            if (!removeTrigger(triggerKey)) {
                result = false;
            }
        }
        return result;
    }

    @Override
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (internalOperableTrigger == null) {
                return false;
            }

            internalOperableTrigger.setDelegate(newTrigger);

            if (updateTriggerStateToIdle(internalOperableTrigger)) {
                activeTriggers.add(internalOperableTrigger);
            } else {
                activeTriggers.remove(internalOperableTrigger);
            }
        }

        setTriggerStatusInIndex(internalOperableTrigger);

        return true;
    }

    @Override
    public synchronized JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return this.keyToJobMap.get(jobKey);
    }

    @Override
    public synchronized OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return this.keyToTriggerMap.get(triggerKey);
    }

    @Override
    public synchronized boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return this.keyToJobMap.containsKey(jobKey);
    }

    @Override
    public synchronized boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        return this.keyToTriggerMap.containsKey(triggerKey);
    }

    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {
        keyToJobMap.clear();
        keyToTriggerMap.clear();
        groupAndKeyToJobMap.clear();
        groupAndKeyToTriggerMap.clear();
    }

    @Override
    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
            throws ObjectAlreadyExistsException, JobPersistenceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeCalendar(String calName) throws JobPersistenceException {
        return false;
    }

    @Override
    public Calendar retrieveCalendar(String calName) {
        return null;
    }

    @Override
    public synchronized int getNumberOfJobs() throws JobPersistenceException {
        return this.keyToJobMap.size();
    }

    @Override
    public synchronized int getNumberOfTriggers() throws JobPersistenceException {
        return this.keyToTriggerMap.size();
    }

    @Override
    public synchronized int getNumberOfCalendars() throws JobPersistenceException {
        return 0;
    }

    @Override
    public synchronized Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return Collections.unmodifiableSet(this.groupAndKeyToJobMap.row(matcher.getCompareToValue()).keySet());
        } else {
            HashSet<JobKey> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<JobKey, InternalJobDetail>> entry : this.groupAndKeyToJobMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().keySet());
                }
            }

            return result;
        }
    }

    @Override
    public synchronized Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return Collections.unmodifiableSet(this.groupAndKeyToTriggerMap.row(matcher.getCompareToValue()).keySet());
        } else {
            HashSet<TriggerKey> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<TriggerKey, InternalOperableTrigger>> entry : this.groupAndKeyToTriggerMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().keySet());
                }
            }

            return result;
        }
    }

    @Override
    public synchronized List<String> getJobGroupNames() throws JobPersistenceException {
        return new ArrayList<>(this.groupAndKeyToJobMap.rowKeySet());
    }

    @Override
    public synchronized List<String> getTriggerGroupNames() throws JobPersistenceException {
        return new ArrayList<>(this.groupAndKeyToTriggerMap.rowKeySet());
    }

    @Override
    public synchronized List<String> getCalendarNames() throws JobPersistenceException {
        return Collections.emptyList();
    }

    @Override
    public synchronized List<OperableTrigger> getTriggersForJob(JobKey jobKey) {
        InternalJobDetail internalJobDetail = this.keyToJobMap.get(jobKey);

        if (internalJobDetail == null) {
            return null;
        }

        return internalJobDetail.triggers.stream().map(s -> s.delegate).collect(Collectors.toList());
    }

    @Override
    public synchronized TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

        if (internalOperableTrigger == null) {
            return TriggerState.NONE;
        } else {
            return internalOperableTrigger.state.getTriggerState();
        }
    }

    @Override
    public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (internalOperableTrigger == null || internalOperableTrigger.state != InternalOperableTrigger.State.ERROR) {
                return;
            }

            if (updateTriggerStateToIdle(internalOperableTrigger)) {
                activeTriggers.add(internalOperableTrigger);
            } else {
                activeTriggers.remove(internalOperableTrigger);
            }
        }

        setTriggerStatusInIndex(internalOperableTrigger);

    }

    @Override
    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (!pauseTriggerInHeap(internalOperableTrigger)) {
                return;
            }
        }

        setTriggerStatusInIndex(internalOperableTrigger);

    }

    @Override
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Collection<InternalOperableTrigger> matchedTriggers;
        Collection<String> result;

        synchronized (this) {

            matchedTriggers = this.matchTriggers(matcher);

            if (matchedTriggers.isEmpty()) {
                return Collections.emptyList();
            }

            for (InternalOperableTrigger internalOperableTrigger : matchedTriggers) {
                pauseTriggerInHeap(internalOperableTrigger);
            }

            result = matchedTriggers.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        flushDirtyTriggersToIndex();

        return result;
    }

    @Override
    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        synchronized (this) {
            InternalJobDetail internalJobDetail = this.keyToJobMap.get(jobKey);

            if (internalJobDetail == null) {
                return;
            }

            for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                pauseTriggerInHeap(internalOperableTrigger);
            }
        }

        flushDirtyTriggersToIndex();
    }

    @Override
    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        Collection<InternalJobDetail> matchedJobs;
        Collection<String> result;

        synchronized (this) {

            matchedJobs = this.matchJobs(groupMatcher);

            if (matchedJobs.isEmpty()) {
                return Collections.emptyList();
            }

            for (InternalJobDetail internalJobDetail : matchedJobs) {
                for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                    pauseTriggerInHeap(internalOperableTrigger);
                }
            }

            result = matchedJobs.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        flushDirtyTriggersToIndex();

        return result;
    }

    @Override
    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (!resumeTriggerInHeap(internalOperableTrigger)) {
                return;
            }

        }

        setTriggerStatusInIndex(internalOperableTrigger);
    }

    @Override
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Collection<InternalOperableTrigger> matchedTriggers;
        Collection<String> result;

        synchronized (this) {

            matchedTriggers = this.matchTriggers(matcher);

            if (matchedTriggers.isEmpty()) {
                return Collections.emptyList();
            }

            for (InternalOperableTrigger internalOperableTrigger : matchedTriggers) {
                resumeTriggerInHeap(internalOperableTrigger);
            }

            result = matchedTriggers.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        flushDirtyTriggersToIndex();

        return result;
    }

    @Override
    public synchronized Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        return Collections.unmodifiableSet(new HashSet<>(pausedTriggerGroups));
    }

    @Override
    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        synchronized (this) {
            InternalJobDetail internalJobDetail = this.keyToJobMap.get(jobKey);

            if (internalJobDetail == null) {
                return;
            }

            for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                resumeTriggerInHeap(internalOperableTrigger);
            }
        }

        flushDirtyTriggersToIndex();
    }

    @Override
    public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        Collection<InternalJobDetail> matchedJobs;
        Collection<String> result;

        synchronized (this) {

            matchedJobs = this.matchJobs(groupMatcher);

            if (matchedJobs.isEmpty()) {
                return Collections.emptyList();
            }

            for (InternalJobDetail internalJobDetail : matchedJobs) {
                for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                    resumeTriggerInHeap(internalOperableTrigger);
                }
            }

            result = matchedJobs.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        flushDirtyTriggersToIndex();

        return result;
    }

    @Override
    public void pauseAll() throws JobPersistenceException {
        synchronized (this) {
            for (InternalOperableTrigger internalOperableTrigger : this.keyToTriggerMap.values()) {
                pauseTriggerInHeap(internalOperableTrigger);
            }
        }

        flushDirtyTriggersToIndex();
    }

    @Override
    public void resumeAll() throws JobPersistenceException {

        synchronized (this) {
            for (InternalOperableTrigger internalOperableTrigger : this.keyToTriggerMap.values()) {
                resumeTriggerInHeap(internalOperableTrigger);
            }
        }

        flushDirtyTriggersToIndex();
    }

    @Override
    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException {
        if (log.isDebugEnabled()) {
            log.debug("acquireNextTriggers(noLaterThan = " + new Date(noLaterThan) + ", maxCount = " + maxCount + ", timeWindow =" + timeWindow
                    + ") for " + this);
        }

        List<OperableTrigger> result;

        synchronized (this) {
            if (log.isDebugEnabled()) {
                log.debug("Number of active triggers: " + this.activeTriggers.size());
            }

            if (this.activeTriggers.isEmpty()) {
                return Collections.emptyList();
            }

            log.debug("Active triggers: " + activeTriggers);

            result = new ArrayList<OperableTrigger>(Math.min(maxCount, this.activeTriggers.size()));
            Set<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
            Set<InternalOperableTrigger> excludedTriggers = new HashSet<InternalOperableTrigger>();
            long misfireIsBefore = System.currentTimeMillis() - misfireThreshold;

            long batchEnd = noLaterThan;

            for (ActiveTrigger activeTrigger = activeTriggers.pollFirst(); activeTrigger != null; activeTrigger = activeTriggers.pollFirst()) {
                if (activeTrigger.getNextFireTime() == null) {
                    continue;
                }

                InternalOperableTrigger trigger = activeTrigger.delegate;

                if (checkForMisfire(trigger, misfireIsBefore)) {
                    if (trigger.getNextFireTime() != null) {
                        this.activeTriggers.add(trigger);
                    }
                    markDirty(trigger);
                    continue;
                }

                if (activeTrigger.getNextFireTime().getTime() > batchEnd) {
                    activeTriggers.add(trigger);
                    break;
                }

                InternalJobDetail internalJobDetail = keyToJobMap.get(trigger.getJobKey());

                if (internalJobDetail.isConcurrentExectionDisallowed()) {
                    if (acquiredJobKeysForNoConcurrentExec.contains(internalJobDetail.getKey())) {
                        excludedTriggers.add(trigger);
                        continue;
                    } else {
                        acquiredJobKeysForNoConcurrentExec.add(trigger.getJobKey());
                    }
                }

                trigger.setStateAndNode(InternalOperableTrigger.State.ACQUIRED, nodeId);
                trigger.setFireInstanceId(UUID.randomUUID().toString());

                if (result.isEmpty()) {
                    batchEnd = Math.max(activeTrigger.getNextFireTime().getTime(), System.currentTimeMillis()) + timeWindow;
                }

                result.add(trigger);

                if (result.size() >= maxCount) {
                    break;
                }
            }

            this.activeTriggers.addAll(excludedTriggers);
        }

        flushDirtyTriggersToIndex();

        log.debug("Result: " + result);

        return result;
    }

    @Override
    public void releaseAcquiredTrigger(OperableTrigger trigger) {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(trigger.getKey());

            if (internalOperableTrigger == null) {
                return;
            }

            if (internalOperableTrigger.state != InternalOperableTrigger.State.ACQUIRED) {
                return;
            }

            internalOperableTrigger.state = InternalOperableTrigger.State.WAITING;

            activeTriggers.add(internalOperableTrigger);

        }

        setTriggerStatusInIndex(internalOperableTrigger);

    }

    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> firedTriggers) throws JobPersistenceException {
        List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>(firedTriggers.size());

        if (log.isDebugEnabled()) {
            log.debug("triggersFired(" + firedTriggers + ")");
        }

        synchronized (this) {

            for (OperableTrigger trigger : firedTriggers) {
                InternalOperableTrigger internalOperableTrigger = toInternal(trigger);

                if (internalOperableTrigger == null) {
                    continue;
                }

                if (internalOperableTrigger.state != InternalOperableTrigger.State.ACQUIRED) {
                    continue;
                }

                InternalJobDetail jobDetail = this.keyToJobMap.get(trigger.getJobKey());

                if (jobDetail == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Could not find job detail for fired trigger: " + trigger
                                + "; this probably means that the job was deleted after the trigger was acquired. Skipping this trigger");
                    }
                    activeTriggers.remove(internalOperableTrigger);
                    internalOperableTrigger.state = InternalOperableTrigger.State.DELETED;
                    markDirty(internalOperableTrigger);
                    continue;
                }

                Date previousFireTime = trigger.getPreviousFireTime();
                Date scheduledFireTime = trigger.getNextFireTime();

                activeTriggers.remove(internalOperableTrigger);

                // fire time attributes are updated here
                internalOperableTrigger.triggered(null);
                internalOperableTrigger.state = InternalOperableTrigger.State.EXECUTING;
                internalOperableTrigger.node = nodeId;
                markDirty(internalOperableTrigger);

                TriggerFiredBundle triggerFiredBundle = new TriggerFiredBundle(jobDetail, trigger, null, false, new Date(), scheduledFireTime,
                        previousFireTime, trigger.getNextFireTime());

                if (jobDetail.isConcurrentExectionDisallowed()) {
                    jobDetail.blockIdleTriggers();
                } else if (internalOperableTrigger.getNextFireTime() != null) {
                    this.activeTriggers.add(internalOperableTrigger);
                }

                results.add(new TriggerFiredResult(triggerFiredBundle));
            }
        }

        flushDirtyTriggersToIndex();

        if (log.isDebugEnabled()) {
            log.debug("triggersFired() = " + results);

        }

        return results;

    }

    @Override
    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
        if (log.isDebugEnabled()) {
            log.debug("triggeredJobComplete(" + trigger + ")");
        }

        synchronized (this) {
            InternalJobDetail internalJobDetail = toInternal(jobDetail);
            InternalOperableTrigger internalOperableTrigger = toInternal(trigger);

            if (internalJobDetail != null) {
                if (internalJobDetail.isPersistJobDataAfterExecution()) {
                    JobDataMap newData = jobDetail.getJobDataMap();
                    if (newData != null) {
                        newData = (JobDataMap) newData.clone();
                        newData.clearDirtyFlag();
                    }
                    internalJobDetail.delegate = internalJobDetail.getJobBuilder().setJobData(newData).build();
                }

                if (internalJobDetail.isConcurrentExectionDisallowed()) {
                    internalJobDetail.deblockTriggers();
                    signaler.signalSchedulingChange(0L);
                }
            } else {
                blockedJobs.remove(jobDetail.getKey());
            }

            if (internalOperableTrigger != null) {
                switch (triggerInstCode) {
                case DELETE_TRIGGER:
                    log.error("DELETE_TRIGGER is not supported: " + internalOperableTrigger);
                    internalOperableTrigger.setState(InternalOperableTrigger.State.ERROR);
                    this.activeTriggers.remove(internalOperableTrigger);
                    signaler.signalSchedulingChange(0L);
                    break;
                case SET_TRIGGER_COMPLETE:
                    internalOperableTrigger.setState(InternalOperableTrigger.State.COMPLETE);
                    this.activeTriggers.remove(internalOperableTrigger);
                    signaler.signalSchedulingChange(0L);
                    break;
                case SET_TRIGGER_ERROR:
                    internalOperableTrigger.setState(InternalOperableTrigger.State.ERROR);
                    this.activeTriggers.remove(internalOperableTrigger);
                    signaler.signalSchedulingChange(0L);
                    break;
                case SET_ALL_JOB_TRIGGERS_ERROR:
                    setAllTriggersOfJobToState(internalJobDetail, InternalOperableTrigger.State.ERROR);
                    signaler.signalSchedulingChange(0L);
                    break;
                case SET_ALL_JOB_TRIGGERS_COMPLETE:
                    setAllTriggersOfJobToState(internalJobDetail, InternalOperableTrigger.State.COMPLETE);
                    signaler.signalSchedulingChange(0L);
                    break;
                default:
                    internalOperableTrigger.setState(InternalOperableTrigger.State.WAITING);
                    if (this.keyToJobMap.containsKey(internalOperableTrigger.getJobKey())) {
                        // Only add to activeTriggers if this scheduler still "knows" this job. Otherwise, it might have been moved to another node
                        this.activeTriggers.add(internalOperableTrigger);
                    }
                    break;
                }
            }
        }

        flushDirtyTriggersToIndex();

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

    public String getNodeId() {
        return nodeId;
    }

    private InternalJobDetail toInternal(JobDetail jobDetail) {
        if (jobDetail instanceof IndexJobStateStore.InternalJobDetail) {
            return (InternalJobDetail) jobDetail;
        } else {
            return this.keyToJobMap.get(jobDetail.getKey());
        }
    }

    private InternalOperableTrigger toInternal(OperableTrigger trigger) {
        if (trigger instanceof InternalOperableTrigger) {
            return (InternalOperableTrigger) trigger;
        } else {
            return this.keyToTriggerMap.get(trigger.getKey());
        }
    }

    private synchronized InternalOperableTrigger storeTriggerInHeap(OperableTrigger newTrigger, boolean replaceExisting)
            throws ObjectAlreadyExistsException, JobPersistenceException {

        InternalJobDetail internalJobDetail = this.keyToJobMap.get(newTrigger.getJobKey());

        if (internalJobDetail == null) {
            throw new JobPersistenceException("Trigger " + newTrigger + " references non-existing job" + newTrigger.getJobKey());
        }

        // TODO replaceexisting

        return storeTriggerInHeap(internalJobDetail, newTrigger);
    }

    private synchronized InternalOperableTrigger storeTriggerInHeap(InternalJobDetail internalJobDetail, OperableTrigger newTrigger) {

        InternalOperableTrigger internalOperableTrigger = new InternalOperableTrigger(this, newTrigger);

        internalJobDetail.addTrigger(internalOperableTrigger);

        addToCollections(internalOperableTrigger);

        internalOperableTrigger.computeFirstFireTime(null);
        internalOperableTrigger.node = this.nodeId;

        updateTriggerStateToIdle(internalOperableTrigger);

        if (internalOperableTrigger.getState() == InternalOperableTrigger.State.WAITING) {
            this.activeTriggers.add(internalOperableTrigger);
        }

        return internalOperableTrigger;
    }

    private synchronized void addToCollections(InternalJobDetail internalJobDetail) {
        keyToJobMap.put(internalJobDetail.getKey(), internalJobDetail);
        groupAndKeyToJobMap.put(internalJobDetail.getKey().getGroup(), internalJobDetail.getKey(), internalJobDetail);

        if (!internalJobDetail.triggers.isEmpty()) {
            for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                this.addToCollections(internalOperableTrigger);
            }
        }
    }

    private synchronized void addToCollections(InternalOperableTrigger internalOperableTrigger) {
        this.groupAndKeyToTriggerMap.put(internalOperableTrigger.getKey().getGroup(), internalOperableTrigger.getKey(), internalOperableTrigger);
        this.keyToTriggerMap.put(internalOperableTrigger.getKey(), internalOperableTrigger);
    }

    private synchronized void initActiveTriggers() {
        activeTriggers.clear();

        for (InternalOperableTrigger trigger : this.keyToTriggerMap.values()) {
            if (trigger.state == InternalOperableTrigger.State.WAITING) {
                activeTriggers.add(trigger);
            }
        }
    }

    private boolean updateTriggerStateToIdle(InternalOperableTrigger internalOperableTrigger) {
        if (pausedTriggerGroups.contains(internalOperableTrigger.getKey().getGroup())
                || pausedJobGroups.contains(internalOperableTrigger.getJobKey().getGroup())) {
            if (blockedJobs.contains(internalOperableTrigger.getJobKey())) {
                internalOperableTrigger.setState(InternalOperableTrigger.State.PAUSED_BLOCKED);
            } else {
                internalOperableTrigger.setState(InternalOperableTrigger.State.PAUSED);
            }
            return false;
        } else if (blockedJobs.contains(internalOperableTrigger.getJobKey())) {
            internalOperableTrigger.setState(InternalOperableTrigger.State.BLOCKED);
            return false;
        } else {
            internalOperableTrigger.setState(InternalOperableTrigger.State.WAITING);
            return true;
        }
    }

    private void setTriggerStatusInIndex(InternalOperableTrigger internalOperableTrigger) {
        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
            internalOperableTrigger.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            IndexRequest indexRequest = new IndexRequest(statusIndexName).id(internalOperableTrigger.getKeyString()).source(xContentBuilder);

            this.client.index(indexRequest);
        } catch (Exception e) {
            log.error("Error while writing trigger status: " + internalOperableTrigger, e);
            this.dirtyTriggers.get().add(internalOperableTrigger);
        }
    }

    private synchronized Collection<InternalOperableTrigger> matchTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return this.groupAndKeyToTriggerMap.row(matcher.getCompareToValue()).values();
        } else {
            HashSet<InternalOperableTrigger> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<TriggerKey, InternalOperableTrigger>> entry : this.groupAndKeyToTriggerMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().values());
                }
            }

            return result;
        }
    }

    private synchronized Collection<InternalJobDetail> matchJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return this.groupAndKeyToJobMap.row(matcher.getCompareToValue()).values();
        } else {
            HashSet<InternalJobDetail> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<JobKey, InternalJobDetail>> entry : this.groupAndKeyToJobMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().values());
                }
            }

            return result;
        }
    }

    private synchronized boolean pauseTriggerInHeap(InternalOperableTrigger internalOperableTrigger) {
        if (internalOperableTrigger == null || internalOperableTrigger.state == InternalOperableTrigger.State.COMPLETE) {
            return false;
        }

        if (internalOperableTrigger.state == InternalOperableTrigger.State.BLOCKED) {
            internalOperableTrigger.state = InternalOperableTrigger.State.PAUSED_BLOCKED;
        } else {
            internalOperableTrigger.state = InternalOperableTrigger.State.PAUSED;
        }

        activeTriggers.remove(internalOperableTrigger);
        markDirty(internalOperableTrigger);

        return true;
    }

    private synchronized boolean resumeTriggerInHeap(InternalOperableTrigger internalOperableTrigger) {
        if (internalOperableTrigger == null) {
            return false;
        }

        if (internalOperableTrigger.state != InternalOperableTrigger.State.PAUSED
                && internalOperableTrigger.state != InternalOperableTrigger.State.PAUSED_BLOCKED) {
            return false;
        }

        internalOperableTrigger.state = InternalOperableTrigger.State.WAITING;

        checkForMisfire(internalOperableTrigger);

        if (updateTriggerStateToIdle(internalOperableTrigger)) {
            activeTriggers.add(internalOperableTrigger);
        } else {
            activeTriggers.remove(internalOperableTrigger);
        }

        markDirty(internalOperableTrigger);
        return true;
    }

    private synchronized void setAllTriggersOfJobToState(InternalJobDetail internalJobDetail, InternalOperableTrigger.State state) {

        for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
            if (internalOperableTrigger.state == state) {
                continue;
            }

            internalOperableTrigger.state = state;

            if (state != InternalOperableTrigger.State.WAITING) {
                this.activeTriggers.remove(internalOperableTrigger);
            }

            markDirty(internalOperableTrigger);
        }

    }

    private void initJobs() {
        Collection<InternalJobDetail> jobs = this.loadJobs();

        boolean triggersStillExecutingOnOtherNodesExist = false;

        synchronized (this) {
            resetJobs();

            for (InternalJobDetail job : jobs) {
                addToCollections(job);
            }

            initActiveTriggers();

            if (triggersStillExecutingOnOtherNodes.size() > 0) {
                triggersStillExecutingOnOtherNodesExist = true;
            }

            log.info("Scheduler " + schedulerName + " is initialized. Jobs: " + jobs.size() + " Active Triggers: " + activeTriggers.size());
        }

        initialized = true;

        flushDirtyTriggersToIndex();

        if (triggersStillExecutingOnOtherNodesExist) {
            periodicMaintenanceExecutor.getQueue().clear();
            periodicMaintenanceExecutor.schedule(() -> checkTriggersStillExecutingOnOtherNodes(), 10, TimeUnit.SECONDS);
        }
    }

    private void updateAfterClusterConfigChange() {
        try {
            log.info("Reinitializing jobs for " + IndexJobStateStore.this);
            periodicMaintenanceExecutor.getQueue().clear();
            initJobs();
            signaler.signalSchedulingChange(0L);
            log.debug("Finished reinitializing jobs for " + IndexJobStateStore.this);
        } catch (Exception e) {
            try {
                // Let a potential cluster shutdown catch up
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                log.debug(e1);
            }
            if (!shutdown) {
                log.error("Error while initializing jobs for " + IndexJobStateStore.this, e);
            }
        }
    }

    private synchronized void resetJobs() {
        keyToJobMap.clear();
        keyToTriggerMap.clear();
        groupAndKeyToJobMap.clear();
        groupAndKeyToTriggerMap.clear();
        activeTriggers.clear();
    }

    private Collection<InternalJobDetail> loadJobs() {
        long start = System.currentTimeMillis();

        try {
            if (log.isTraceEnabled()) {
                log.trace("Going to load jobs; ");
            }

            Set<JobType> jobConfigSet = this.loadJobConfigAfterReachingYellowStatus();

            notifyJobConfigInitListeners(jobConfigSet);

            if (log.isDebugEnabled()) {
                log.debug("Job configurations loaded: " + jobConfigSet);
            }

            Map<TriggerKey, InternalOperableTrigger> triggerStates = this.loadTriggerStates(jobConfigSet);
            Collection<InternalJobDetail> result = new ArrayList<>(jobConfigSet.size());

            for (JobType jobConfig : jobConfigSet) {
                result.add(createInternalJobDetailFromJobConfig(jobConfig, triggerStates));
            }

            if (log.isInfoEnabled()) {
                log.info("Jobs loaded: " + result);
            }

            return result;
        } catch (ElasticsearchException e) {
            log.info("loadJobs() failed after " + (System.currentTimeMillis() - start) + " ms", e);
            throw e;
        }
    }

    public String updateJobs() {
        Set<JobType> newJobConfig = this.loadJobConfig();
        Map<JobKey, InternalJobDetail> newJobs = new HashMap<>();
        Map<JobKey, InternalJobDetail> updatedJobs = new HashMap<>();
        Map<JobKey, InternalJobDetail> deletedJobs = new HashMap<>();
        Set<JobType> newJobTypes = new HashSet<>();
        Map<JobType, JobType> updatedJobTypes = new HashMap<>();
        Set<JobType> deletedJobTypes = new HashSet<>();
        Set<JobKey> newJobKeys = new HashSet<>(newJobConfig.size());

        Map<JobKey, JobType> loadedJobConfig = this.getLoadedJobConfig();

        if (log.isInfoEnabled()) {
            log.info("Updating jobs:\n " + newJobConfig + "\n");
        }

        // First pass: Collect new jobs so that we can load the states

        for (JobType newJob : newJobConfig) {
            JobKey jobKey = newJob.getJobKey();
            JobType existingJob = loadedJobConfig.get(jobKey);

            if (existingJob == null) {
                newJobTypes.add(newJob);
            }
        }

        notifyJobConfigListenersBeforeChange(newJobTypes);

        // Second pass: do the actual update; inside a critical block

        synchronized (this) {
            loadedJobConfig = this.getLoadedJobConfig();
            newJobTypes.clear();

            if (log.isDebugEnabled()) {
                log.debug("Loaded job config:\n" + loadedJobConfig + "\n");
            }

            for (JobType newJob : newJobConfig) {
                JobKey jobKey = newJob.getJobKey();
                newJobKeys.add(jobKey);
                JobType existingJob = loadedJobConfig.get(jobKey);

                if (log.isDebugEnabled()) {
                    log.info("New job: " + newJob + "; existing job: " + existingJob);
                }

                if (existingJob == null) {
                    InternalJobDetail newJobDetail = createInternalJobDetailFromJobConfig(newJob, Collections.emptyMap());
                    newJobs.put(jobKey, newJobDetail);
                    newJobTypes.add(newJob);
                    addToCollections(newJobDetail);
                    newJobDetail.engageTriggers();

                } else if (existingJob.getVersion() < newJob.getVersion() || existingJob.getVersion() == -1 || newJob.getVersion() == -1) {
                    InternalJobDetail existingJobDetail = this.keyToJobMap.get(existingJob.getJobKey());

                    if (existingJobDetail != null) {

                        if (updateJob(existingJobDetail, existingJob, newJob)) {
                            updatedJobs.put(jobKey, createInternalJobDetailFromJobConfig(newJob, Collections.emptyMap()));
                        }

                        updatedJobTypes.put(existingJob, newJob);
                    } else {
                        log.info("Found existing job config but no matching job detail for " + existingJob
                                + ". This is a bit weird. Will create job detail now.");

                        InternalJobDetail newJobDetail = createInternalJobDetailFromJobConfig(newJob, Collections.emptyMap());
                        newJobs.put(jobKey, newJobDetail);
                        newJobTypes.add(newJob);
                        addToCollections(newJobDetail);
                        newJobDetail.engageTriggers();
                    }
                }
            }

            for (JobKey existingJob : loadedJobConfig.keySet()) {
                if (!newJobKeys.contains(existingJob)) {
                    InternalJobDetail jobDetail = this.keyToJobMap.get(existingJob);
                    deletedJobs.put(existingJob, jobDetail);
                    @SuppressWarnings("unchecked")
                    JobType deletedJobConfig = (JobType) jobDetail.baseConfig;
                    deletedJobTypes.add(deletedJobConfig);
                    removeJob(jobDetail);
                }
            }
        }

        flushDirtyTriggersToIndex();

        if (newJobs.size() != 0 || updatedJobs.size() != 0 || deletedJobs.size() != 0) {
            signaler.signalSchedulingChange(0L);

            log.info("Job update finished.\nNew Jobs: " + newJobs.values() + "\nUpdated Jobs: " + updatedJobs.values() + "\nDeleted Jobs: "
                    + deletedJobs.values());

            notifyJobConfigListenersAfterChange(newJobTypes, updatedJobTypes, deletedJobTypes);

            return "new: " + newJobs.values().size() + "; upd: " + updatedJobs.size() + "; del: " + deletedJobs.size();
        } else {
            log.info("Job update finished. Nothing changed.");

            return "no changes";
        }

    }

    public void addJobConfigListener(JobConfigListener<JobType> jobCofigListener) {
        this.jobConfigListeners.add(jobCofigListener);
    }

    private void notifyJobConfigInitListeners(Set<JobType> jobs) {

        for (JobConfigListener<JobType> listener : this.jobConfigListeners) {
            try {
                listener.onInit(jobs);
            } catch (Exception e) {
                log.error("Exception in JobConfigListener.onInit()", e);
            }
        }
    }

    private void notifyJobConfigListenersBeforeChange(Set<JobType> newJobs) {

        for (JobConfigListener<JobType> listener : this.jobConfigListeners) {
            try {
                listener.beforeChange(newJobs);
            } catch (Exception e) {
                log.error("Exception in JobConfigListener.beforeChange()", e);
            }
        }
    }

    private void notifyJobConfigListenersAfterChange(Set<JobType> newJobs, Map<JobType, JobType> updatedJobs, Set<JobType> deletedJobs) {

        for (JobConfigListener<JobType> listener : this.jobConfigListeners) {
            try {
                listener.afterChange(newJobs, updatedJobs, deletedJobs);
            } catch (Exception e) {
                log.error("Exception in JobConfigListener.afterChange()", e);
            }
        }
    }

    private synchronized boolean updateJob(InternalJobDetail existingJobDetail, JobType existingJobConfig, JobType newJobConfig) {
        existingJobDetail.baseConfig = newJobConfig;
        JobDetail newJobDetail = this.jobFactory.createJobDetail(newJobConfig);
        boolean changed = false;

        if (!areJobDetailsEqual(existingJobDetail, newJobDetail)) {
            existingJobDetail.delegate = newJobDetail;
            changed = true;
        }

        if (updateTriggers(existingJobDetail, existingJobConfig, newJobConfig)) {
            changed = true;
        }

        return changed;
    }

    private boolean areJobDetailsEqual(JobDetail existingJobDetail, JobDetail newJobDetail) {

        if (!Objects.equal(existingJobDetail.getDescription(), newJobDetail.getDescription())) {
            return false;
        }

        if (existingJobDetail.getJobClass() != newJobDetail.getJobClass()) {
            return false;
        }

        if (existingJobDetail.isDurable() != newJobDetail.isDurable()) {
            return false;
        }

        if (!Objects.equal(existingJobDetail.getJobDataMap(), newJobDetail.getJobDataMap())) {
            return false;
        }

        return true;
    }

    private synchronized boolean updateTriggers(InternalJobDetail existingJobDetail, JobType existingJobConfig, JobType newJobConfig) {
        Map<TriggerKey, InternalOperableTrigger> existingTriggers = existingJobDetail.getTriggersAsMap();
        Map<TriggerKey, InternalOperableTrigger> existingTriggersNotInNewTriggers = new HashMap<>(existingTriggers);
        Set<TriggerKey> newTriggerKeys = new HashSet<>();
        Map<TriggerKey, InternalOperableTrigger> newTriggers = new HashMap<>();
        Map<TriggerKey, InternalOperableTrigger> changedTriggers = new HashMap<>();
        boolean changed = false;

        for (Trigger newTrigger : newJobConfig.getTriggers()) {
            newTriggerKeys.add(newTrigger.getKey());
            InternalOperableTrigger existingTrigger = existingTriggers.get(newTrigger.getKey());

            if (existingTrigger == null) {
                InternalOperableTrigger newInternalOperableTrigger = storeTriggerInHeap(existingJobDetail, (OperableTrigger) newTrigger);
                newInternalOperableTrigger.markDirty();
                newTriggers.put(newInternalOperableTrigger.getKey(), newInternalOperableTrigger);
                changed = true;
            } else if (!existingTrigger.delegate.equals(newTrigger)) {
                existingTrigger.delegate = (OperableTrigger) newTrigger;
                existingTrigger.markDirty();
                changedTriggers.put(existingTrigger.getKey(), existingTrigger);
                changed = true;
                existingTriggersNotInNewTriggers.remove(existingTrigger.getKey());
            } else {
                // unchanged
                existingTriggersNotInNewTriggers.remove(existingTrigger.getKey());
            }
        }

        for (Map.Entry<TriggerKey, InternalOperableTrigger> entry : existingTriggersNotInNewTriggers.entrySet()) {
            this.keyToTriggerMap.remove(entry.getKey());
            this.groupAndKeyToTriggerMap.remove(entry.getKey().getGroup(), entry.getKey());
            this.activeTriggers.remove(entry.getValue());
            entry.getValue().jobDetail.triggers.remove(entry.getValue());
            changed = true;
        }

        if (changed) {
            log.info("Updated triggers of " + existingJobDetail + ":\nNew triggers: " + newTriggers + "\nChanged triggers: " + changedTriggers
                    + "\nRemoved triggers: " + existingTriggersNotInNewTriggers);
        } else {
            log.info("No triggers of " + existingJobConfig + " have been changed");
        }
        return changed;
    }

    private synchronized void removeJob(InternalJobDetail jobDetail) {
        if (jobDetail == null) {
            return;
        }

        JobKey jobKey = jobDetail.getKey();

        this.groupAndKeyToJobMap.remove(jobKey.getGroup(), jobKey);
        this.blockedJobs.remove(jobKey);
        this.keyToJobMap.remove(jobKey);

        for (InternalOperableTrigger trigger : jobDetail.triggers) {
            TriggerKey triggerKey = trigger.getKey();

            this.groupAndKeyToTriggerMap.remove(triggerKey.getGroup(), triggerKey);
            this.keyToTriggerMap.remove(triggerKey);
            this.activeTriggers.remove(trigger);
        }
    }

    private InternalJobDetail createInternalJobDetailFromJobConfig(JobType jobConfig, Map<TriggerKey, InternalOperableTrigger> triggerStates) {
        InternalJobDetail internalJobDetail = new InternalJobDetail(this.jobFactory.createJobDetail(jobConfig), jobConfig, this);

        for (Trigger triggerConfig : jobConfig.getTriggers()) {
            if (!(triggerConfig instanceof OperableTrigger)) {
                log.error("Trigger is not OperableTrigger: " + triggerConfig);
                continue;
            }

            OperableTrigger operableTriggerConfig = (OperableTrigger) triggerConfig;
            InternalOperableTrigger internalOperableTrigger = triggerStates.get(triggerConfig.getKey());

            if (internalOperableTrigger != null) {
                internalOperableTrigger.setDelegate(operableTriggerConfig);
                checkTriggerStateAfterRecovery(internalOperableTrigger);
            } else {
                internalOperableTrigger = new InternalOperableTrigger(this, operableTriggerConfig);
                internalOperableTrigger.computeFirstFireTime(null);
                internalOperableTrigger.node = this.nodeId;

                updateTriggerStateToIdle(internalOperableTrigger);
            }

            internalJobDetail.addTrigger(internalOperableTrigger);
        }

        return internalJobDetail;
    }

    private void checkTriggersStillExecutingOnOtherNodes() {
        Set<TriggerKey> triggerKeys;

        synchronized (this) {
            if (this.triggersStillExecutingOnOtherNodes.isEmpty()) {
                return;
            }

            triggerKeys = new HashSet<>();
            Iterator<InternalOperableTrigger> iter = this.triggersStillExecutingOnOtherNodes.iterator();

            while (iter.hasNext()) {
                InternalOperableTrigger trigger = iter.next();

                if (trigger.getState() == InternalOperableTrigger.State.EXECUTING) {
                    triggerKeys.add(trigger.getKey());
                } else {
                    iter.remove();
                }
            }
        }

        if (triggerKeys.isEmpty()) {
            return;
        }

        ArrayList<String> triggerKeysAsString = new ArrayList<>(triggerKeys.size());

        for (TriggerKey triggerKey : triggerKeys) {
            triggerKeysAsString.add(triggerKey.toString());
        }

        client.execute(CheckForExecutingTriggerAction.INSTANCE,
                new CheckForExecutingTriggerRequest(schedulerName, new ArrayList<>(triggerKeysAsString)),
                new ActionListener<CheckForExecutingTriggerResponse>() {

                    @Override
                    public void onResponse(CheckForExecutingTriggerResponse response) {
                        Set<TriggerKey> triggersToBeReset = new HashSet<>(triggerKeys);
                        triggersToBeReset.removeAll(response.getAllRunningTriggerKeys());

                        log.info("Triggers to be reset after CheckForExecutingTriggerAction: " + triggersToBeReset);

                        if (triggersToBeReset.size() > 0) {
                            resetTriggersFormerlyRunningOnOtherNodes(triggersToBeReset);
                        }

                        if (triggersStillExecutingOnOtherNodes.size() > 0) {
                            periodicMaintenanceExecutor.getQueue().clear();
                            periodicMaintenanceExecutor.schedule(() -> checkTriggersStillExecutingOnOtherNodes(), 10, TimeUnit.SECONDS);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("CheckForExecutingTriggerAction failed", e);
                    }
                });
    }

    private void resetTriggersFormerlyRunningOnOtherNodes(Set<TriggerKey> triggersToBeReset) {
        Map<TriggerKey, InternalOperableTrigger> triggerStates = loadTriggerStatesByTriggerKeys(triggersToBeReset);

        synchronized (this) {
            for (InternalOperableTrigger refreshedTrigger : triggerStates.values()) {
                InternalOperableTrigger actualTrigger = this.keyToTriggerMap.get(refreshedTrigger.getKey());

                if (actualTrigger == null) {
                    log.error("Could not find actualTrigger for " + refreshedTrigger);
                    continue;
                }

                this.triggersStillExecutingOnOtherNodes.remove(actualTrigger);

                InternalJobDetail internalJobDetail = actualTrigger.getJobDetail();

                if (actualTrigger.getState() != InternalOperableTrigger.State.EXECUTING) {
                    // Something has refreshed this before
                    continue;
                }

                if (refreshedTrigger.getState() == InternalOperableTrigger.State.EXECUTING) {
                    if (actualTrigger.getNextFireTime() == null) {
                        actualTrigger.computeFirstFireTime(null);
                    }
                    actualTrigger.node = this.nodeId;
                    updateTriggerStateToIdle(actualTrigger);

                    // TODO avoid/ignore version conflicts

                } else {
                    actualTrigger.nextFireTime = refreshedTrigger.nextFireTime;
                    actualTrigger.state = refreshedTrigger.state;
                    actualTrigger.previousFireTime = refreshedTrigger.previousFireTime;
                    actualTrigger.node = this.nodeId;
                    actualTrigger.timesTriggered = refreshedTrigger.timesTriggered;
                }

                if (actualTrigger.state == InternalOperableTrigger.State.WAITING) {
                    if (internalJobDetail.isConcurrentExectionDisallowed()) {
                        internalJobDetail.deblockTriggers();
                        signaler.signalSchedulingChange(0L);
                    }
                    activeTriggers.add(actualTrigger);
                }
            }
        }

        flushDirtyTriggersToIndex();
    }

    @SuppressWarnings("unchecked")
    private synchronized Map<JobKey, JobType> getLoadedJobConfig() {
        HashMap<JobKey, JobType> result = new HashMap<>(this.keyToJobMap.size());

        for (Map.Entry<JobKey, InternalJobDetail> entry : this.keyToJobMap.entrySet()) {
            result.put(entry.getKey(), (JobType) entry.getValue().baseConfig);
        }

        return result;
    }

    private synchronized void checkTriggerStateAfterRecovery(InternalOperableTrigger internalOperableTrigger) {
        switch (internalOperableTrigger.getState()) {
        case EXECUTING:
            if (this.nodeId.equals(internalOperableTrigger.getNode())) {
                log.info("Trigger " + internalOperableTrigger + " is still executing on local node.");
            } else {
                log.info("Trigger " + internalOperableTrigger + " is marked as still executing on node " + internalOperableTrigger.getNode());

                this.triggersStillExecutingOnOtherNodes.add(internalOperableTrigger);
            }
            break;
        case ACQUIRED:
        case BLOCKED:
        case WAITING:
            if (internalOperableTrigger.getNextFireTime() == null) {
                internalOperableTrigger.computeFirstFireTime(null);
            }
            internalOperableTrigger.node = this.nodeId;
            updateTriggerStateToIdle(internalOperableTrigger);
            break;
        case PAUSED_BLOCKED:
            if (internalOperableTrigger.getNextFireTime() == null) {
                internalOperableTrigger.computeFirstFireTime(null);
            }
            internalOperableTrigger.setStateAndNode(InternalOperableTrigger.State.PAUSED, this.nodeId);
            break;
        default:
            // No change needed
            break;
        }
    }

    private Map<TriggerKey, InternalOperableTrigger> loadTriggerStates(Set<JobType> jobConfig) {
        Map<String, TriggerKey> triggerIds = this.getTriggerIds(jobConfig);

        return loadTriggerStates(triggerIds);
    }

    private Map<TriggerKey, InternalOperableTrigger> loadTriggerStatesByTriggerKeys(Set<TriggerKey> triggerKeys) {
        Map<String, TriggerKey> triggerIds = this.getTriggerIdsByTriggerKeys(triggerKeys);

        return loadTriggerStates(triggerIds);
    }

    private Map<TriggerKey, InternalOperableTrigger> loadTriggerStates(Map<String, TriggerKey> triggerIds) {
        try {

            if (triggerIds.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<TriggerKey, InternalOperableTrigger> result = new HashMap<>(triggerIds.size());

            QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds(triggerIds.keySet().toArray(new String[triggerIds.size()]));

            SearchResponse searchResponse = client.prepareSearch(this.statusIndexName).setQuery(queryBuilder).setSize(1000)
                    .setScroll(new TimeValue(10000)).get();

            try {
                do {
                    for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                        try {
                            TriggerKey triggerKey = triggerIds.get(searchHit.getId());

                            InternalOperableTrigger internalOperableTrigger = InternalOperableTrigger.fromAttributeMap(this, triggerKey,
                                    searchHit.getSourceAsMap());

                            result.put(triggerKey, internalOperableTrigger);

                        } catch (Exception e) {
                            log.error("Error while loading " + searchHit, e);
                        }
                    }

                    searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(10000)).execute().actionGet();

                } while (searchResponse.getHits().getHits().length != 0);
            } finally {
                Actions.clearScrollAsync(client, searchResponse);
            }
            return result;
        } catch (IndexNotFoundException e) {
            return Collections.emptyMap();
        } catch (ElasticsearchException e) {
            log.error("Error in loadTriggerStates()", e);
            throw e;
        }
    }

    private Map<String, TriggerKey> getTriggerIds(Set<JobType> jobConfig) {
        Map<String, TriggerKey> result = new HashMap<String, TriggerKey>(jobConfig.size() * 3);

        for (JobConfig job : jobConfig) {
            for (Trigger trigger : job.getTriggers()) {
                result.put(quartzKeyToKeyString(trigger.getKey()), trigger.getKey());
            }
        }

        return result;
    }

    private Map<String, TriggerKey> getTriggerIdsByTriggerKeys(Set<TriggerKey> triggerKeys) {
        Map<String, TriggerKey> result = new HashMap<String, TriggerKey>(triggerKeys.size());

        for (TriggerKey triggerKey : triggerKeys) {
            result.put(quartzKeyToKeyString(triggerKey), triggerKey);
        }

        return result;
    }

    private Set<JobType> loadJobConfigAfterReachingYellowStatus() {
        try {
            // TODO XXX
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForYellowStatus()
                    .setWaitForNoInitializingShards(true).setTimeout(TimeValue.timeValueSeconds(1)).execute().actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Cluster health before loading job config: " + clusterHealthResponse);
            }

            if (clusterHealthResponse.isTimedOut()) {
                log.warn("Timeout while waiting for initialized cluster status. Will try to continue anyway. " + clusterHealthResponse);
            }

        } catch (ElasticsearchException e) {
            log.error("Error while getting cluster health ", e);
            throw e;
        }

        return Sets.newHashSet(this.jobConfigSource);
    }

    private Set<JobType> loadJobConfig() {
        return Sets.newHashSet(this.jobConfigSource);
    }

    private String quartzKeyToKeyString(org.quartz.utils.Key<?> key) {
        if (statusIndexIdPrefix != null) {
            return escapePeriod(statusIndexIdPrefix) + "." + escapePeriod(key.getGroup()) + "." + escapePeriod(key.getName());
        } else {
            return escapePeriod(key.getGroup()) + "." + escapePeriod(key.getName());
        }
    }

    private String escapePeriod(String string) {
        if (string == null) {
            return null;
        }

        if (!needsEscapePeriod(string)) {
            return string;
        }

        int length = string.length();

        StringBuilder result = new StringBuilder(length * 2);

        for (int i = 0; i < length; i++) {
            char c = string.charAt(i);

            if (c == '.') {
                result.append("\\.");
            } else if (c == '\\') {
                result.append("\\\\");
            } else {
                result.append(c);
            }
        }

        return result.toString();

    }

    private boolean needsEscapePeriod(String string) {
        int length = string.length();

        for (int i = 0; i < length; i++) {
            char c = string.charAt(i);

            if (c == '.' || c == '\\') {
                return true;
            }
        }

        return false;
    }

    private boolean checkForMisfire(InternalOperableTrigger internalOperableTrigger) {
        return checkForMisfire(internalOperableTrigger, System.currentTimeMillis() - misfireThreshold);
    }

    private boolean checkForMisfire(InternalOperableTrigger internalOperableTrigger, long isMisfireBefore) {

        Date nextFireTime = internalOperableTrigger.getNextFireTime();

        if (nextFireTime == null || nextFireTime.getTime() > isMisfireBefore
                || internalOperableTrigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
            return false;
        }

        Calendar calendar = null;
        if (internalOperableTrigger.getCalendarName() != null) {
            calendar = retrieveCalendar(internalOperableTrigger.getCalendarName());
        }

        signaler.notifyTriggerListenersMisfired((OperableTrigger) internalOperableTrigger.getDelegate().clone());

        internalOperableTrigger.updateAfterMisfire(calendar);

        this.markDirty(internalOperableTrigger);

        if (internalOperableTrigger.getNextFireTime() == null) {
            synchronized (this) {
                internalOperableTrigger.state = InternalOperableTrigger.State.COMPLETE;
                this.activeTriggers.remove(internalOperableTrigger);
            }
            signaler.notifySchedulerListenersFinalized(internalOperableTrigger);
            return true;

        } else if (nextFireTime.equals(internalOperableTrigger.getNextFireTime())) {
            return false;
        } else {
            return true;
        }
    }

    private void markDirty(InternalOperableTrigger trigger) {
        this.dirtyTriggers.get().add(trigger);
    }

    private void flushDirtyTriggersToIndex() {
        Set<InternalOperableTrigger> triggers = new HashSet<>(this.dirtyTriggers.get());
        this.dirtyTriggers.get().clear();

        if (log.isDebugEnabled()) {
            log.debug("Flushing dirty triggers: " + triggers);
        }

        for (OperableTrigger trigger : triggers) {

            setTriggerStatusInIndex((InternalOperableTrigger) trigger);

        }
    }

    static class InternalJobDetail implements JobDetail, JobDetailWithBaseConfig {

        private static final long serialVersionUID = -4500332272991179774L;

        private JobDetail delegate;
        private final IndexJobStateStore<?> jobStore;
        private List<InternalOperableTrigger> triggers = new ArrayList<>();
        private JobConfig baseConfig;

        InternalJobDetail(JobDetail jobDetail, JobConfig baseConfig, IndexJobStateStore<?> jobStore) {
            this.delegate = jobDetail;
            this.baseConfig = baseConfig;
            this.jobStore = jobStore;
        }

        public void addTrigger(InternalOperableTrigger trigger) {
            this.triggers.add(trigger);
            trigger.setJobDetail(this);
        }

        public JobKey getKey() {
            return delegate.getKey();
        }

        public String getDescription() {
            return delegate.getDescription();
        }

        public Class<? extends Job> getJobClass() {
            return delegate.getJobClass();
        }

        public JobDataMap getJobDataMap() {
            return delegate.getJobDataMap();
        }

        public boolean isDurable() {
            return delegate.isDurable();
        }

        public boolean isPersistJobDataAfterExecution() {
            return delegate.isPersistJobDataAfterExecution();
        }

        public boolean isConcurrentExectionDisallowed() {
            return delegate.isConcurrentExectionDisallowed();
        }

        public boolean requestsRecovery() {
            return delegate.requestsRecovery();
        }

        public JobBuilder getJobBuilder() {
            return delegate.getJobBuilder();
        }

        @Override
        public JobConfig getBaseConfig() {
            return this.baseConfig;
        }

        @Override
        public <T> T getBaseConfig(Class<T> type) {
            return type.cast(baseConfig);
        }

        @Override
        public Object clone() {
            return new InternalJobDetail(this.delegate, this.baseConfig, this.jobStore);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            JobKey key = getKey();
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof JobDetail))
                return false;
            JobDetail other = (JobDetail) obj;
            JobKey key = getKey();

            if (key == null) {
                if (other.getKey() != null)
                    return false;
            } else if (!key.equals(other.getKey()))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "InternalJobDetail [key=" + getKey() + ", class=" + getJobClass() + ", jobDataMap=" + new HashMap<>(getJobDataMap())
                    + ", triggers=" + triggers + "]";
        }

        void blockIdleTriggers() {
            for (InternalOperableTrigger trigger : triggers) {
                if (trigger.state == InternalOperableTrigger.State.WAITING) {
                    trigger.setState(InternalOperableTrigger.State.BLOCKED);
                } else if (trigger.state == InternalOperableTrigger.State.PAUSED) {
                    trigger.setState(InternalOperableTrigger.State.PAUSED_BLOCKED);
                }

                this.jobStore.activeTriggers.remove(trigger);
            }

            this.jobStore.blockedJobs.add(getKey());
        }

        void deblockTriggers() {
            this.jobStore.blockedJobs.remove(getKey());

            for (InternalOperableTrigger trigger : triggers) {
                if (trigger.state == InternalOperableTrigger.State.BLOCKED) {
                    trigger.setState(InternalOperableTrigger.State.WAITING);
                    this.jobStore.activeTriggers.add(trigger);
                } else if (trigger.state == InternalOperableTrigger.State.PAUSED_BLOCKED) {
                    trigger.setState(InternalOperableTrigger.State.PAUSED);
                }
            }
        }

        void engageTriggers() {
            for (InternalOperableTrigger trigger : this.triggers) {
                if (trigger.state == InternalOperableTrigger.State.WAITING) {
                    this.jobStore.activeTriggers.add(trigger);
                } else {
                    this.jobStore.activeTriggers.remove(trigger);
                }
            }
        }

        Map<TriggerKey, InternalOperableTrigger> getTriggersAsMap() {
            return triggers.stream().collect(Collectors.toMap(Trigger::getKey, Function.identity()));
        }

    }

    static class InternalOperableTrigger implements OperableTrigger, ToXContentObject {
        private static final long serialVersionUID = -181071146931763579L;
        private OperableTrigger delegate;
        private final TriggerKey key;
        private final String keyString;
        private final IndexJobStateStore<?> jobStore;
        private State state = State.WAITING;
        private String stateInfo = null;
        private String node;
        private Date previousFireTime;
        private Date nextFireTime;
        private Integer timesTriggered;
        private InternalJobDetail jobDetail;

        InternalOperableTrigger(IndexJobStateStore<?> jobStore, TriggerKey key) {
            this.key = java.util.Objects.requireNonNull(key, "TriggerKey must not be null");
            this.keyString = jobStore.quartzKeyToKeyString(key);
            this.jobStore = jobStore;
        }

        InternalOperableTrigger(IndexJobStateStore<?> jobStore, OperableTrigger operableTrigger) {
            this(jobStore, operableTrigger.getKey());
            this.delegate = operableTrigger;
        }

        public void triggered(Calendar calendar) {
            delegate.triggered(calendar);
        }

        public Date computeFirstFireTime(Calendar calendar) {
            return delegate.computeFirstFireTime(calendar);
        }

        public void setKey(TriggerKey key) {
            delegate.setKey(key);
        }

        public void setJobKey(JobKey key) {
            delegate.setJobKey(key);
        }

        public void setDescription(String description) {
            delegate.setDescription(description);
        }

        public void setCalendarName(String calendarName) {
            delegate.setCalendarName(calendarName);
        }

        public CompletedExecutionInstruction executionComplete(JobExecutionContext context, JobExecutionException result) {
            return delegate.executionComplete(context, result);
        }

        public void setJobDataMap(JobDataMap jobDataMap) {
            delegate.setJobDataMap(jobDataMap);
        }

        public void setPriority(int priority) {
            delegate.setPriority(priority);
        }

        public void setStartTime(Date startTime) {
            delegate.setStartTime(startTime);
        }

        public void updateAfterMisfire(Calendar cal) {
            delegate.updateAfterMisfire(cal);
        }

        public void setEndTime(Date endTime) {
            delegate.setEndTime(endTime);
        }

        public void updateWithNewCalendar(Calendar cal, long misfireThreshold) {
            delegate.updateWithNewCalendar(cal, misfireThreshold);
        }

        public void setMisfireInstruction(int misfireInstruction) {
            delegate.setMisfireInstruction(misfireInstruction);
        }

        public void validate() throws SchedulerException {
            delegate.validate();
        }

        public Object clone() {
            return new InternalOperableTrigger(jobStore, delegate);
        }

        public void setFireInstanceId(String id) {
            delegate.setFireInstanceId(id);
        }

        public String getFireInstanceId() {
            return delegate.getFireInstanceId();
        }

        public void setNextFireTime(Date nextFireTime) {
            this.nextFireTime = nextFireTime;

            if (delegate != null) {
                delegate.setNextFireTime(nextFireTime);
            }
        }

        public void setPreviousFireTime(Date previousFireTime) {
            this.previousFireTime = previousFireTime;

            if (delegate != null) {
                delegate.setPreviousFireTime(previousFireTime);
            }
        }

        public TriggerKey getKey() {
            if(delegate == null) {
                return key;
            }
            return delegate.getKey();
        }

        public JobKey getJobKey() {
            return delegate.getJobKey();
        }

        public String getDescription() {
            return delegate.getDescription();
        }

        public String getCalendarName() {
            return delegate.getCalendarName();
        }

        public JobDataMap getJobDataMap() {
            return delegate.getJobDataMap();
        }

        public int getPriority() {
            return delegate.getPriority();
        }

        public boolean mayFireAgain() {
            return delegate.mayFireAgain();
        }

        public Date getStartTime() {
            return delegate.getStartTime();
        }

        public Date getEndTime() {
            return delegate.getEndTime();
        }

        public Date getNextFireTime() {
            if (delegate != null) {
                return delegate.getNextFireTime();
            } else {
                return nextFireTime;
            }
        }

        public Date getPreviousFireTime() {
            if (delegate != null) {
                return delegate.getPreviousFireTime();
            } else {
                return previousFireTime;
            }
        }

        public Date getFireTimeAfter(Date afterTime) {
            return delegate.getFireTimeAfter(afterTime);
        }

        public Date getFinalFireTime() {
            return delegate.getFinalFireTime();
        }

        public int getMisfireInstruction() {
            return delegate.getMisfireInstruction();
        }

        public TriggerBuilder<? extends Trigger> getTriggerBuilder() {
            return delegate.getTriggerBuilder();
        }

        public ScheduleBuilder<? extends Trigger> getScheduleBuilder() {
            return delegate.getScheduleBuilder();
        }

        public boolean equals(Object other) {
            return delegate.equals(other);
        }

        public int compareTo(Trigger other) {
            return delegate.compareTo(other);
        }

        public String getKeyString() {
            return keyString;
        }

        public OperableTrigger getDelegate() {
            return delegate;
        }

        public void setDelegate(OperableTrigger delegate) {
            this.delegate = delegate;

            if (delegate != null) {
                delegate.setPreviousFireTime(this.previousFireTime);
                delegate.setNextFireTime(this.nextFireTime);

                this.setTimesTriggeredInDelegate(this.delegate, this.timesTriggered);
            }
        }

        public State getState() {
            return state;
        }

        public void setState(State state) {
            if (this.state == state) {
                return;
            }

            this.state = state;
            markDirty();
        }

        public void setStateWithoutMarkingDirty(State state) {
            if (this.state == state) {
                return;
            }

            this.state = state;
        }

        public void setStateAndNode(State state, String nodeId) {
            if (this.state == state && Objects.equal(this.node, nodeId)) {
                return;
            }

            this.state = state;
            this.node = nodeId;
            markDirty();
        }

        public String toString() {
            return key + " " + state + " " + this.getPreviousFireTime() + " <-> " + this.getNextFireTime();
        }

        void markDirty() {
            if (this.jobDetail != null) {
                this.jobDetail.jobStore.markDirty(this);
            }
        }

        static enum State {

            WAITING(TriggerState.NORMAL), ACQUIRED(TriggerState.NORMAL), EXECUTING(TriggerState.NORMAL), COMPLETE(TriggerState.COMPLETE),
            BLOCKED(TriggerState.BLOCKED), ERROR(TriggerState.ERROR), PAUSED(TriggerState.PAUSED), PAUSED_BLOCKED(TriggerState.PAUSED),
            DELETED(TriggerState.NORMAL);

            private final TriggerState triggerState;

            State(TriggerState triggerState) {
                this.triggerState = triggerState;
            }

            public TriggerState getTriggerState() {
                return triggerState;
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("state", this.state.toString());
            builder.field("nextFireTime", getNextFireTime() != null ? getNextFireTime().getTime() : null);
            builder.field("prevFireTime", getPreviousFireTime() != null ? getPreviousFireTime().getTime() : null);
            builder.field("info", this.stateInfo);
            builder.field("node", this.node);
            builder.field("timesTriggered", this.getTimesTriggered());
            builder.endObject();
            return builder;
        }

        public static InternalOperableTrigger fromAttributeMap(IndexJobStateStore<?> jobStore, TriggerKey triggerKey,
                Map<String, Object> attributeMap) {
            InternalOperableTrigger result = new InternalOperableTrigger(jobStore, triggerKey);

            try {
                result.state = State.valueOf((String) attributeMap.get("state"));
                result.node = (String) attributeMap.get("node");
                result.setNextFireTime(toDate(attributeMap.get("nextFireTime")));
                result.setPreviousFireTime(toDate(attributeMap.get("prevFireTime")));
                result.stateInfo = (String) attributeMap.get("info");
                result.setTimesTriggered(
                        attributeMap.get("timesTriggered") instanceof Number ? ((Number) attributeMap.get("timesTriggered")).intValue() : null);

            } catch (Exception e) {
                log.error("Error while parsing trigger " + triggerKey, e);
                result.state = State.ERROR;
                result.stateInfo = "Error while parsing " + e;
            }

            return result;
        }

        private static Date toDate(Object time) {
            if (time instanceof Number) {
                return new Date(((Number) time).longValue());
            } else {
                return null;
            }
        }

        public String getStateInfo() {
            return stateInfo;
        }

        public void setStateInfo(String stateInfo) {
            this.stateInfo = stateInfo;
        }

        public String getNode() {
            return node;
        }

        public void setNode(String node) {
            this.node = node;
        }

        public Integer getTimesTriggered() {
            if (delegate instanceof DailyTimeIntervalTrigger) {
                return ((DailyTimeIntervalTrigger) delegate).getTimesTriggered();
            } else if (delegate instanceof SimpleTrigger) {
                return ((SimpleTrigger) delegate).getTimesTriggered();
            } else {
                return this.timesTriggered;
            }
        }

        public void setTimesTriggered(Integer timesTriggered) {
            this.timesTriggered = timesTriggered;

            setTimesTriggeredInDelegate(delegate, timesTriggered);
        }

        public void setTimesTriggeredInDelegate(OperableTrigger delegate, Integer timesTriggered) {
            if (delegate instanceof CalendarIntervalTriggerImpl) {
                ((CalendarIntervalTriggerImpl) delegate).setTimesTriggered(timesTriggered != null ? timesTriggered : 0);
            } else if (delegate instanceof DailyTimeIntervalTriggerImpl) {
                ((DailyTimeIntervalTriggerImpl) delegate).setTimesTriggered(timesTriggered != null ? timesTriggered : 0);
            } else if (delegate instanceof SimpleTriggerImpl) {
                ((SimpleTriggerImpl) delegate).setTimesTriggered(timesTriggered != null ? timesTriggered : 0);
            }
        }

        public InternalJobDetail getJobDetail() {
            return jobDetail;
        }

        public void setJobDetail(InternalJobDetail jobDetail) {
            this.jobDetail = jobDetail;
        }

    }

    static class ActiveTriggerQueue {
        private final TreeSet<ActiveTrigger> queue = new TreeSet<ActiveTrigger>(ActiveTrigger.COMPARATOR);
        private final Map<TriggerKey, ActiveTrigger> keyToActiveTriggerMap = new HashMap<>();

        void add(InternalOperableTrigger trigger) {
            ActiveTrigger activeTrigger = new ActiveTrigger(trigger);

            ActiveTrigger oldActiveTrigger = keyToActiveTriggerMap.put(trigger.getKey(), activeTrigger);

            if (oldActiveTrigger != null) {
                queue.remove(oldActiveTrigger);
            }

            queue.add(activeTrigger);
        }

        void addAll(Collection<InternalOperableTrigger> triggers) {
            for (InternalOperableTrigger trigger : triggers) {
                add(trigger);
            }
        }

        void remove(InternalOperableTrigger trigger) {

            ActiveTrigger oldActiveTrigger = keyToActiveTriggerMap.remove(trigger.getKey());

            if (oldActiveTrigger != null) {
                queue.remove(oldActiveTrigger);
            }
        }

        boolean isEmpty() {
            return this.queue.isEmpty();
        }

        int size() {
            return this.queue.size();
        }

        ActiveTrigger pollFirst() {
            ActiveTrigger result = queue.pollFirst();

            if (result != null) {
                keyToActiveTriggerMap.remove(result.getKey());
            }

            return result;
        }

        void clear() {
            queue.clear();
            keyToActiveTriggerMap.clear();
        }

        @Override
        public String toString() {
            return this.queue.toString();
        }
    }

    static class ActiveTrigger implements Trigger {

        private static final long serialVersionUID = -4666180063413542273L;

        final static TriggerTimeComparator COMPARATOR = new TriggerTimeComparator();

        private final InternalOperableTrigger delegate;
        private final boolean mayFireAgain;
        private final Date startTime;
        private final Date endTime;
        private final Date nextFireTime;
        private final Date previousFireTime;
        private final Date finalFireTime;

        ActiveTrigger(InternalOperableTrigger delegate) {
            this.delegate = delegate;
            this.mayFireAgain = delegate.mayFireAgain();
            this.startTime = copyDate(delegate.getStartTime());
            this.endTime = copyDate(delegate.getEndTime());
            this.nextFireTime = copyDate(delegate.getNextFireTime());
            this.previousFireTime = copyDate(delegate.getPreviousFireTime());
            this.finalFireTime = copyDate(delegate.getFinalFireTime());
        }

        @Override
        public TriggerKey getKey() {
            return this.delegate.getKey();
        }

        @Override
        public JobKey getJobKey() {
            return this.delegate.getJobKey();
        }

        @Override
        public String getDescription() {
            return this.delegate.getDescription();
        }

        @Override
        public String getCalendarName() {
            return this.delegate.getCalendarName();

        }

        @Override
        public JobDataMap getJobDataMap() {
            return this.delegate.getJobDataMap();

        }

        @Override
        public int getPriority() {
            return this.delegate.getPriority();
        }

        @Override
        public boolean mayFireAgain() {
            return this.mayFireAgain;
        }

        @Override
        public Date getStartTime() {
            return this.startTime;
        }

        @Override
        public Date getEndTime() {
            return this.endTime;
        }

        @Override
        public Date getNextFireTime() {
            return this.nextFireTime;
        }

        @Override
        public Date getPreviousFireTime() {
            return this.previousFireTime;
        }

        @Override
        public Date getFireTimeAfter(Date afterTime) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date getFinalFireTime() {
            return this.finalFireTime;
        }

        @Override
        public int getMisfireInstruction() {
            return this.delegate.getMisfireInstruction();
        }

        @Override
        public TriggerBuilder<? extends Trigger> getTriggerBuilder() {
            return this.delegate.getTriggerBuilder();
        }

        @Override
        public ScheduleBuilder<? extends Trigger> getScheduleBuilder() {
            return this.delegate.getScheduleBuilder();
        }

        @Override
        public int compareTo(Trigger other) {
            return COMPARATOR.compare(this, other);
        }

        @Override
        public String toString() {
            return this.delegate.getKey().toString();
        }

        private static Date copyDate(Date date) {
            if (date != null) {
                return new Date(date.getTime());
            } else {
                return null;
            }
        }

    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public String toString() {
        return "IndexJobStateStore [schedulerName=" + schedulerName + ", statusIndexName=" + statusIndexName + ", jobConfigSource=" + jobConfigSource
                + ", jobFactory=" + jobFactory + "]";
    }
}
