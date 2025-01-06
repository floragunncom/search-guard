package com.floragunn.aim.scheduler.store;

import com.floragunn.aim.api.internal.InternalSchedulerAPI;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface TriggerStore<JobConfigType extends JobConfig> {
    void initialize(SchedulerSignaler signaler, Client client, String node, String schedulerName, ScheduledExecutorService maintenanceExecutor)
            throws SchedulerConfigException;

    void shutdown();

    void load(Map<JobKey, InternalJobDetail<JobConfigType>> jobs);

    InternalOperableTrigger add(OperableTrigger newTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException;

    void addAll(Set<? extends Trigger> newTriggers, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException;

    boolean contains(TriggerKey triggerKey) throws JobPersistenceException;

    InternalOperableTrigger get(TriggerKey triggerKey) throws JobPersistenceException;

    List<InternalOperableTrigger> getAll(JobKey jobKey) throws JobPersistenceException;

    InternalOperableTrigger remove(TriggerKey triggerKey) throws JobPersistenceException;

    List<InternalOperableTrigger> removeAll(JobKey jobKey) throws JobPersistenceException;

    boolean replace(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException;

    void resetFromErrorState(TriggerKey triggerKey) throws JobPersistenceException;

    int size() throws JobPersistenceException;

    Set<TriggerKey> keySet() throws JobPersistenceException;

    void clear() throws JobPersistenceException;

    boolean pause(TriggerKey triggerKey) throws JobPersistenceException;

    void pauseAll(Collection<TriggerKey> triggerKeys) throws JobPersistenceException;

    void pauseAll(JobKey jobKey) throws JobPersistenceException;

    void resume(TriggerKey triggerKey) throws JobPersistenceException;

    void resumeAll(Collection<TriggerKey> triggerKeys) throws JobPersistenceException;

    void resumeAll(JobKey jobKey) throws JobPersistenceException;

    List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException;

    void releaseAcquiredTrigger(TriggerKey triggerKey);

    List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers, Map<JobKey, JobDetail> jobDetails) throws JobPersistenceException;

    void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, Trigger.CompletedExecutionInstruction triggerInstCode);

    Set<String> getPausedGroups() throws JobPersistenceException;

    class HeapIndexTriggerStore<JobConfigType extends JobConfig> implements TriggerStore<JobConfigType> {
        private static final Logger LOG = LogManager.getLogger(HeapIndexTriggerStore.class);
        private static final long MISFIRE_THRESHOLD = 10000;

        private final String index;

        private final Map<TriggerKey, InternalOperableTrigger> keyToTriggerMap;
        private final ActiveTriggerQueue activeTriggerQueue;
        private final Set<JobKey> blockedJobs;
        private final Set<TriggerKey> pausedTriggers;
        private final Set<InternalOperableTrigger> dirty;
        private final Set<InternalOperableTrigger> stillExecutingOnOtherNode;
        private final ExecutorService stateUpdateExecutor;

        private Client client;
        private String node;
        private String schedulerName;
        private SchedulerSignaler signaler;
        private ScheduledExecutorService maintenanceExecutor;

        public HeapIndexTriggerStore(String index) {
            this.index = index;

            keyToTriggerMap = new HashMap<>();
            activeTriggerQueue = new ActiveTriggerQueue();
            blockedJobs = new HashSet<>();
            pausedTriggers = new HashSet<>();
            dirty = new HashSet<>();
            stillExecutingOnOtherNode = Collections.synchronizedSet(new HashSet<>());
            stateUpdateExecutor = Executors.newSingleThreadExecutor();
        }

        @Override
        public void initialize(SchedulerSignaler signaler, Client client, String node, String schedulerName,
                ScheduledExecutorService maintenanceExecutor) throws SchedulerConfigException {
            this.client = client;
            this.node = node;
            this.schedulerName = schedulerName;
            this.signaler = signaler;
            this.maintenanceExecutor = maintenanceExecutor;
        }

        @Override
        public void shutdown() {
            stateUpdateExecutor.shutdown();
            try {
                if (stateUpdateExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                    stateUpdateExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                stateUpdateExecutor.shutdownNow();
            }
        }

        @Override
        public void load(Map<JobKey, InternalJobDetail<JobConfigType>> jobs) {
            long startTime = System.currentTimeMillis();
            Map<String, Trigger> triggerIdToTrigger = new HashMap<>(jobs.size());
            for (InternalJobDetail<JobConfigType> jobDetail : jobs.values()) {
                Collection<Trigger> triggers = jobDetail.getJobConfig().getTriggers();
                triggerIdToTrigger.putAll(triggers.stream().collect(Collectors.toMap(trigger -> trigger.getKey().toString(), Function.identity())));
                LOG.trace("Loading triggers {}", triggers);
            }
            if (triggerIdToTrigger.isEmpty()) {
                return;
            }
            try {
                MultiGetResponse response = client.prepareMultiGet().addIds(index, triggerIdToTrigger.keySet()).get();
                //activeTriggerQueue.clear();
                for (MultiGetItemResponse item : response) {
                    if (item.isFailed()) {
                        LOG.error("Failed to load trigger {}", item.getId(), item.getFailure().getFailure());
                        continue;
                    }

                    OperableTrigger operableTrigger = (OperableTrigger) triggerIdToTrigger.get(item.getId());
                    InternalOperableTrigger internalOperableTrigger;
                    if (item.getResponse().isExists()) {
                        try {
                            DocNode triggerState = DocNode.parse(Format.JSON).from(item.getResponse().getSourceAsBytesRef().utf8ToString());
                            internalOperableTrigger = InternalOperableTrigger.from(operableTrigger, triggerState);
                            if (internalOperableTrigger.isExecutingOnOtherNodeAfterRecovery(node, blockedJobs)) {
                                stillExecutingOnOtherNode.add(internalOperableTrigger);
                            }
                        } catch (ConfigValidationException e) {
                            LOG.warn("Trigger state was invalid", e);
                            internalOperableTrigger = InternalOperableTrigger.from(node, operableTrigger);
                        }
                    } else {
                        internalOperableTrigger = InternalOperableTrigger.from(node, operableTrigger);
                        internalOperableTrigger.computeFirstFireTime(null);
                    }
                    keyToTriggerMap.put(internalOperableTrigger.getKey(), internalOperableTrigger);
                    if (internalOperableTrigger.getState() == InternalOperableTrigger.State.WAITING) {
                        activeTriggerQueue.add(internalOperableTrigger);
                    } else if (internalOperableTrigger.getState() == InternalOperableTrigger.State.PAUSED
                            || internalOperableTrigger.getState() == InternalOperableTrigger.State.PAUSED_BLOCKED) {
                        pausedTriggers.add(internalOperableTrigger.getKey());
                    }
                    markDirty(internalOperableTrigger);
                }
                if (!stillExecutingOnOtherNode.isEmpty()) {
                    maintenanceExecutor.schedule(() -> checkTriggersExecutingOnOtherNode(jobs), 10, TimeUnit.SECONDS);
                }
                flushDirty();
            } catch (Exception e) {
                LOG.error("Loading triggers failed after {}ms", System.currentTimeMillis() - startTime, e);
            }
        }

        @Override
        public InternalOperableTrigger add(OperableTrigger newTrigger, boolean replaceExisting)
                throws ObjectAlreadyExistsException, JobPersistenceException {
            if (!replaceExisting && contains(newTrigger.getKey())) {
                throw new ObjectAlreadyExistsException(newTrigger);
            }
            InternalOperableTrigger trigger = InternalOperableTrigger.from(node, newTrigger);
            prepareAdd(trigger);
            keyToTriggerMap.put(trigger.getKey(), trigger);
            flushDirty();
            return trigger;
        }

        @Override
        public void addAll(Set<? extends Trigger> newTriggers, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
            Map<TriggerKey, InternalOperableTrigger> triggers = new HashMap<>(newTriggers.size());
            for (Trigger newTrigger : newTriggers) {
                if (!replaceExisting && contains(newTrigger.getKey())) {
                    throw new ObjectAlreadyExistsException(newTrigger);
                }
                InternalOperableTrigger trigger = InternalOperableTrigger.from(node, (OperableTrigger) newTrigger);
                triggers.put(trigger.getKey(), trigger);
                prepareAdd(trigger);
            }
            keyToTriggerMap.putAll(triggers);
            flushDirty();
        }

        @Override
        public boolean contains(TriggerKey triggerKey) {
            return keyToTriggerMap.containsKey(triggerKey);
        }

        @Override
        public InternalOperableTrigger get(TriggerKey triggerKey) {
            return keyToTriggerMap.get(triggerKey);
        }

        @Override
        public List<InternalOperableTrigger> getAll(JobKey jobKey) {
            return keyToTriggerMap.values().stream().filter(internalOperableTrigger -> jobKey.equals(internalOperableTrigger.getJobKey())).toList();
        }

        @Override
        public InternalOperableTrigger remove(TriggerKey triggerKey) throws JobPersistenceException {
            LOG.trace("Removing active trigger '{}'", triggerKey);
            return keyToTriggerMap.remove(triggerKey);
        }

        @Override
        public List<InternalOperableTrigger> removeAll(JobKey jobKey) throws JobPersistenceException {
            List<InternalOperableTrigger> result = new ArrayList<>();
            for (TriggerKey key : keyToTriggerMap.entrySet().stream().filter(entry -> jobKey.equals(entry.getValue().getJobKey()))
                    .map(Map.Entry::getKey).toList()) {
                InternalOperableTrigger trigger = remove(key);
                if (trigger != null) {
                    result.add(trigger);
                }
            }
            return result;
        }

        @Override
        public boolean replace(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
            InternalOperableTrigger trigger = get(triggerKey);
            if (trigger == null) {
                return false;
            }
            add(newTrigger, true);
            return true;
        }

        @Override
        public void resetFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
            InternalOperableTrigger trigger = get(triggerKey);
            if (trigger == null || !InternalOperableTrigger.State.ERROR.equals(trigger.getState())) {
                return;
            }
            if (trigger.isNotBlockedAfterUpdateToIdle(blockedJobs)) {
                activeTriggerQueue.add(trigger);
            } else {
                activeTriggerQueue.remove(triggerKey);
            }
            markDirty(trigger);
            flushDirty();
        }

        @Override
        public int size() throws JobPersistenceException {
            return keyToTriggerMap.size();
        }

        @Override
        public Set<TriggerKey> keySet() throws JobPersistenceException {
            return keyToTriggerMap.keySet();
        }

        @Override
        public void clear() throws JobPersistenceException {
            keyToTriggerMap.clear();
            activeTriggerQueue.clear();
        }

        @Override
        public boolean pause(TriggerKey triggerKey) throws JobPersistenceException {
            InternalOperableTrigger trigger = get(triggerKey);
            if (trigger == null) {
                return false;
            }
            if (trigger.pause()) {
                pausedTriggers.add(triggerKey);
                activeTriggerQueue.remove(triggerKey);
                markDirty(trigger);
                flushDirty();
                return true;
            }
            return false;
        }

        @Override
        public void pauseAll(Collection<TriggerKey> triggerKeys) throws JobPersistenceException {
            for (TriggerKey triggerKey : triggerKeys) {
                InternalOperableTrigger trigger = get(triggerKey);
                if (trigger != null && trigger.pause()) {
                    pausedTriggers.add(triggerKey);
                    activeTriggerQueue.remove(triggerKey);
                    markDirty(trigger);
                }
            }
        }

        @Override
        public void pauseAll(JobKey jobKey) throws JobPersistenceException {
            for (InternalOperableTrigger trigger : getAll(jobKey)) {
                if (trigger.pause()) {
                    pausedTriggers.add(trigger.getKey());
                    activeTriggerQueue.remove(trigger.getKey());
                    markDirty(trigger);
                }
            }
            flushDirty();
        }

        @Override
        public void resume(TriggerKey triggerKey) throws JobPersistenceException {
            InternalOperableTrigger trigger = get(triggerKey);
            if (trigger == null || (trigger.getState() != InternalOperableTrigger.State.PAUSED
                    && trigger.getState() != InternalOperableTrigger.State.PAUSED_BLOCKED)) {
                return;
            }
            prepareResume(trigger);
            trigger.resume();
            flushDirty();
        }

        @Override
        public void resumeAll(Collection<TriggerKey> triggerKeys) throws JobPersistenceException {
            for (TriggerKey triggerKey : triggerKeys) {
                InternalOperableTrigger trigger = get(triggerKey);
                if (trigger == null || (trigger.getState() != InternalOperableTrigger.State.PAUSED
                        && trigger.getState() != InternalOperableTrigger.State.PAUSED_BLOCKED)) {
                    continue;
                }
                prepareResume(trigger);
                trigger.resume();
            }
            flushDirty();
        }

        @Override
        public void resumeAll(JobKey jobKey) throws JobPersistenceException {
            for (InternalOperableTrigger trigger : getAll(jobKey)) {
                if (trigger.getState() != InternalOperableTrigger.State.PAUSED
                        && trigger.getState() != InternalOperableTrigger.State.PAUSED_BLOCKED) {
                    continue;
                }
                prepareResume(trigger);
                trigger.resume();
            }
            flushDirty();
        }

        @Override
        public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException {
            LOG.debug("Number of active triggers: {}", activeTriggerQueue.size());
            if (activeTriggerQueue.isEmpty()) {
                return List.of();
            }
            LOG.debug("Active triggers: {}", activeTriggerQueue);
            List<OperableTrigger> result = new ArrayList<>(Math.min(maxCount, activeTriggerQueue.size()));
            long misfireIsBefore = System.currentTimeMillis() - MISFIRE_THRESHOLD;
            long batchEnd = noLaterThan;

            for (ActiveTriggerQueue.ActiveTrigger activeTrigger = activeTriggerQueue
                    .pollFirst(); activeTrigger != null; activeTrigger = activeTriggerQueue.pollFirst()) {
                if (activeTrigger.getNextFireTime() == null) {
                    LOG.trace("Next fire time is null for trigger: {}", activeTrigger);
                    continue;
                }

                InternalOperableTrigger trigger = activeTrigger.getDelegate();
                if (hasMisfire(trigger, misfireIsBefore)) {
                    LOG.trace("Trigger had a misfire: {}", activeTrigger);
                    if (trigger.getNextFireTime() != null) {
                        activeTriggerQueue.add(trigger);
                    }
                    markDirty(trigger);
                    continue;
                }

                if (activeTrigger.getNextFireTime().getTime() > batchEnd) {
                    LOG.trace("Next fire time for trigger {} is at {}; after batch end. Ignoring for now", activeTrigger, trigger.getNextFireTime());
                    activeTriggerQueue.add(trigger);
                    break;
                }
                trigger.setState(InternalOperableTrigger.State.ACQUIRED);
                trigger.setNode(node);
                trigger.setFireInstanceId(UUID.randomUUID().toString());

                if (result.isEmpty()) {
                    batchEnd = timeWindow + Math.max(activeTrigger.getNextFireTime().getTime(), System.currentTimeMillis());
                }
                result.add(trigger);
                if (result.size() >= maxCount) {
                    break;
                }
            }
            flushDirty();
            LOG.debug("Acquiring result: {}", result.stream().map(operableTrigger -> operableTrigger.getKey().toString()).toList());
            return result;
        }

        @Override
        public void releaseAcquiredTrigger(TriggerKey triggerKey) {
            InternalOperableTrigger trigger = get(triggerKey);
            if (trigger == null || InternalOperableTrigger.State.ACQUIRED != trigger.getState()) {
                return;
            }
            LOG.trace("Releasing acquired trigger: {}", triggerKey);
            trigger.setState(InternalOperableTrigger.State.WAITING);
            activeTriggerQueue.add(trigger);
            markDirty(trigger);
            flushDirty();
        }

        @Override
        public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers, Map<JobKey, JobDetail> jobDetails)
                throws JobPersistenceException {
            List<TriggerFiredResult> result = new ArrayList<>(triggers.size());
            for (OperableTrigger operableTrigger : triggers) {
                InternalOperableTrigger trigger = keyToTriggerMap.get(operableTrigger.getKey());
                if (trigger == null || trigger.getState() != InternalOperableTrigger.State.ACQUIRED) {
                    continue;
                }
                JobDetail jobDetail = jobDetails.get(trigger.getJobKey());
                if (jobDetail == null) {
                    LOG.debug(
                            "Could not find job detail for fired trigger: {}; this probably means that the job was deleted after the trigger was acquired. Skipping this trigger",
                            trigger);
                    activeTriggerQueue.remove(trigger.getKey());
                    trigger.setState(InternalOperableTrigger.State.DELETED);
                    markDirty(trigger);
                    continue;
                }
                Date previousFireTime = trigger.getPreviousFireTime();
                Date nextFireTime = trigger.getNextFireTime();

                activeTriggerQueue.remove(trigger.getKey());
                trigger.triggered(null);
                trigger.setState(InternalOperableTrigger.State.EXECUTING);
                trigger.setNode(node);
                markDirty(trigger);

                TriggerFiredBundle triggerFiredBundle = new TriggerFiredBundle(jobDetail, trigger, null, false, new Date(), nextFireTime,
                        previousFireTime, trigger.getNextFireTime());
                if (jobDetail.isConcurrentExectionDisallowed()) {
                    blockIdleTriggers(jobDetail.getKey());
                } else if (trigger.getNextFireTime() != null) {
                    activeTriggerQueue.add(trigger);
                }
                result.add(new TriggerFiredResult(triggerFiredBundle));
            }
            flushDirty();
            return result;
        }

        @Override
        public void triggeredJobComplete(OperableTrigger operableTrigger, JobDetail jobDetail,
                Trigger.CompletedExecutionInstruction triggerInstCode) {
            if (jobDetail.isConcurrentExectionDisallowed()) {
                unblockTriggers(jobDetail.getKey());
                signaler.signalSchedulingChange(0);
            } else {
                blockedJobs.remove(jobDetail.getKey());
            }
            InternalOperableTrigger trigger = keyToTriggerMap.get(operableTrigger.getKey());
            if (trigger != null) {
                switch (triggerInstCode) {
                case SET_TRIGGER_COMPLETE:
                    trigger.setState(InternalOperableTrigger.State.COMPLETE);
                    activeTriggerQueue.remove(trigger.getKey());
                    markDirty(trigger);
                    signaler.signalSchedulingChange(0);
                    break;
                case DELETE_TRIGGER:
                    LOG.trace("Trigger {} for job {} is being deleted: {}", trigger.getKey(), trigger.getJobKey(), trigger);
                    trigger.setState(InternalOperableTrigger.State.DELETED);
                    activeTriggerQueue.remove(trigger.getKey());
                    markDirty(trigger);
                    signaler.signalSchedulingChange(0);
                    break;
                case SET_ALL_JOB_TRIGGERS_COMPLETE:
                    for (InternalOperableTrigger internalOperableTrigger : getAll(jobDetail.getKey())) {
                        internalOperableTrigger.setState(InternalOperableTrigger.State.COMPLETE);
                        activeTriggerQueue.remove(internalOperableTrigger.getKey());
                        markDirty(internalOperableTrigger);
                    }
                    signaler.signalSchedulingChange(0);
                    break;
                case SET_TRIGGER_ERROR:
                    LOG.trace("Trigger {} for job {} is set to error state.", trigger.getKey(), trigger.getJobKey());
                    trigger.setState(InternalOperableTrigger.State.ERROR);
                    activeTriggerQueue.remove(trigger.getKey());
                    markDirty(trigger);
                    break;
                case SET_ALL_JOB_TRIGGERS_ERROR:
                    for (InternalOperableTrigger internalOperableTrigger : getAll(jobDetail.getKey())) {
                        internalOperableTrigger.setState(InternalOperableTrigger.State.ERROR);
                        activeTriggerQueue.remove(internalOperableTrigger.getKey());
                        markDirty(internalOperableTrigger);
                    }
                    signaler.signalSchedulingChange(0);
                    break;
                default:
                    trigger.setState(InternalOperableTrigger.State.WAITING);
                    activeTriggerQueue.add(trigger);
                    break;
                }
                LOG.trace("Trigger '{}' completed with CompletedExecutionInstruction '{}' and State '{}'", trigger.getKey(), triggerInstCode,
                        trigger.getState());
            }
            flushDirty();
        }

        @Override
        public Set<String> getPausedGroups() throws JobPersistenceException {
            return pausedTriggers.stream().map(TriggerKey::getGroup).collect(Collectors.toSet());
        }

        private void prepareAdd(InternalOperableTrigger trigger) {
            trigger.computeFirstFireTime(null);
            if (trigger.isNotBlockedAfterUpdateToIdle(blockedJobs)) {
                activeTriggerQueue.add(trigger);
            }
            markDirty(trigger);
        }

        private void prepareResume(InternalOperableTrigger trigger) {
            trigger.setState(InternalOperableTrigger.State.WAITING);
            hasMisfire(trigger, System.currentTimeMillis() - MISFIRE_THRESHOLD);
            if (trigger.isNotBlockedAfterUpdateToIdle(blockedJobs)) {
                activeTriggerQueue.add(trigger);
            } else {
                activeTriggerQueue.remove(trigger.getKey());
            }
            markDirty(trigger);
        }

        private void blockIdleTriggers(JobKey jobKey) {
            for (InternalOperableTrigger trigger : getAll(jobKey)) {
                if (trigger.getState() == InternalOperableTrigger.State.WAITING) {
                    trigger.setState(InternalOperableTrigger.State.BLOCKED);
                    activeTriggerQueue.remove(trigger.getKey());
                } else if (trigger.getState() == InternalOperableTrigger.State.PAUSED) {
                    trigger.setState(InternalOperableTrigger.State.PAUSED_BLOCKED);
                }
            }
            blockedJobs.add(jobKey);
        }

        private void unblockTriggers(JobKey jobKey) {
            blockedJobs.remove(jobKey);
            for (InternalOperableTrigger trigger : getAll(jobKey)) {
                if (trigger.getState() == InternalOperableTrigger.State.BLOCKED) {
                    trigger.setState(InternalOperableTrigger.State.WAITING);
                    activeTriggerQueue.add(trigger);
                } else if (trigger.getState() == InternalOperableTrigger.State.PAUSED_BLOCKED) {
                    trigger.setState(InternalOperableTrigger.State.PAUSED);
                }
            }
        }

        private boolean hasMisfire(InternalOperableTrigger trigger, long misfireIsBefore) {
            Date nextFireTime = trigger.getNextFireTime();
            if (nextFireTime == null || nextFireTime.getTime() > misfireIsBefore
                    || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
                return false;
            }

            signaler.notifyTriggerListenersMisfired((Trigger) trigger.getDelegate().clone());
            trigger.updateAfterMisfire(null);
            markDirty(trigger);

            if (trigger.getNextFireTime() == null) {
                trigger.setState(InternalOperableTrigger.State.COMPLETE);
                activeTriggerQueue.remove(trigger.getKey());
                signaler.notifySchedulerListenersFinalized(trigger);
                return true;
            }
            return !nextFireTime.equals(trigger.getNextFireTime());
        }

        private void checkTriggersExecutingOnOtherNode(Map<JobKey, InternalJobDetail<JobConfigType>> jobs) {
            if (stillExecutingOnOtherNode.isEmpty()) {
                return;
            }
            Set<InternalOperableTrigger> triggers = new HashSet<>(stillExecutingOnOtherNode.size());
            Iterator<InternalOperableTrigger> it = stillExecutingOnOtherNode.iterator();
            while (it.hasNext()) {
                InternalOperableTrigger trigger = it.next();
                if (trigger.getState() == InternalOperableTrigger.State.EXECUTING) {
                    triggers.add(trigger);
                } else {
                    it.remove();
                }
            }
            if (triggers.isEmpty()) {
                return;
            }
            try {
                InternalSchedulerAPI.CheckExecutingTriggers.Response response = client
                        .execute(InternalSchedulerAPI.CheckExecutingTriggers.INSTANCE, new InternalSchedulerAPI.CheckExecutingTriggers.Request(
                                schedulerName, triggers.stream().map(InternalOperableTrigger::getKey).collect(Collectors.toSet())))
                        .get();
                Set<InternalOperableTrigger> remainingTriggers = new HashSet<>(triggers);
                remainingTriggers.removeIf(internalOperableTrigger -> response.getTriggerKeys().contains(internalOperableTrigger.getKey()));
                LOG.info("Triggers to be reset after execution on other node check: {}", remainingTriggers);
                if (!remainingTriggers.isEmpty()) {
                    resetTriggerFromOtherNode(remainingTriggers, jobs);
                }
                if (!stillExecutingOnOtherNode.isEmpty()) {
                    maintenanceExecutor.schedule(() -> checkTriggersExecutingOnOtherNode(jobs), 10, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                LOG.error("Check for executing triggers failed", e);
            }
        }

        private void resetTriggerFromOtherNode(Set<InternalOperableTrigger> triggers, Map<JobKey, InternalJobDetail<JobConfigType>> jobs) {
            Map<String, InternalOperableTrigger> triggerIdToTrigger = triggers.stream()
                    .collect(Collectors.toMap(internalOperableTrigger -> internalOperableTrigger.getKey().toString(), Function.identity()));
            try {
                MultiGetResponse response = client.prepareMultiGet().addIds(index, triggerIdToTrigger.keySet()).get();
                for (MultiGetItemResponse item : response) {
                    if (item.isFailed()) {
                        LOG.error("Failed to load trigger {}", item.getId(), item.getFailure().getFailure());
                        continue;
                    }
                    InternalOperableTrigger actualTrigger = triggerIdToTrigger.get(item.getId());
                    stillExecutingOnOtherNode.remove(actualTrigger);
                    if (actualTrigger.getState() != InternalOperableTrigger.State.EXECUTING) {
                        continue;
                    }
                    DocNode triggerState = DocNode.parse(Format.JSON).from(item.getResponse().getSourceAsBytesRef().utf8ToString());
                    if (!InternalOperableTrigger.State.EXECUTING.name().equals(triggerState.get(InternalOperableTrigger.STATE_FIELD))) {
                        actualTrigger.refreshConfig(triggerState);
                    } else {
                        if (actualTrigger.getNextFireTime() == null) {
                            actualTrigger.computeFirstFireTime(null);
                        }
                        actualTrigger.isNotBlockedAfterUpdateToIdle(blockedJobs);
                    }
                    actualTrigger.setNode(node);
                    if (actualTrigger.getState() == InternalOperableTrigger.State.WAITING) {
                        InternalJobDetail<JobConfigType> jobDetail = jobs.get(actualTrigger.getJobKey());
                        if (jobDetail != null && jobDetail.isConcurrentExectionDisallowed()) {
                            unblockTriggers(jobDetail.getKey());
                            signaler.signalSchedulingChange(0);
                        }
                        activeTriggerQueue.add(actualTrigger);
                    }
                    markDirty(actualTrigger);
                }
                flushDirty();
            } catch (Exception e) {
                LOG.error("Unable to reset triggers from other nodes", e);
            }
        }

        private synchronized void markDirty(InternalOperableTrigger trigger) {
            dirty.add(trigger);
        }

        private synchronized void markDirty(Collection<InternalOperableTrigger> triggers) {
            dirty.addAll(triggers);
        }

        private synchronized void flushDirty() {
            if (dirty.isEmpty()) {
                return;
            }
            Map<String, InternalOperableTrigger> dirtyTriggers = new HashMap<>(
                    dirty.stream().collect(Collectors.toMap(trigger -> trigger.getKey().toString(), Function.identity())));
            dirty.clear();
            stateUpdateExecutor.submit(() -> {
                try {
                    BulkRequest request = new BulkRequest(index).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    for (Map.Entry<String, InternalOperableTrigger> entry : dirtyTriggers.entrySet()) {
                        if (entry.getValue().getState() == InternalOperableTrigger.State.DELETED) {
                            request.add(new DeleteRequest().id(entry.getKey()));
                        } else {
                            request.add(new IndexRequest().id(entry.getKey()).source(entry.getValue().toDocNode()));
                        }
                    }
                    BulkResponse response = client.bulk(request).get();
                    if (response.hasFailures()) {
                        for (BulkItemResponse item : response.getItems()) {
                            if (item.isFailed()) {
                                LOG.error("Error while writing trigger status: {}", item.getFailure().getMessage());
                                markDirty(dirtyTriggers.get(item.getId()));
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Error while updating trigger status: {}", dirtyTriggers, e);
                    markDirty(dirtyTriggers.values());
                }
            });
        }
    }

    class ActiveTriggerQueue {
        private static final Logger LOG = LogManager.getLogger(ActiveTriggerQueue.class);
        private final TreeSet<ActiveTrigger> queue;
        private final Map<TriggerKey, ActiveTrigger> keyToTriggerMap;

        public ActiveTriggerQueue() {
            queue = new TreeSet<>(new Trigger.TriggerTimeComparator());
            keyToTriggerMap = new HashMap<>();
        }

        @Override
        public synchronized String toString() {
            return queue.toString();
        }

        synchronized void add(InternalOperableTrigger newTrigger) {
            ActiveTrigger trigger = new ActiveTrigger(newTrigger);
            ActiveTrigger existing = keyToTriggerMap.put(trigger.getKey(), trigger);
            if (existing != null) {
                LOG.trace("Replacing trigger {} from active queue", existing);
                queue.remove(existing);
            }
            queue.add(trigger);
        }

        synchronized void addAll(Collection<InternalOperableTrigger> triggers) {
            for (InternalOperableTrigger trigger : triggers) {
                add(trigger);
            }
        }

        synchronized ActiveTrigger remove(TriggerKey triggerKey) {
            LOG.trace("Removing trigger {} from active queue", triggerKey);
            ActiveTrigger trigger = keyToTriggerMap.remove(triggerKey);
            if (trigger != null) {
                queue.remove(trigger);
            }
            return trigger;
        }

        synchronized boolean isEmpty() {
            return queue.isEmpty();
        }

        synchronized int size() {
            return queue.size();
        }

        synchronized void clear() {
            LOG.trace("Clearing triggers {}", queue);
            queue.clear();
            keyToTriggerMap.clear();
        }

        synchronized ActiveTrigger pollFirst() {
            ActiveTrigger trigger = queue.pollFirst();
            if (trigger != null) {
                LOG.trace("Polling trigger {}", trigger.getKey());
                keyToTriggerMap.remove(trigger.getKey());
            }
            return trigger;
        }

        static class ActiveTrigger extends AbstractDelegateTrigger<InternalOperableTrigger> {
            private static final long serialVersionUID = -7663283588734212055L;

            private static final TriggerTimeComparator COMPARATOR = new TriggerTimeComparator();

            private final boolean mayFireAgain;
            private final Date startTime;
            private final Date endTime;
            private final Date nextFireTime;
            private final Date previousFireTime;
            private final Date finalFireTime;

            private static Date cloneDate(Date date) {
                return date != null ? (Date) date.clone() : null;
            }

            public ActiveTrigger(InternalOperableTrigger delegate) {
                super(delegate);
                mayFireAgain = delegate.mayFireAgain();
                startTime = cloneDate(delegate.getStartTime());
                endTime = cloneDate(delegate.getEndTime());
                nextFireTime = cloneDate(delegate.getNextFireTime());
                previousFireTime = cloneDate(delegate.getPreviousFireTime());
                finalFireTime = cloneDate(delegate.getFinalFireTime());
            }

            @Override
            public boolean mayFireAgain() {
                return mayFireAgain;
            }

            @Override
            public Date getStartTime() {
                return startTime;
            }

            @Override
            public Date getEndTime() {
                return endTime;
            }

            @Override
            public Date getNextFireTime() {
                return nextFireTime;
            }

            @Override
            public Date getPreviousFireTime() {
                return previousFireTime;
            }

            @Override
            public Date getFinalFireTime() {
                return finalFireTime;
            }

            @Override
            public int compareTo(Trigger other) {
                return COMPARATOR.compare(this, other);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (!(o instanceof ActiveTrigger)) {
                    return false;
                }
                ActiveTrigger other = (ActiveTrigger) o;
                return mayFireAgain == other.mayFireAgain && Objects.equals(startTime, other.startTime) && Objects.equals(endTime, other.endTime)
                        && Objects.equals(nextFireTime, other.nextFireTime) && Objects.equals(previousFireTime, other.previousFireTime)
                        && Objects.equals(finalFireTime, other.finalFireTime);
            }

            @Override
            public int hashCode() {
                return delegate.getKey().hashCode();
            }

            @Override
            public String toString() {
                return getKey().toString();
            }

            public InternalOperableTrigger getDelegate() {
                return delegate;
            }
        }
    }
}
