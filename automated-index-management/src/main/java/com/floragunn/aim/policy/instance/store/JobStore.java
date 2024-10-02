package com.floragunn.aim.policy.instance.store;

import com.floragunn.searchsupport.jobs.JobConfigListener;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobDetailWithBaseConfig;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.SchedulerSignaler;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public interface JobStore<JobType extends InternalJobDetail> {
    void initialize(SchedulerSignaler signaler, Client client, String node, String schedulerName, ScheduledExecutorService configUpdateExecutor)
            throws SchedulerConfigException;

    void shutdown();

    Map<JobKey, InternalJobDetail> load();

    JobType add(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException;

    boolean contains(JobKey jobKey) throws JobPersistenceException;

    JobType get(JobKey jobKey) throws JobPersistenceException;

    Map<JobKey, JobDetail> getAllAsMap(Collection<JobKey> jobKeys) throws JobPersistenceException;

    boolean remove(JobKey jobKey) throws JobPersistenceException;

    boolean removeAll(List<JobKey> jobKeys) throws JobPersistenceException;

    int size() throws JobPersistenceException;

    Set<JobKey> keySet() throws JobPersistenceException;

    void clear() throws JobPersistenceException;

    void jobComplete(JobDetail jobDetail);

    class HeapJobStore<JobConfigType extends JobConfig> implements JobStore<InternalJobDetail> {
        private static final Logger LOG = LogManager.getLogger(HeapJobStore.class);

        private final JobConfigFactory<JobConfigType> jobConfigFactory;
        private final Iterable<JobConfigType> jobConfigSource;
        private final Collection<JobConfigListener<JobConfigType>> jobConfigListeners;

        private final Map<JobKey, InternalJobDetail> keyToJobMap;

        public HeapJobStore(JobConfigFactory<JobConfigType> jobConfigFactory, Iterable<JobConfigType> jobConfigSource,
                Collection<JobConfigListener<JobConfigType>> jobConfigListeners) {
            this.jobConfigFactory = jobConfigFactory;
            this.jobConfigSource = jobConfigSource;
            this.jobConfigListeners = jobConfigListeners;

            keyToJobMap = new HashMap<>();
        }

        @Override
        public void initialize(SchedulerSignaler signaler, Client client, String node, String schedulerName,
                ScheduledExecutorService configUpdateExecutor) throws SchedulerConfigException {

        }

        @Override
        public void shutdown() {

        }

        @Override
        public Map<JobKey, InternalJobDetail> load() {
            long startTime = System.currentTimeMillis();
            try {
                Set<JobConfigType> jobConfigs = Sets.newHashSet(jobConfigSource);
                for (JobConfigListener<JobConfigType> jobConfigListener : jobConfigListeners) {
                    jobConfigListener.onInit(jobConfigs);
                }
                for (JobConfigType jobConfig : jobConfigs) {
                    InternalJobDetail jobDetail = new InternalJobDetail(jobConfigFactory.createJobDetail(jobConfig), jobConfig);
                    keyToJobMap.put(jobDetail.getKey(), jobDetail);
                }
                return keyToJobMap;
            } catch (Exception e) {
                LOG.error("Loading jobs failed after {}ms", System.currentTimeMillis() - startTime, e);
            }
            return Map.of();
        }

        @Override
        public InternalJobDetail add(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
            if (!replaceExisting && contains(newJob.getKey())) {
                throw new ObjectAlreadyExistsException(newJob);
            }
            InternalJobDetail jobDetail;
            if (newJob instanceof InternalJobDetail) {
                jobDetail = (InternalJobDetail) newJob;
            } else if (newJob instanceof JobDetailWithBaseConfig) {
                jobDetail = new InternalJobDetail(newJob, ((JobDetailWithBaseConfig) newJob).getBaseConfig());
            } else {
                jobDetail = new InternalJobDetail(newJob, null);
            }
            keyToJobMap.put(newJob.getKey(), jobDetail);
            return jobDetail;
        }

        @Override
        public boolean contains(JobKey jobKey) throws JobPersistenceException {
            return keyToJobMap.containsKey(jobKey);
        }

        @Override
        public InternalJobDetail get(JobKey jobKey) throws JobPersistenceException {
            return keyToJobMap.get(jobKey);
        }

        @Override
        public Map<JobKey, JobDetail> getAllAsMap(Collection<JobKey> jobKeys) throws JobPersistenceException {
            return keyToJobMap.entrySet().stream().filter(entry -> jobKeys.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public boolean remove(JobKey jobKey) throws JobPersistenceException {
            return keyToJobMap.remove(jobKey) != null;
        }

        @Override
        public boolean removeAll(List<JobKey> list) throws JobPersistenceException {
            boolean result = true;
            for (JobKey jobKey : list) {
                result &= remove(jobKey);
            }
            return result;
        }

        @Override
        public int size() throws JobPersistenceException {
            return keyToJobMap.size();
        }

        @Override
        public Set<JobKey> keySet() throws JobPersistenceException {
            return keyToJobMap.keySet();
        }

        @Override
        public void clear() {
            keyToJobMap.clear();
        }

        public void jobComplete(JobDetail jobDetail) {
            InternalJobDetail internalJobDetail = keyToJobMap.get(jobDetail.getKey());
            if (internalJobDetail != null) {
                if (internalJobDetail.isPersistJobDataAfterExecution()) {
                    JobDataMap jobDataMap = internalJobDetail.getJobDataMap();
                    if (jobDataMap != null) {
                        jobDataMap = (JobDataMap) jobDataMap.clone();
                        jobDataMap.clearDirtyFlag();
                    }
                    internalJobDetail.setDelegate(internalJobDetail.getJobBuilder().setJobData(jobDataMap).build());
                }
            }
        }
    }
}
