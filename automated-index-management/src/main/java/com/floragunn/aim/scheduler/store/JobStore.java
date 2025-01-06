package com.floragunn.aim.scheduler.store;

import com.floragunn.searchsupport.jobs.JobConfigListener;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobDetailWithBaseConfig;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public interface JobStore<JobConfigType extends JobConfig> {
    void initialize(SchedulerSignaler signaler, Client client, String node, String schedulerName, ScheduledExecutorService configUpdateExecutor)
            throws SchedulerConfigException;

    void shutdown();

    Map<JobKey, InternalJobDetail<JobConfigType>> load(Iterable<JobConfigType> jobConfigs);

    InternalJobDetail<JobConfigType> add(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException;

    boolean contains(JobKey jobKey) throws JobPersistenceException;

    InternalJobDetail<JobConfigType> get(JobKey jobKey) throws JobPersistenceException;

    Map<JobKey, JobDetail> getAllAsMap(Collection<JobKey> jobKeys) throws JobPersistenceException;

    InternalJobDetail<JobConfigType> remove(JobKey jobKey) throws JobPersistenceException;

    List<InternalJobDetail<JobConfigType>> removeAll(List<JobKey> jobKeys) throws JobPersistenceException;

    int size() throws JobPersistenceException;

    Set<JobKey> keySet() throws JobPersistenceException;

    void clear() throws JobPersistenceException;

    void jobComplete(JobDetail jobDetail);

    class HeapJobStore<JobConfigType extends JobConfig> implements JobStore<JobConfigType> {
        private static final Logger LOG = LogManager.getLogger(HeapJobStore.class);

        private final JobConfigFactory<JobConfigType> jobConfigFactory;
        private final Collection<JobConfigListener<JobConfigType>> jobConfigListeners;

        private final Map<JobKey, InternalJobDetail<JobConfigType>> keyToJobMap;

        public HeapJobStore(JobConfigFactory<JobConfigType> jobConfigFactory, Collection<JobConfigListener<JobConfigType>> jobConfigListeners) {
            this.jobConfigFactory = jobConfigFactory;
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
        public Map<JobKey, InternalJobDetail<JobConfigType>> load(Iterable<JobConfigType> jobConfigs) {
            Map<JobKey, InternalJobDetail<JobConfigType>> result = new HashMap<>();
            for (JobConfigType jobConfig : jobConfigs) {
                LOG.trace("Loading config {}", jobConfig);
                if (!contains(jobConfig.getJobKey())) {
                    InternalJobDetail<JobConfigType> jobDetail = new InternalJobDetail<>(jobConfigFactory.createJobDetail(jobConfig), jobConfig);
                    result.put(jobDetail.getKey(), jobDetail);
                }
            }
            for (JobConfigListener<JobConfigType> jobConfigListener : jobConfigListeners) {
                jobConfigListener.onInit(result.values().stream().map(InternalJobDetail::getJobConfig).collect(Collectors.toSet()));
            }
            keyToJobMap.putAll(result);
            return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        public InternalJobDetail<JobConfigType> add(JobDetail newJob, boolean replaceExisting)
                throws ObjectAlreadyExistsException, JobPersistenceException {
            if (!replaceExisting && contains(newJob.getKey())) {
                throw new ObjectAlreadyExistsException(newJob);
            }
            InternalJobDetail<JobConfigType> jobDetail;
            if (newJob instanceof InternalJobDetail<?>) {
                jobDetail = (InternalJobDetail<JobConfigType>) newJob;
            } else if (newJob instanceof JobDetailWithBaseConfig) {
                jobDetail = new InternalJobDetail<>(newJob, ((ConfigJobDetail<JobConfigType>) newJob).getJobConfig());
            } else {
                jobDetail = new InternalJobDetail<>(newJob, null);
            }
            keyToJobMap.put(newJob.getKey(), jobDetail);
            return jobDetail;
        }

        @Override
        public boolean contains(JobKey jobKey) {
            return keyToJobMap.containsKey(jobKey);
        }

        @Override
        public InternalJobDetail<JobConfigType> get(JobKey jobKey) throws JobPersistenceException {
            return keyToJobMap.get(jobKey);
        }

        @Override
        public Map<JobKey, JobDetail> getAllAsMap(Collection<JobKey> jobKeys) throws JobPersistenceException {
            return keyToJobMap.entrySet().stream().filter(entry -> jobKeys.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public InternalJobDetail<JobConfigType> remove(JobKey jobKey) throws JobPersistenceException {
            return keyToJobMap.remove(jobKey);
        }

        @Override
        public List<InternalJobDetail<JobConfigType>> removeAll(List<JobKey> list) throws JobPersistenceException {
            List<InternalJobDetail<JobConfigType>> removed = new ArrayList<>(list.size());
            for (JobKey jobKey : list) {
                InternalJobDetail<JobConfigType> jobDetail = remove(jobKey);
                if (jobDetail != null) {
                    removed.add(jobDetail);
                }
            }
            return removed;
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
            InternalJobDetail<JobConfigType> internalJobDetail = keyToJobMap.get(jobDetail.getKey());
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
