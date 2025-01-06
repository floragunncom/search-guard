package com.floragunn.aim.scheduler;

import com.floragunn.searchsupport.jobs.core.DynamicQuartzThreadPool;
import com.floragunn.searchsupport.jobs.core.QuartzSchedulerWithCustomizableThreadGroup;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.DefaultThreadExecutor;
import org.quartz.impl.StdJobRunShellFactory;
import org.quartz.impl.StdScheduler;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobFactory;
import org.quartz.spi.JobStore;
import org.quartz.spi.ThreadPool;

import java.time.Duration;

public class SchedulerBuilder {
    private static final DefaultThreadExecutor DEFAULT_THREAD_EXECUTOR = new DefaultThreadExecutor();

    private final String name;
    private final JobStore store;
    private final JobFactory jobFactory;
    private final String nodeName;

    private ThreadPool threadPool;
    private int maxThreads = 3;
    private int threadPriority = Thread.NORM_PRIORITY;
    private Duration threadKeepAlive = Duration.ofHours(1);
    private int maxBatchSize = 1;
    private long batchTimeWindow = 0;
    private int idleWaitTime = -1;
    private long dbFailureRetryInterval = -1;

    public SchedulerBuilder(String name, JobStore store, JobFactory jobFactory, String nodeName) {
        this.name = name;
        this.store = store;
        this.jobFactory = jobFactory;
        this.nodeName = nodeName;
    }

    public SchedulerBuilder threadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
        return this;
    }

    public SchedulerBuilder maxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
        return this;
    }

    public SchedulerBuilder threadPriority(int threadPriority) {
        this.threadPriority = threadPriority;
        return this;
    }

    public SchedulerBuilder threadKeepAlive(Duration threadKeepAlive) {
        this.threadKeepAlive = threadKeepAlive;
        return this;
    }

    public SchedulerBuilder maxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    public SchedulerBuilder batchTimeWindow(long batchTimeWindow) {
        this.batchTimeWindow = batchTimeWindow;
        return this;
    }

    public SchedulerBuilder idleWaitTime(int idleWaitTime) {
        this.idleWaitTime = idleWaitTime;
        return this;
    }

    public SchedulerBuilder dbFailureRetryInterval(long dbFailureRetryInterval) {
        this.dbFailureRetryInterval = dbFailureRetryInterval;
        return this;
    }

    public Scheduler build() throws SchedulerException {
        if (this.threadPool == null) {
            String suffix = nodeName != null ? "[" + nodeName + "]" : "";
            this.threadPool = new DynamicQuartzThreadPool(Thread.currentThread().getThreadGroup(), name, suffix, maxThreads, threadPriority,
                    threadKeepAlive);
        }

        threadPool.setInstanceName(name);
        threadPool.initialize();

        JobRunShellFactory jobRunShellFactory = new StdJobRunShellFactory();
        QuartzSchedulerResources qrs = new QuartzSchedulerResources();
        qrs.setName(name);
        qrs.setInstanceId(name);
        qrs.setJobRunShellFactory(jobRunShellFactory);
        qrs.setThreadPool(threadPool);
        qrs.setThreadExecutor(DEFAULT_THREAD_EXECUTOR);
        qrs.setJobStore(store);
        qrs.setMaxBatchSize(maxBatchSize);
        qrs.setBatchTimeWindow(batchTimeWindow);
        qrs.setMakeSchedulerThreadDaemon(true);

        QuartzScheduler quartzScheduler = new QuartzSchedulerWithCustomizableThreadGroup(qrs, Thread.currentThread().getThreadGroup(), idleWaitTime,
                dbFailureRetryInterval);
        ClassLoadHelper classLoaderHelper = new CascadingClassLoadHelper();
        classLoaderHelper.initialize();

        store.initialize(classLoaderHelper, quartzScheduler.getSchedulerSignaler());

        Scheduler scheduler = new StdScheduler(quartzScheduler);
        jobRunShellFactory.initialize(scheduler);
        quartzScheduler.initialize();
        scheduler.setJobFactory(jobFactory);

        return scheduler;
    }
}
