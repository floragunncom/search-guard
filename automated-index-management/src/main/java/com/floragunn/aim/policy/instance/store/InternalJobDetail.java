package com.floragunn.aim.policy.instance.store;

import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobDetailWithBaseConfig;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;

import java.util.Objects;

public class InternalJobDetail implements JobDetail, JobDetailWithBaseConfig {
    private static final long serialVersionUID = -3283802307736834293L;

    private final JobConfig jobConfig;
    private JobDetail delegate;

    public InternalJobDetail(JobDetail delegate, JobConfig jobConfig) {
        this.delegate = delegate;
        this.jobConfig = jobConfig;
    }

    @Override
    public JobConfig getBaseConfig() {
        return jobConfig;
    }

    @Override
    public <T> T getBaseConfig(Class<T> type) {
        return type.cast(jobConfig);
    }

    @Override
    public JobKey getKey() {
        return delegate.getKey();
    }

    @Override
    public String getDescription() {
        return delegate.getDescription();
    }

    @Override
    public Class<? extends Job> getJobClass() {
        return delegate.getJobClass();
    }

    @Override
    public JobDataMap getJobDataMap() {
        return delegate.getJobDataMap();
    }

    @Override
    public boolean isDurable() {
        return delegate.isDurable();
    }

    @Override
    public boolean isPersistJobDataAfterExecution() {
        return delegate.isPersistJobDataAfterExecution();
    }

    @Override
    public boolean isConcurrentExectionDisallowed() {
        return delegate.isConcurrentExectionDisallowed();
    }

    @Override
    public boolean requestsRecovery() {
        return delegate.requestsRecovery();
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Object clone() {
        return new InternalJobDetail(delegate, jobConfig);
    }

    @Override
    public JobBuilder getJobBuilder() {
        return delegate.getJobBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof InternalJobDetail jobDetail))
            return false;
        return Objects.equals(jobConfig, jobDetail.jobConfig) && Objects.equals(delegate, jobDetail.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobConfig, delegate);
    }

    public void setDelegate(JobDetail delegate) {
        this.delegate = delegate;
    }
}
