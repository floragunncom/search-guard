package com.floragunn.searchsupport.jobs.config;

import org.quartz.JobDetail;

public interface JobDetailWithBaseConfig extends JobDetail {
    JobConfig getBaseConfig();
    <T> T getBaseConfig(Class<T> type);
}
