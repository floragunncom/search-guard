package com.floragunn.aim.scheduler.store;

import com.floragunn.searchsupport.jobs.config.JobConfig;
import org.quartz.JobDetail;

public interface ConfigJobDetail<JobConfigType extends JobConfig> extends JobDetail {
    JobConfigType getJobConfig();
}
