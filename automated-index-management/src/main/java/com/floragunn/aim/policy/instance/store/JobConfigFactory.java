package com.floragunn.aim.policy.instance.store;

import com.floragunn.searchsupport.jobs.config.JobConfig;
import org.quartz.JobDetail;

public interface JobConfigFactory<JobConfigType extends JobConfig> {
    JobDetail createJobDetail(JobConfigType jobType);
}
