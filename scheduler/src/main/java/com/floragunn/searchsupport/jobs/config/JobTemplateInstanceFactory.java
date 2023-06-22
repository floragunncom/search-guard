package com.floragunn.searchsupport.jobs.config;

import com.floragunn.fluent.collections.ImmutableList;

public interface JobTemplateInstanceFactory<JobType extends JobConfig> {
    ImmutableList<JobType> instantiateTemplate(JobType job);
}
