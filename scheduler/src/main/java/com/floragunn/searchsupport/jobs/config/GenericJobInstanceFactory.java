package com.floragunn.searchsupport.jobs.config;

import com.floragunn.fluent.collections.ImmutableList;

public interface GenericJobInstanceFactory<JobType extends JobConfig> {
    ImmutableList<JobType> instantiateGeneric(JobType job);
}
