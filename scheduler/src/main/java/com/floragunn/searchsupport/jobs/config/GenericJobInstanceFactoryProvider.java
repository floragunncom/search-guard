package com.floragunn.searchsupport.jobs.config;

import com.floragunn.searchsupport.jobs.cluster.CurrentNodeJobSelector;

@FunctionalInterface
public interface GenericJobInstanceFactoryProvider<JobType extends JobConfig> {
    GenericJobInstanceFactory<JobType> provide(CurrentNodeJobSelector currentNodeJobSelector);
}
