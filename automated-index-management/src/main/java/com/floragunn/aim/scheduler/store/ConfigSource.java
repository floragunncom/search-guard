package com.floragunn.aim.scheduler.store;

import com.floragunn.searchsupport.jobs.config.JobConfig;

public interface ConfigSource<ConfigType extends JobConfig> {
    Iterable<ConfigType> getConfigs();
}
