package com.floragunn.searchsupport.jobs.config;

import java.util.Collection;
import java.util.Map;

import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.Trigger;

public interface JobConfig {

    JobKey getJobKey();

    String getDescription();

    Class<? extends Job> getJobClass();

    Map<String, Object> getJobDataMap();

    boolean isDurable();

    Collection<Trigger> getTriggers();

    int hashCode();

    boolean equals(Object other);
    
    long getVersion();
    
    String getAuthToken();
    
    String getSecureAuthTokenAudience();

}
