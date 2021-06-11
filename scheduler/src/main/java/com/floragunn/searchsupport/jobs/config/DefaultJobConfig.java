package com.floragunn.searchsupport.jobs.config;

import java.util.List;
import java.util.Map;

import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.Trigger;

public class DefaultJobConfig implements JobConfig {

    private JobKey jobKey;
    private String description;
    private Class<? extends Job> jobClass;
    private Map<String, Object> jobDataMap;
    private boolean durable;
    private List<Trigger> triggers;
    private long version;
    private String authToken;
    private String secureAuthTokenAudience;
    
    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public DefaultJobConfig(Class<? extends Job> jobClass) {
        this.jobClass = jobClass;
    }

    public JobKey getJobKey() {
        return jobKey;
    }

    public void setJobKey(JobKey jobKey) {
        this.jobKey = jobKey;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getJobDataMap() {
        return jobDataMap;
    }

    public void setJobDataMap(Map<String, Object> jobDataMap) {
        this.jobDataMap = jobDataMap;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public List<Trigger> getTriggers() {
        return triggers;
    }

    public void setTriggers(List<Trigger> triggers) {
        this.triggers = triggers;
    }

    public int hashCode() {
        return this.jobKey.hashCode();
    }

    public boolean equals(Object other) {
        if (!(other instanceof JobConfig)) {
            return false;
        }

        return jobKey.equals(((JobConfig) other).getJobKey());
    }

    public Class<? extends Job> getJobClass() {
        return jobClass;
    }

    public void setJobClass(Class<? extends Job> jobClass) {
        this.jobClass = jobClass;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public String getSecureAuthTokenAudience() {
        return secureAuthTokenAudience;
    }

    public void setSecureAuthTokenAudience(String secureAuthTokenAudience) {
        this.secureAuthTokenAudience = secureAuthTokenAudience;
    }


}
