package com.floragunn.aim.scheduler;

import org.quartz.JobKey;

public interface JobSelector {
    boolean isJobSelected(JobKey jobKey);
}
