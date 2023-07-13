package com.floragunn.searchsupport.jobs.cluster;

import com.floragunn.searchsupport.jobs.config.JobConfig;

/**
 * Select job to be execute on local cluster node
 */
public interface CurrentNodeJobSelector {
    CurrentNodeJobSelector EXECUTE_ON_ALL_NODES = job -> true;
    boolean isJobSelected(JobConfig jobConfig);
}
