package com.floragunn.searchsupport.jobs.cluster;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.quartz.spi.JobStore;

public interface DistributedJobStore extends JobStore {
    void clusterConfigChanged(ClusterChangedEvent event);

    boolean isInitialized();
}
