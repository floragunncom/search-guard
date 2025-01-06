package com.floragunn.aim.scheduler;

import com.floragunn.searchsupport.jobs.cluster.NodeComparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.quartz.JobKey;

import java.util.Arrays;

public class DynamicJobDistributor implements JobSelector {
    private static final Logger LOG = LogManager.getLogger(DynamicJobDistributor.class);

    private final String name;
    private final NodeComparator<?> nodeComparator;

    private String nodeFilter;
    private String localNodeName;

    private int availableNodeCount = 0;
    private int currentNodeIndex = -1;
    private Object[] availableNodeIds;

    public DynamicJobDistributor(String name, NodeComparator<?> nodeComparator, String nodeFilter, String localNodeName) {
        this.name = name;
        this.nodeComparator = nodeComparator;
        this.nodeFilter = nodeFilter;
        this.localNodeName = localNodeName;
    }

    @Override
    public String toString() {
        return "DynamicJobDistributor " + name;
    }

    @Override
    public boolean isJobSelected(JobKey jobKey) {
        if (availableNodeCount == 0) {
            return false;
        }
        int jobNodeIndex = Math.abs(jobKey.hashCode()) % availableNodeCount;
        LOG.trace("isJobSelected({}, {})\navailableNodeCount: {}\njobNodeIndex: {}", jobKey, currentNodeIndex, availableNodeCount, jobNodeIndex);
        return jobNodeIndex == currentNodeIndex;
    }

    public void initialize() {
        update("initialize");
    }

    public boolean isReschedule(ClusterState state) {
        this.localNodeName = state.nodes().getLocalNodeId();
        return update("cluster state change");
    }

    public boolean isReschedule(String nodeFilter) {
        this.nodeFilter = nodeFilter;
        return update("settings change");
    }

    public boolean isThisNodeConfiguredForExecution() {
        return currentNodeIndex > -1;
    }

    private boolean update(String eventKind) {
        Object[] newAvailableNodeIds = nodeComparator.resolveNodeFilters(nodeFilter == null ? null : nodeFilter.split(","));
        Arrays.sort(newAvailableNodeIds);

        if (availableNodeIds != null && Arrays.equals(availableNodeIds, newAvailableNodeIds)) {
            LOG.trace("Got {} event on {}, but nodes did not change", eventKind, localNodeName);
            return false;
        }
        LOG.debug("Update of {} on {}: {}", this, localNodeName, newAvailableNodeIds);
        int newAvailableNodeCount = newAvailableNodeIds.length;
        int newCurrentNodeIndex = newAvailableNodeCount == 0 ? -1
                : Math.max(Arrays.binarySearch(newAvailableNodeIds, nodeComparator.resolveNodeId(localNodeName)), -1);

        availableNodeIds = newAvailableNodeIds;
        if (availableNodeCount == newAvailableNodeCount && currentNodeIndex == newCurrentNodeIndex) {
            LOG.debug("{} does not require rescheduling of jobs. This node remains at index: {}; available nodes count remains at: {}",
                    eventKind.substring(0, 1).toUpperCase() + eventKind.substring(1), currentNodeIndex, availableNodeCount);
            return false;
        }

        availableNodeCount = newAvailableNodeCount;
        currentNodeIndex = newCurrentNodeIndex;

        if (currentNodeIndex == -1) {
            LOG.debug("This node is not configured to execute jobs for {}. Node filter: {}", this, nodeFilter);
            return false;
        }
        LOG.debug("Node index of {} is {} after update", localNodeName, currentNodeIndex);
        return true;
    }
}
