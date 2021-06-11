package com.floragunn.searchsupport.jobs.cluster;

import java.util.Comparator;

import org.elasticsearch.cluster.service.ClusterService;

public class NodeIdComparator implements NodeComparator<String> {

    private final ClusterService clusterService;

    public NodeIdComparator(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    private static Comparator<String> delegate = Comparator.nullsFirst(String::compareTo);

    @Override
    public int compare(String o1, String o2) {
        return delegate.compare(o1, o2);
    }

    @Override
    public String resolveNodeId(String nodeId) {
        return nodeId;
    }

    @Override
    public String[] resolveNodeFilters(String[] nodeFilterElements) {
        return clusterService.state().nodes().resolveNodes(nodeFilterElements);
    }
}
