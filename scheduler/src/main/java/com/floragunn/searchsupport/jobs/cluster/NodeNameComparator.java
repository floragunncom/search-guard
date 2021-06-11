package com.floragunn.searchsupport.jobs.cluster;

import java.util.Comparator;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;

public class NodeNameComparator implements NodeComparator<String> {

    private static Comparator<String> delegate = Comparator.nullsFirst(String::compareTo);
    private final ClusterService clusterService;

    public NodeNameComparator(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public int compare(String o1, String o2) {
        return delegate.compare(o1, o2);
    }

    @Override
    public String resolveNodeId(String nodeId) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);

        if (node != null) {
            return node.getName();
        } else {
            return "*** Unknown node id " + nodeId;
        }
    }

    @Override
    public String[] resolveNodeFilters(String[] nodeFilterElements) {
        String[] nodeIds = clusterService.state().nodes().resolveNodes(nodeFilterElements);
        String[] result = new String[nodeIds.length];

        for (int i = 0; i < nodeIds.length; i++) {
            result[i] = resolveNodeId(nodeIds[i]);
        }

        return result;
    }
}
