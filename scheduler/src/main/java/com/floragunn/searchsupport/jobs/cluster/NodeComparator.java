package com.floragunn.searchsupport.jobs.cluster;

import java.util.Comparator;

public interface NodeComparator<NodeType> extends Comparator<NodeType> {
    NodeType resolveNodeId(String nodeId);
    NodeType [] resolveNodeFilters(String [] nodeFilterElements);
}
