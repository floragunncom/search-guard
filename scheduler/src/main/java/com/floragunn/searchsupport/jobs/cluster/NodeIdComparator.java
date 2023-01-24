/*
 * Copyright 2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
